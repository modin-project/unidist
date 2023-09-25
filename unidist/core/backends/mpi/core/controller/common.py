# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Common functionality related to `controller`."""

import itertools
import time

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.local_object_store import LocalObjectStore
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore
from unidist.core.backends.mpi.core.serialization import ComplexDataSerializer
from unidist.config import MpiBackoff

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402

logger = common.get_logger("common", "common.log")


class RoundRobin:
    __instance = None

    def __init__(self):
        self.reserved_ranks = []
        mpi_state = communication.MPIState.get_instance()
        self.rank_to_schedule = itertools.cycle(
            (
                global_rank
                for global_rank in mpi_state.workers
                # check if a rank to schedule is not equal to the rank
                # of the current process to not get into recursive scheduling
                if global_rank != mpi_state.global_rank
            )
        )
        logger.debug(f"RoundRobin init for {mpi_state.global_rank} rank")

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``RoundRobin``.

        Returns
        -------
        RoundRobin
        """
        if cls.__instance is None:
            cls.__instance = RoundRobin()
        return cls.__instance

    def schedule_rank(self):
        """
        Find the next non-reserved rank for task/actor-task execution.

        Returns
        -------
        int
            A rank number.
        """
        next_rank = None
        mpi_state = communication.MPIState.get_instance()

        # Go rank by rank to find the first one non-reserved
        for _ in mpi_state.workers:
            rank = next(self.rank_to_schedule)
            if rank not in self.reserved_ranks:
                next_rank = rank
                break

        if next_rank is None:
            raise Exception("All ranks blocked")

        return next_rank

    def reserve_rank(self, rank):
        """
        Reserve the rank for the actor scheduling.

        This makes the rank unavailable for scheduling a new actor or task
        until it gets released.

        Parameters
        ----------
        rank : int
            A rank number.
        """
        self.reserved_ranks.append(rank)
        logger.debug(
            f"RoundRobin reserve rank {rank} for actor "
            + f"on worker with rank {communication.MPIState.get_instance().global_rank}"
        )

    def release_rank(self, rank):
        """
        Release the rank reserved for the actor.

        This makes the rank available for scheduling a new actor or task.

        Parameters
        ----------
        rank : int
            A rank number.
        """
        self.reserved_ranks.remove(rank)
        logger.debug(
            f"RoundRobin release rank {rank} reserved for actor "
            + f"on worker with rank {communication.MPIState.get_instance().global_rank}"
        )


def pull_data(comm, owner_rank):
    """
    Receive data from another MPI process.

    Data can come from shared memory or direct communication depending on the package type.
    The data is de-serialized from received buffers.

    Parameters
    ----------
    owner_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received data object from another MPI process.
    """
    info_package = communication.mpi_recv_object(comm, owner_rank)
    if info_package["package_type"] == common.MetadataPackage.SHARED_DATA:
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()
        data_id = local_store.get_unique_data_id(info_package["id"])

        if local_store.contains(data_id):
            return {
                "id": data_id,
                "data": local_store.get(data_id),
            }

        data = shared_store.get(data_id, owner_rank, info_package)
        local_store.put(data_id, data)
        return {
            "id": data_id,
            "data": data,
        }
    elif info_package["package_type"] == common.MetadataPackage.LOCAL_DATA:
        return communication.recv_complex_data(
            comm, owner_rank, info_package=info_package
        )
    else:
        raise ValueError("Unexpected package of data info!")


def pull_data_in_parallel(comm, owner_ranks, data_ids):
    """
    Receive data from another MPI process using shared memory or direct communiction, depending on package type.
    The data is de-serialized from received buffers.

    Parameters
    ----------
    owner_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received data object from another MPI process.
    """
    pending_requests = []
    shared_objects = {}
    local_objects = {}
    logger.debug("before mpi_recv_object")
    async_reqs = [
        communication.mpi_irecv_object(comm, owner_rank) for owner_rank in owner_ranks
    ]
    info_packages = MPI.Request.waitall(async_reqs)
    logger.debug("after mpi_recv_object")
    for i, (owner_rank, data_id) in enumerate(zip(owner_ranks, data_ids)):
        info_package = info_packages[i]
        if info_package["package_type"] == common.MetadataPackage.SHARED_DATA:
            local_store = LocalObjectStore.get_instance()
            shared_store = SharedObjectStore.get_instance()

            data = shared_store.get(data_id, owner_rank, info_package)
            local_store.put(data_id, data)
            shared_objects[data_id] = data
        elif info_package["package_type"] == common.MetadataPackage.LOCAL_DATA:
            pending_request = communication.irecv_complex_data(
                comm, owner_rank, info_package=info_package
            )
            pending_requests.append(pending_request)
            local_objects[data_id] = pending_request

        else:
            raise ValueError("Unexpected package of data info!")

    logger.debug("after irecv")
    # MPI.Request.Waitall(
    #     [
    #         request
    #         for pending_request in pending_requests
    #         for request in pending_request.requests
    #     ]
    # )
    while not MPI.Request.Testall(
        [
            request
            for pending_request in pending_requests
            for request in pending_request.requests
        ]
    ):
        time.sleep(MpiBackoff.get())
    logger.debug("after waitall")
    local_objects_copy = local_objects.copy()
    for data_id, pending_request in local_objects_copy.items():
        # Set the necessary metadata for unpacking
        deserializer = ComplexDataSerializer(
            pending_request.raw_buffers, pending_request.buffer_count
        )
        # Start unpacking
        data = deserializer.deserialize(pending_request.msgpack_buffer)
        local_objects[data_id] = data["data"]

    all_objects = {**shared_objects, **local_objects}
    return all_objects


def request_worker_data(data_id):
    """
    Get an object(s) associated with `data_id` from the object storage.

    Parameters
    ----------
    data_id : unidist.core.backends.common.data_id.DataID
        An ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """
    mpi_state = communication.MPIState.get_instance()
    local_store = LocalObjectStore.get_instance()

    owner_rank = local_store.get_data_owner(data_id)

    logger.debug("GET {} id from {} rank".format(data_id._id, owner_rank))

    # Worker request
    operation_type = common.Operation.GET
    operation_data = {
        "source": mpi_state.global_rank,
        "id": data_id.base_data_id(),
        # set `is_blocking_op` to `True` to tell a worker
        # to send the data directly to the requester
        # without any delay
        "is_blocking_op": True,
    }
    # We use a blocking send here because we have to wait for
    # completion of the communication, which is necessary for the pipeline to continue.
    communication.send_simple_operation(
        mpi_state.comm,
        operation_type,
        operation_data,
        owner_rank,
    )

    # Blocking get
    complex_data = pull_data(mpi_state.comm, owner_rank)
    if data_id.base_data_id() != complex_data["id"]:
        raise ValueError("Unexpected data_id!")
    data = complex_data["data"]

    # Caching the result, check the protocol correctness here
    local_store.put(data_id, data)

    return data


def get_remote_objects(data_ids):
    """
    Get an object(s) associated with `data_id` from the object storage.

    Parameters
    ----------
    data_id : unidist.core.backends.common.data_id.DataID
        An ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """
    logger.debug("start get_remote_objects")
    mpi_state = communication.MPIState.get_instance()
    local_store = LocalObjectStore.get_instance()

    owner_ranks = [local_store.get_data_owner(data_id) for data_id in data_ids]

    logger.debug("before loop with send")
    async_reqs = []
    for data_id, owner_rank in zip(data_ids, owner_ranks):
        # Worker request
        operation_type = common.Operation.GET
        operation_data = {
            "source": mpi_state.global_rank,
            "id": data_id.base_data_id(),
            # set `is_blocking_op` to `True` to tell a worker
            # to send the data directly to the requester
            # without any delay
            "is_blocking_op": False,
        }
        # We use a blocking send here because we have to wait for
        # completion of the communication, which is necessary for the pipeline to continue.
        reqs = communication.isend_simple_operation(
            mpi_state.comm,
            operation_type,
            operation_data,
            owner_rank,
        )
        async_reqs.extend([req[0] for req in reqs])
    MPI.Request.waitall(async_reqs)
    logger.debug("after loop with send")

    logger.debug("before pull_data_in_parallel")
    all_objects = pull_data_in_parallel(mpi_state.comm, owner_ranks, data_ids)
    logger.debug("after pull_data_in_parallel")
    for obj, data_id in zip(all_objects.keys(), data_ids):
        if data_id.base_data_id() != obj:
            raise ValueError("Unexpected data_id!")

    return all_objects


def _push_local_data(dest_rank, data_id, is_blocking_op, is_serialized):
    """
    Send local data associated with passed ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    is_blocking_op : bool
        Whether the communication should be blocking or not.
        If ``True``, the request should be processed immediatly
        even for a worker since it can get into controller mode.
    is_serialized : bool
        `data_id` is already serialized or not.
    """
    local_store = LocalObjectStore.get_instance()

    # Check if data was already pushed
    if not local_store.is_already_sent(data_id, dest_rank):
        logger.debug("PUT LOCAL {} id to {} rank".format(data_id._id, dest_rank))

        mpi_state = communication.MPIState.get_instance()
        async_operations = AsyncOperations.get_instance()
        # Push the local master data to the target worker directly
        if is_serialized:
            operation_data = local_store.get_serialized_data(data_id)
        else:
            operation_data = {
                "id": data_id,
                "data": local_store.get(data_id),
            }

        operation_type = common.Operation.PUT_DATA
        if is_blocking_op:
            serialized_data = communication.send_complex_data(
                mpi_state.comm,
                operation_data,
                dest_rank,
                is_serialized,
            )
        else:
            h_list, serialized_data = communication.isend_complex_operation(
                mpi_state.comm,
                operation_type,
                operation_data,
                dest_rank,
                is_serialized,
            )
            async_operations.extend(h_list)

        if not is_serialized:
            local_store.cache_serialized_data(data_id, serialized_data)

        #  Remember pushed id
        local_store.cache_send_info(data_id, dest_rank)


def _push_shared_data(dest_rank, data_id, is_blocking_op):
    """
    Send the data associated with the data ID to target rank using shared memory.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.common.data_id.DataID
        An ID to data.
    is_blocking_op : bool
        Whether the communication should be blocking or not.
        If ``True``, the request should be processed immediatly
        even for a worker since it can get into controller mode.
    """
    mpi_state = communication.MPIState.get_instance()
    shared_store = SharedObjectStore.get_instance()
    mpi_state = communication.MPIState.get_instance()
    operation_type = common.Operation.PUT_SHARED_DATA
    async_operations = AsyncOperations.get_instance()
    info_package = shared_store.get_shared_info(data_id)
    # wrap to dict for sending and correct deserialization of the object by the recipient
    operation_data = dict(info_package)
    if is_blocking_op:
        communication.mpi_send_object(mpi_state.comm, operation_data, dest_rank)
    else:
        h_list = communication.isend_simple_operation(
            mpi_state.comm,
            operation_type,
            operation_data,
            dest_rank,
        )
        async_operations.extend(h_list)


def _push_data_owner(dest_rank, data_id):
    """
    Send data location associated with data ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    value : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    local_store = LocalObjectStore.get_instance()
    operation_type = common.Operation.PUT_OWNER
    operation_data = {
        "id": data_id,
        "owner": local_store.get_data_owner(data_id),
    }
    async_operations = AsyncOperations.get_instance()
    h_list = communication.isend_simple_operation(
        communication.MPIState.get_instance().comm,
        operation_type,
        operation_data,
        dest_rank,
    )
    async_operations.extend(h_list)


def push_data(dest_rank, value, is_blocking_op=False):
    """
    Parse and send all values to destination rank.

    Process all arguments recursivelly and send all ID associated data or it's location
    to the target rank.

    Parameters
    ----------
    dest_rank : int
        Rank where task data is needed.
    value : iterable or dict or object
        Arguments to be sent.
    is_blocking_op : bool
        Whether the communication should be blocking or not.
        If ``True``, the request should be processed immediatly
        even for a worker since it can get into controller mode.
    """
    local_store = LocalObjectStore.get_instance()
    shared_store = SharedObjectStore.get_instance()

    if isinstance(value, (list, tuple)):
        for v in value:
            push_data(dest_rank, v)
    elif isinstance(value, dict):
        for v in value.values():
            push_data(dest_rank, v)
    elif is_data_id(value):
        data_id = value
        if shared_store.contains(data_id):
            _push_shared_data(dest_rank, data_id, is_blocking_op)
        elif local_store.contains(data_id):
            if local_store.is_already_serialized(data_id):
                _push_local_data(dest_rank, data_id, is_blocking_op, is_serialized=True)
            else:
                data = local_store.get(data_id)
                if shared_store.should_be_shared(data):
                    shared_store.put(data_id, data)
                    _push_shared_data(dest_rank, data_id, is_blocking_op)
                else:
                    _push_local_data(
                        dest_rank, data_id, is_blocking_op, is_serialized=False
                    )
        elif local_store.contains_data_owner(data_id):
            _push_data_owner(dest_rank, data_id)
        else:
            raise ValueError("Unknown DataID!")
