# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Common functionality related to `controller`."""

import itertools

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.local_object_store import LocalObjectStore
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore
from unidist.core.backends.mpi.core.serialization import serialize_complex_data


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


def pull_data(comm, owner_rank=None):
    """
    Receive data from another MPI process.

    Data can come from shared memory or direct communication depending on the package type.
    The data is de-serialized from received buffers.

    Parameters
    ----------
    owner_rank : int or None
        Source MPI process to receive data from.
        If ``None``, data will be received from any source based on `iprobe`.

    Returns
    -------
    object
        Received data object from another MPI process.
    """
    if owner_rank is None:
        info_package, source = communication.mpi_iprobe_recv_object(
            comm, tag=common.MPITag.OBJECT_BLOCKING
        )
        owner_rank = source
    else:
        info_package = communication.mpi_recv_object(comm, owner_rank)
    if info_package["package_type"] == common.MetadataPackage.SHARED_DATA:
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()
        data_id = info_package["id"]

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
        local_store = LocalObjectStore.get_instance()
        data = communication.recv_complex_data(
            comm, owner_rank, info_package=info_package
        )
        data_id = info_package["id"]
        local_store.put(data_id, data)
        return {
            "id": data_id,
            "data": data,
        }
    elif info_package["package_type"] == common.MetadataPackage.TASK_DATA:
        return communication.recv_complex_data(
            comm, owner_rank, info_package=info_package
        )
    else:
        raise ValueError("Unexpected package of data info!")


def request_worker_data(data_ids):
    """
    Get objects associated with `data_ids` from the object storage.

    Parameters
    ----------
    data_ids : list[unidist.core.backends.mpi.core.common.MpiDataID]
        IDs to objects to get data from.

    Returns
    -------
    object
        A Python object.
    """
    mpi_state = communication.MPIState.get_instance()
    local_store = LocalObjectStore.get_instance()
    async_operations = AsyncOperations.get_instance()

    for data_id in data_ids:
        owner_rank = local_store.get_data_owner(data_id)

        logger.debug("GET {} id from {} rank".format(data_id._id, owner_rank))

        # Worker request
        operation_type = common.Operation.GET
        operation_data = {
            "source": mpi_state.global_rank,
            "id": data_id,
            # set `is_blocking_op` to `True` to tell a worker
            # to send the data directly to the requester
            # without any delay
            "is_blocking_op": True,
        }
        h_list = communication.isend_simple_operation(
            mpi_state.global_comm,
            operation_type,
            operation_data,
            owner_rank,
        )
        # We do not wait for async requests here because
        # we can receive the data from the first available worker below
        async_operations.extend(h_list)

    data_count = 0
    while data_count < len(data_ids):
        # Remote data gets available in the local store inside `pull_data`
        complex_data = pull_data(mpi_state.global_comm)
        if isinstance(complex_data["data"], Exception):
            raise complex_data["data"]
        data_id = complex_data["id"]
        if data_id in data_ids:
            data_count += 1
        else:
            raise RuntimeError(
                f"DataID {data_id} isn't in the requested list {data_ids}"
            )


def _push_local_data(dest_rank, data_id, is_blocking_op, is_serialized):
    """
    Send local data associated with passed ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
            # Insert `data_id` to get full metadata package further
            operation_data["id"] = data_id
        else:
            operation_data = {
                "id": data_id,
                "data": local_store.get(data_id),
            }

        operation_type = common.Operation.PUT_DATA
        if is_blocking_op:
            serialized_data = communication.send_complex_data(
                mpi_state.global_comm,
                operation_data,
                dest_rank,
                is_serialized=is_serialized,
            )
        else:
            h_list, serialized_data = communication.isend_complex_operation(
                mpi_state.global_comm,
                operation_type,
                operation_data,
                dest_rank,
                is_serialized=is_serialized,
            )
            async_operations.extend(h_list)

        if not is_serialized or not local_store.is_already_serialized(data_id):
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
    data_id : unidist.core.backends.mpi.core.common.MpiDataID
        An ID to data.
    is_blocking_op : bool
        Whether the communication should be blocking or not.
        If ``True``, the request should be processed immediatly
        even for a worker since it can get into controller mode.
    """
    local_store = LocalObjectStore.get_instance()
    # Check if data was already pushed
    if not local_store.is_already_sent(data_id, dest_rank):
        mpi_state = communication.MPIState.get_instance()
        shared_store = SharedObjectStore.get_instance()
        mpi_state = communication.MPIState.get_instance()
        operation_type = common.Operation.PUT_SHARED_DATA
        async_operations = AsyncOperations.get_instance()
        info_package = shared_store.get_shared_info(data_id)
        # wrap to dict for sending and correct deserialization of the object by the recipient
        operation_data = dict(info_package)
        if is_blocking_op:
            communication.mpi_send_object(
                mpi_state.global_comm,
                operation_data,
                dest_rank,
                tag=common.MPITag.OBJECT_BLOCKING,
            )
        else:
            h_list = communication.isend_simple_operation(
                mpi_state.global_comm,
                operation_type,
                operation_data,
                dest_rank,
            )
            async_operations.extend(h_list)
        local_store.cache_send_info(data_id, dest_rank)


def _push_data_owner(dest_rank, data_id):
    """
    Send data location associated with data ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        communication.MPIState.get_instance().global_comm,
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
                serialized_data = serialize_complex_data(data)
                if shared_store.is_allocated() and shared_store.should_be_shared(
                    serialized_data
                ):
                    shared_store.put(data_id, serialized_data)
                    _push_shared_data(dest_rank, data_id, is_blocking_op)
                else:
                    local_store.cache_serialized_data(data_id, serialized_data)
                    _push_local_data(
                        dest_rank, data_id, is_blocking_op, is_serialized=True
                    )
        elif local_store.contains_data_owner(data_id):
            _push_data_owner(dest_rank, data_id)
        else:
            raise ValueError("Unknown DataID!")
