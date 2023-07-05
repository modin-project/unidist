# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Common functionality related to `controller`."""
import itertools

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.object_store import ObjectStore
from unidist.core.backends.mpi.core.shared_store import SharedStore

logger = common.get_logger("common", "common.log")


class RoundRobin:
    __instance = None

    def __init__(self):
        self.reserved_ranks = []
        mpi_state = communication.MPIState.get_instance()
        self.rank_to_schedule = itertools.cycle(
            (
                rank
                for rank in mpi_state.workers
                # check if a rank to schedule is not equal to the rank
                # of the current process to not get into recursive scheduling
                if rank != mpi_state.rank
            )
        )
        logger.debug(f"RoundRobin init for {mpi_state.rank} rank")

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
            + f"on worker with rank {communication.MPIState.get_instance().rank}"
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
            + f"on worker with rank {communication.MPIState.get_instance().rank}"
        )


def get_complex_data(comm, owner_rank):
    info_package = communication.mpi_recv_object(comm, owner_rank)
    if info_package["package_type"] == communication.DataInfoType.SHARED_DATA:
        object_store = ObjectStore.get_instance()
        shared_store = SharedStore.get_instance()
        info_package["id"] = object_store.get_unique_data_id(info_package["id"])
        shared_store.put_shared_info(info_package["id"], info_package)

        # check data in shared memory
        is_contained_in_shared_memory = SharedStore.get_instance().contains(
            info_package["id"]
        )

        if not is_contained_in_shared_memory:
            sh_buf = shared_store.get_shared_buffer(info_package["id"])
            # recv serialized data to shared memory
            owner_monitor = (
                communication.MPIState.get_instance().get_monitor_by_worker_rank(
                    owner_rank
                )
            )
            communication.send_simple_operation(
                comm,
                operation_type=common.Operation.REQUEST_SHARED_DATA,
                operation_data=info_package,
                dest_rank=owner_monitor,
            )
            communication.mpi_recv_shared_buffer(comm, sh_buf, owner_monitor)
            shared_store.put_service_info(info_package["id"])
        data = shared_store.get(info_package["id"])
        return {
            "id": info_package["id"],
            "data": data,
        }
    if info_package["package_type"] == communication.DataInfoType.LOCAL_DATA:
        return communication.recv_complex_data(comm, owner_rank, info=info_package)
    else:
        raise ValueError("Unexpected package of data info!")


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
    object_store = ObjectStore.get_instance()

    owner_rank = object_store.get_data_owner(data_id)

    logger.debug("GET {} id from {} rank".format(data_id._id, owner_rank))

    # Worker request
    operation_type = common.Operation.GET
    operation_data = {
        "source": mpi_state.rank,
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
    complex_data = get_complex_data(mpi_state.comm, owner_rank)
    if data_id.base_data_id() != complex_data["id"]:
        raise ValueError("Unexpected data_id!")
    data = complex_data["data"]

    # Caching the result, check the protocol correctness here
    object_store.put(data_id, data)

    return data


def _push_local_data(dest_rank, data_id, is_blocking_op, is_serialized):
    """
    Send local data associated with passed ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    object_store = ObjectStore.get_instance()

    # Check if data was already pushed
    if not object_store.is_already_sent(data_id, dest_rank):
        logger.debug("PUT LOCAL {} id to {} rank".format(data_id._id, dest_rank))

        mpi_state = communication.MPIState.get_instance()
        async_operations = AsyncOperations.get_instance()
        # Push the local master data to the target worker directly
        if is_serialized:
            operation_data = object_store.get_serialized_data(data_id)
        else:
            operation_data = {
                "id": data_id,
                "data": object_store.get(data_id),
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
            object_store.cache_serialized_data(data_id, serialized_data)

        #  Remember pushed id
        object_store.cache_send_info(data_id, dest_rank)


def _push_shared_data(dest_rank, data_id, is_blocking_op):
    """
    Send data location associated with data ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    value : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    mpi_state = communication.MPIState.get_instance()
    shared_store = SharedStore.get_instance()
    mpi_state = communication.MPIState.get_instance()
    operation_type = common.Operation.PUT_SHARED_DATA
    async_operations = AsyncOperations.get_instance()
    info_package = shared_store.get_data_shared_info(data_id)
    if is_blocking_op:
        communication.mpi_send_object(mpi_state.comm, info_package, dest_rank)
    else:
        h_list = communication.isend_simple_operation(
            mpi_state.comm,
            operation_type,
            info_package,
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
    object_store = ObjectStore.get_instance()
    operation_type = common.Operation.PUT_OWNER
    operation_data = {
        "id": data_id,
        "owner": object_store.get_data_owner(data_id),
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
    """
    object_store = ObjectStore.get_instance()
    shared_store = SharedStore.get_instance()

    if isinstance(value, (list, tuple)):
        for v in value:
            push_data(dest_rank, v)
    elif isinstance(value, dict):
        for v in value.values():
            push_data(dest_rank, v)
    elif is_data_id(value):
        data_id = value
        if shared_store.contains_shared_info(data_id):
            _push_shared_data(dest_rank, data_id, is_blocking_op)
        elif object_store.is_already_serialized(data_id):
            _push_local_data(dest_rank, data_id, is_blocking_op, is_serialized=True)
        elif object_store.contains(data_id):
            data = object_store.get(data_id)
            if shared_store.is_should_be_shared(data):
                put_to_shared_memory(data_id)
                _push_shared_data(dest_rank, data_id, is_blocking_op)
            else:
                _push_local_data(
                    dest_rank, data_id, is_blocking_op, is_serialized=False
                )
        elif object_store.contains_data_owner(data_id):
            _push_data_owner(dest_rank, data_id)
        else:
            raise ValueError("Unknown DataID!")


def put_to_shared_memory(data_id):
    object_store = ObjectStore.get_instance()
    shared_store = SharedStore.get_instance()

    operation_data = object_store.get(data_id)
    # reservation_data, serialized_data = shared_store.reserve_shared_memory(
    #     operation_data
    # )
    mpi_state = communication.MPIState.get_instance()
    reservation_data, serialized_data = communication.reserve_shared_memory(
        mpi_state.comm, data_id, operation_data, is_serialized=False
    )

    shared_store.put(data_id, reservation_data, serialized_data)
