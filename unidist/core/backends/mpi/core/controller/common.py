# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Common functionality related to `controller`."""

import itertools
from mpi4py import futures

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.controller.object_store import object_store

logger = common.get_logger("common", "common.log")

# initial worker number is equal to rank 2 because
# rank 0 is for controller process
# rank 1 is for monitor process
initial_worker_number = 2


class RoundRobin:
    __instance = None

    def __init__(self):
        self.reserved_ranks = []
        self.rank_to_schedule = itertools.cycle(
            (
                rank
                for rank in range(
                    initial_worker_number,
                    communication.MPIState.get_instance().world_size,
                )
                # check if a rank to schedule is not equal to the rank
                # of the current process to not get into recursive scheduling
                if rank != communication.MPIState.get_instance().rank
            )
        )
        logger.debug(
            f"RoundRobin init for {communication.MPIState.get_instance().rank} rank"
        )

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

        # Go rank by rank to find the first one non-reserved
        for _ in range(
            initial_worker_number, communication.MPIState.get_instance().world_size
        ):
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
    data = communication.recv_complex_data(mpi_state.comm, owner_rank)

    # Caching the result, check the protocol correctness here
    object_store.put(data_id, data)

    return data


def _push_local_data(dest_rank, data_id):
    """
    Send local data associated with passed ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    # Check if data was already pushed
    if not object_store.is_already_sent(data_id, dest_rank):
        logger.debug("PUT LOCAL {} id to {} rank".format(data_id._id, dest_rank))

        mpi_state = communication.MPIState.get_instance()
        async_operations = AsyncOperations.get_instance()
        # Push the local master data to the target worker directly
        operation_type = common.Operation.PUT_DATA
        if object_store.is_already_serialized(data_id):
            serialized_data = object_store.get_serialized_data(data_id)
            h_list, _ = communication.isend_complex_operation(
                mpi_state.comm,
                operation_type,
                serialized_data,
                dest_rank,
                is_serialized=True,
            )
        else:
            operation_data = {
                "id": data_id,
                "data": object_store.get(data_id),
            }
            h_list, serialized_data = communication.isend_complex_operation(
                mpi_state.comm,
                operation_type,
                operation_data,
                dest_rank,
                is_serialized=False,
            )
            object_store.cache_serialized_data(data_id, serialized_data)
        async_operations.extend(h_list)
        #  Remember pushed id
        object_store.cache_send_info(data_id, dest_rank)


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


def push_data(dest_rank, value):
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
    if isinstance(value, (list, tuple)):
        for v in value:
            push_data(dest_rank, v)
    elif isinstance(value, dict):
        for v in value.values():
            push_data(dest_rank, v)
    elif is_data_id(value):
        if object_store.contains(value):
            _push_local_data(dest_rank, value)
        elif object_store.contains_data_owner(value):
            _push_data_owner(dest_rank, value)
        else:
            raise ValueError("Unknown DataID! {}".format(value))


def queue_or_execute(comm, workQueue, function, args, blocking=False):
    """
    Put the reciveved function in background queue for rank 0,
    or execute the function as is for the workers

    Process all arguments recursivelly and send all ID associated data or it's location
    to the target rank.

    Parameters
    ----------
    comm : communicator
        Communicator to determine the caller calls the function from mainthread or worker threads
    workQueue : Queue
        The queue shared between background and main thread.
    function : function
        Function to be executed.
    args : list
        Arguments to the function.
    bolcking : boolean
        If the function passed needs a value returned immediately.


    """
    if 0 == comm.Get_rank():
        future = futures.Future()
        if blocking:
            workQueue.put([future, [function, args]])
            return future.result()
        else:
            workQueue.put([None, [function, args]])
    else:
        result = function(*args)
        return result
