# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Monitoring process."""

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


class TaskCounter:
    __instance = None

    def __init__(self):
        self.task_counter = 0

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``TaskCounter``.

        Returns
        -------
        TaskCounter
        """
        if cls.__instance is None:
            cls.__instance = TaskCounter()
        return cls.__instance

    def increment(self):
        """Increment task counter by one."""
        self.task_counter += 1


class DataIDTracker:
    """
    Class that keeps track of all completed data IDs.

    """

    __instance = None

    def __init__(self):
        self.completed_data_ids = set()

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``DataIDTracker``.

        Returns
        -------
        DataIDTracker
        """
        if cls.__instance is None:
            cls.__instance = DataIDTracker()
        return cls.__instance

    def add_to_completed(self, data_ids):
        """
        Adds the given data_ids to set of completed data IDs

        Returns
        -------
        None
        """
        self.completed_data_ids.update(data_ids)


class WaitHandler:
    """
    Class that handles wait requests.
    """

    __instance = None

    def __init__(self):
        self.completed_data_ids = set()
        self.awaited_data_ids = []
        self.ready = []
        self.num_returns = 0

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``WaitHandler``.

        Returns
        -------
        WaitHandler
        """
        if cls.__instance is None:
            cls.__instance = WaitHandler()
        return cls.__instance

    def add_wait_request(self, awaited_data_ids, num_returns):
        """
        Add a wait request for a list of data_ids and number of data_ids to be awaited.

        Parameters
        ----------
        awaited_data_ids : List[unidist.core.backends.common.data_id.DataID]
            List of data_ids to be awaited.
        num_returns : int,
            The number of ``DataID``-s that should be returned as ready.
        """

        self.awaited_data_ids = awaited_data_ids
        self.num_returns = num_returns

    def process_wait_requests(self):
        """
        Check if wait requests are pending to be processed.

        Process pending wait requests and send the data_ids if number of data_ids that are
        ready are equal to the num_returns.

        """
        i = 0
        if self.awaited_data_ids:
            while i < len(self.awaited_data_ids):
                data_id = self.awaited_data_ids[i]
                data = {"ready": self.ready, "not_ready": self.awaited_data_ids}

                if data_id in DataIDTracker.get_instance().completed_data_ids:
                    self.awaited_data_ids.remove(data_id)
                    self.ready.append(data_id)
                else:
                    i = i + 1
                if self.num_returns == len(self.ready):
                    communication.mpi_send_object(
                        communication.MPIState.get_instance().comm,
                        data,
                        communication.MPIRank.ROOT,
                    )
                    self.ready = []
                    self.awaited_data_ids = []


def monitor_loop():
    """
    Infinite monitor operations processing loop.

    Tracks the number of executed tasks.

    Notes
    -----
    The loop exits on special cancelation operation.
    ``unidist.core.backends.mpi.core.common.Operations`` defines a set of supported operations.
    """
    task_counter = TaskCounter.get_instance()
    mpi_state = communication.MPIState.get_instance()
    async_operations = AsyncOperations.get_instance()
    wait_handler = WaitHandler.get_instance()
    data_tracker = DataIDTracker.get_instance()

    while True:
        # Listen receive operation from any source
        operation_type, source_rank = communication.recv_operation_type(mpi_state.comm)
        # Proceed the request
        if operation_type == common.Operation.TASK_DONE:
            task_counter.increment()
            output_data_ids = communication.recv_simple_operation(
                mpi_state.comm, source_rank
            )
            data_tracker.add_to_completed(output_data_ids)
            wait_handler.process_wait_requests()
        elif operation_type == common.Operation.WAIT:
            data = communication.recv_simple_operation(mpi_state.comm, source_rank)
            awaited_data_ids = data["data_ids"]
            num_returns = data["num_returns"]
            wait_handler.add_wait_request(awaited_data_ids, num_returns)
            wait_handler.process_wait_requests()
        elif operation_type == common.Operation.GET_TASK_COUNT:
            # We use a blocking send here because the receiver is waiting for the result.
            communication.mpi_send_object(
                mpi_state.comm,
                task_counter.task_counter,
                source_rank,
            )
        elif operation_type == common.Operation.CANCEL:
            async_operations.finish()
            if not MPI.Is_finalized():
                MPI.Finalize()
            break  # leave event loop and shutdown monitoring
        else:
            raise ValueError("Unsupported operation!")

        # Check completion status of previous async MPI routines
        async_operations.check()
