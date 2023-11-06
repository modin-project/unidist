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
from unidist.core.backends.mpi.core.monitor.shared_memory_manager import (
    SharedMemoryManager,
)
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


mpi_state = communication.MPIState.get_instance()
logger_name = "monitor_{}".format(mpi_state.global_rank if mpi_state is not None else 0)
log_file = "{}.log".format(logger_name)
monitor_logger = common.get_logger(logger_name, log_file)


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
    Class that keeps track of all completed (ready) data IDs.
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
        Add the given data IDs to the set of completed (ready) data IDs.

        Parameters
        ----------
        data_ids : list
            List of data IDs to be added to the set of completed (ready) data IDs.
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
        Add a wait request for a list of data IDs and the number of data IDs to be awaited.

        Parameters
        ----------
        awaited_data_ids : list
            List of data IDs to be awaited.
        num_returns : int
            The number of ``DataID``-s that should be returned as ready.
        """
        self.awaited_data_ids = awaited_data_ids
        self.num_returns = num_returns

    def process_wait_requests(self):
        """
        Check if wait requests are pending to be processed.

        Process pending wait requests and send the data_ids to the requester
        if number of data IDs that are ready are equal to the num_returns.
        """
        data_id_tracker = DataIDTracker.get_instance()
        i = 0
        if self.awaited_data_ids:
            while i < len(self.awaited_data_ids):
                data_id = self.awaited_data_ids[i]
                if data_id in data_id_tracker.completed_data_ids:
                    self.ready.append(data_id)
                    self.awaited_data_ids.remove(data_id)
                    if len(self.ready) == self.num_returns:
                        operation_data = {
                            "ready": self.ready,
                            "not_ready": self.awaited_data_ids,
                        }
                        communication.mpi_send_object(
                            communication.MPIState.get_instance().global_comm,
                            operation_data,
                            communication.MPIRank.ROOT,
                        )
                        self.ready = []
                        self.awaited_data_ids = []
                else:
                    i += 1


def monitor_loop():
    """
    Infinite monitor operations processing loop.

    Tracks the number of executed tasks and completed (ready) data IDs.

    Notes
    -----
    The loop exits on special cancelation operation.
    ``unidist.core.backends.mpi.core.common.Operations`` defines a set of supported operations.
    """
    task_counter = TaskCounter.get_instance()
    mpi_state = communication.MPIState.get_instance()
    wait_handler = WaitHandler.get_instance()
    data_id_tracker = DataIDTracker.get_instance()
    shared_store = SharedObjectStore.get_instance()
    shm_manager = SharedMemoryManager()

    workers_ready_to_shutdown = []
    shutdown_workers = False
    # Once all workers excluding ``Root`` and ``Monitor`` ranks are ready to shutdown,
    # ``Monitor` sends the shutdown signal to every worker, as well as notifies ``Root`` that
    # it can exit the program.
    # Barrier to check if monitor process is ready to start the communication loop
    mpi_state.comm.Barrier()
    monitor_logger.debug("Monitor loop started")
    while True:
        # Listen receive operation from any source
        operation_type, source_rank = communication.mpi_recv_operation(
            mpi_state.global_comm
        )
        monitor_logger.debug(
            f"common.Operation processing - {operation_type} from {source_rank} rank"
        )
        # Proceed the request
        if operation_type == common.Operation.TASK_DONE:
            task_counter.increment()
            output_data_ids = communication.mpi_recv_object(
                mpi_state.global_comm, source_rank
            )
            data_id_tracker.add_to_completed(output_data_ids)
            wait_handler.process_wait_requests()
        elif operation_type == common.Operation.WAIT:
            # TODO: WAIT request can be received from several workers,
            # but not only from master. Handle this case when requested.
            operation_data = communication.mpi_recv_object(
                mpi_state.global_comm, source_rank
            )
            awaited_data_ids = operation_data["data_ids"]
            num_returns = operation_data["num_returns"]
            wait_handler.add_wait_request(awaited_data_ids, num_returns)
            wait_handler.process_wait_requests()
        elif operation_type == common.Operation.GET_TASK_COUNT:
            # We use a blocking send here because the receiver is waiting for the result.
            communication.mpi_send_object(
                mpi_state.global_comm,
                task_counter.task_counter,
                source_rank,
            )
        elif operation_type == common.Operation.RESERVE_SHARED_MEMORY:
            request = communication.mpi_recv_object(mpi_state.global_comm, source_rank)
            reservation_info = shm_manager.get(request["id"])
            if reservation_info is None:
                reservation_info = shm_manager.put(request["id"], request["size"])
                is_first_request = True
            else:
                is_first_request = False

            communication.mpi_send_object(
                mpi_state.global_comm,
                data={**reservation_info, "is_first_request": is_first_request},
                dest_rank=source_rank,
            )
        elif operation_type == common.Operation.REQUEST_SHARED_DATA:
            info_package = communication.mpi_recv_object(
                mpi_state.global_comm, source_rank
            )
            data_id = info_package["id"]
            if data_id is None:
                raise ValueError("Requested DataID is None")
            reservation_info = shm_manager.get(data_id)
            if reservation_info is None:
                raise RuntimeError(f"The monitor does not know the data id {data_id}")
            sh_buf = shared_store.get_shared_buffer(
                reservation_info["first_index"], reservation_info["last_index"]
            )
            communication.mpi_send_buffer(
                mpi_state.global_comm,
                sh_buf,
                dest_rank=source_rank,
                data_type=MPI.BYTE,
            )
        elif operation_type == common.Operation.CLEANUP:
            cleanup_list = communication.recv_serialized_data(
                mpi_state.global_comm, source_rank
            )
            cleanup_list = [common.MpiDataID(*tpl) for tpl in cleanup_list]
            shm_manager.clear(cleanup_list)
        elif operation_type == common.Operation.READY_TO_SHUTDOWN:
            workers_ready_to_shutdown.append(source_rank)
            shutdown_workers = len(workers_ready_to_shutdown) == len(mpi_state.workers)
        elif operation_type == common.Operation.SHUTDOWN:
            SharedObjectStore.get_instance().finalize()
            if not MPI.Is_finalized():
                MPI.Finalize()
            break  # leave event loop and shutdown monitoring
        else:
            raise ValueError(f"Unsupported operation: {operation_type}")

        if shutdown_workers:
            for rank_id in mpi_state.workers + mpi_state.monitor_processes:
                if rank_id != mpi_state.global_rank:
                    communication.mpi_send_operation(
                        mpi_state.global_comm,
                        common.Operation.SHUTDOWN,
                        rank_id,
                    )
            communication.mpi_send_object(
                mpi_state.global_comm,
                common.Operation.SHUTDOWN,
                communication.MPIRank.ROOT,
            )
            SharedObjectStore.get_instance().finalize()
            if not MPI.Is_finalized():
                MPI.Finalize()
            break  # leave event loop and shutdown monitoring
