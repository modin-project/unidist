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

initial_worker_number = 2


class TaskCounter:
    __instance = None

    def __init__(self):
        self.task_counter = 0
        self.task_done_per_worker_unsend = {
            k: 0
            for k in range(
                initial_worker_number, communication.MPIState.get_instance().world_size
            )
        }

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

    def increment(self, rank):
        """Increment task counter by one."""
        self.task_counter += 1
        self.task_done_per_worker_unsend[rank] += 1


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

    while True:
        # Listen receive operation from any source
        operation_type, source_rank = communication.recv_operation_type(mpi_state.comm)

        # Proceed the request
        if operation_type == common.Operation.TASK_DONE:
            task_counter.increment(source_rank)

        elif operation_type == common.Operation.GET_TASK_COUNT:
            # We use a blocking send here because the receiver is waiting for the result.
            info_tasks = {
                "executed_task_counter": task_counter.task_counter,
                "tasks_completed": task_counter.task_done_per_worker_unsend,
            }
            communication.mpi_send_object(
                mpi_state.comm,
                info_tasks,
                source_rank,
            )
            task_counter.task_done_per_worker_unsend = dict.fromkeys(
                task_counter.task_done_per_worker_unsend, 0
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
