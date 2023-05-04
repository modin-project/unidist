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
    completed_data_ids = set()
    waited_data_ids = []
    ready = []
    num_returns = 0

    while True:
        # Listen receive operation from any source
        data, source_rank = communication.recv_operation_type(mpi_state.comm)
        operation_type = data["operation_type"] if isinstance(data, dict) else data
        # Proceed the request
        if operation_type == common.Operation.TASK_DONE:
            task_counter.increment()
            output_data_ids = data["output_data_ids"]
            # raise ValueError("==== = {} not ready =".format(type(output_data_ids[0])))
            completed_data_ids.update(output_data_ids)

            if waited_data_ids:
                for data_id in waited_data_ids:
                    data = {"ready": ready, "not_ready": waited_data_ids}

                    if data_id in completed_data_ids:
                        waited_data_ids.remove(data_id)
                        ready.append(data_id)
                    if num_returns == len(ready):
                        communication.mpi_send_object(
                            mpi_state.comm,
                            data,
                            communication.MPIRank.ROOT,
                        )
                        ready = []
                        waited_data_ids = []

        elif operation_type == common.Operation.WAIT:
            waited_data_ids = data["data_ids"]
            num_returns = data["num_returns"]
            if waited_data_ids:
                for data_id in waited_data_ids:
                    if data_id in completed_data_ids:
                        waited_data_ids.remove(data_id)
                        ready.append(data_id)
                    if num_returns == len(ready):
                        data = {"ready": ready, "not_ready": waited_data_ids}
                        communication.mpi_send_object(
                            mpi_state.comm,
                            data,
                            source_rank,
                        )
                        ready = []
                        waited_data_ids = []
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
