# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Monitoring process."""

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication

# MPI stuff
comm, rank, world_size = communication.get_mpi_state()

# Global counter of executed task
task_counter = 0


def monitor_loop():
    """
    Infinite monitor operations processing loop.

    Tracks the number of executed tasks.

    Notes
    -----
    The loop exits on special cancelation operation.
    ``unidist.core.backends.mpi.core.common.Operations`` defines a set of supported operations.
    """
    global task_counter

    while True:
        # Listen receive operation from any source
        operation_type, source_rank = communication.recv_operation_type(comm)
        # Proceed the request
        if operation_type == common.Operation.TASK_DONE:
            task_counter += 1
        elif operation_type == common.Operation.GET_TASK_COUNT:
            communication.mpi_send_object(comm, task_counter, source_rank)
        elif operation_type == common.Operation.CANCEL:
            break  # leave event loop and shutdown monitoring
        else:
            raise ValueError("Unsupported operation!")


# Event loop
if rank == communication.MPIRank.MONITOR:
    monitor_loop()
