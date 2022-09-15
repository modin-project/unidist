# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Worker MPI process task processing functionality."""

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.worker.object_store import ObjectStore
from unidist.core.backends.mpi.core.worker.request_store import RequestStore
from unidist.core.backends.mpi.core.worker.task_store import TaskStore
from unidist.core.backends.mpi.core.worker.async_operations import AsyncOperations

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402

# Logger configuration
log_file = "worker_{}.log".format(communication.MPIState.get_instance().rank)
w_logger = common.get_logger("worker", log_file)

# Actors map {handle : actor}
actor_map = {}

# ---------------------------- #
# Worker event processing loop #
# ---------------------------- #


def worker_loop():
    """
    Infinite operations processing loop.

    Master or any worker could be the source of an operation.

    Notes
    -----
    The loop exits on special cancelation operation.
    ``unidist.core.backends.mpi.core.common.Operations`` defines a set of supported operations.
    """
    while True:
        # Listen receive operation from any source
        operation_type, source_rank = communication.recv_operation_type(
            communication.MPIState.get_instance().comm
        )
        w_logger.debug("common.Operation processing - {}".format(operation_type))

        # Proceed the request
        if operation_type == common.Operation.EXECUTE:
            request = communication.recv_complex_data(
                communication.MPIState.get_instance().comm, source_rank
            )

            # Execute the task if possible
            pending_request = TaskStore.get_instance().process_task_request(request)
            if pending_request:
                TaskStore.get_instance().put(pending_request)
            else:
                # Check pending requests. Maybe some data became available.
                TaskStore.get_instance().check_pending_tasks()

        elif operation_type == common.Operation.GET:
            request = communication.recv_simple_operation(
                communication.MPIState.get_instance().comm, source_rank
            )
            RequestStore.get_instance().process_get_request(
                request["source"], request["id"]
            )

        elif operation_type == common.Operation.PUT_DATA:
            request = communication.recv_complex_data(
                communication.MPIState.get_instance().comm, source_rank
            )
            w_logger.debug(
                "PUT (RECV) {} id from {} rank".format(request["id"]._id, source_rank)
            )
            ObjectStore.get_instance().put(request["id"], request["data"])

            # Clear cached request to another worker, if data_id became available
            RequestStore.get_instance().clear_cache(request["id"])

            # Check pending requests. Maybe some data became available.
            TaskStore.get_instance().check_pending_tasks()
            # Check pending actor requests also.
            TaskStore.get_instance().check_pending_actor_tasks()

        elif operation_type == common.Operation.PUT_OWNER:
            request = communication.recv_simple_operation(
                communication.MPIState.get_instance().comm, source_rank
            )
            ObjectStore.get_instance().put_data_owner(request["id"], request["owner"])

            w_logger.debug(
                "PUT_OWNER {} id is owned by {} rank".format(
                    request["id"]._id, request["owner"]
                )
            )

        elif operation_type == common.Operation.WAIT:
            request = communication.recv_simple_operation(
                communication.MPIState.get_instance().comm, source_rank
            )
            w_logger.debug("WAIT for {} id".format(request["id"]._id))
            RequestStore.get_instance().process_wait_request(request["id"])

        elif operation_type == common.Operation.ACTOR_CREATE:
            request = communication.recv_complex_data(
                communication.MPIState.get_instance().comm, source_rank
            )
            cls = request["class"]
            args = request["args"]
            kwargs = request["kwargs"]
            handler = request["handler"]

            actor_map[handler] = cls(*args, **kwargs)

        elif operation_type == common.Operation.ACTOR_EXECUTE:
            request = communication.recv_complex_data(
                communication.MPIState.get_instance().comm, source_rank
            )

            # Prepare the data
            method_name = request["task"]
            handler = request["handler"]
            actor_method = getattr(actor_map[handler], method_name)
            request["task"] = actor_method

            # Execute the actor task if possible
            pending_actor_request = TaskStore.get_instance().process_task_request(
                request
            )
            if pending_actor_request:
                TaskStore.get_instance().put_actor(pending_actor_request)
            else:
                # Check pending requests. Maybe some data became available.
                TaskStore.get_instance().check_pending_actor_tasks()

        elif operation_type == common.Operation.CLEANUP:
            cleanup_list = communication.recv_serialized_data(
                communication.MPIState.get_instance().comm, source_rank
            )
            ObjectStore.get_instance().clear(cleanup_list)

        elif operation_type == common.Operation.CANCEL:
            AsyncOperations.get_instance().finish()
            w_logger.debug("Exit worker event loop")
            if not MPI.Is_finalized():
                MPI.Finalize()
            break  # leave event loop and shutdown worker
        else:
            raise ValueError("Unsupported operation!")

        # Check completion status of previous async MPI routines
        AsyncOperations.get_instance().check()
