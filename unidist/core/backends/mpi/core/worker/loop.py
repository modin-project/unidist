# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Worker MPI process task processing functionality."""
import asyncio
from functools import wraps, partial

from unidist.core.backends.mpi.core.controller.common import get_complex_data
from unidist.core.backends.mpi.core.shared_store import SharedStore

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.object_store import ObjectStore
from unidist.core.backends.mpi.core.worker.request_store import RequestStore
from unidist.core.backends.mpi.core.worker.task_store import TaskStore
from unidist.core.backends.mpi.core.async_operations import AsyncOperations

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402

mpi_state = communication.MPIState.get_instance()
# Logger configuration
# When building documentation we do not have MPI initialized so
# we use the condition to set "worker_0.log" in order to build it succesfully.
logger_name = "worker_{}".format(mpi_state.rank if mpi_state is not None else 0)
log_file = "{}.log".format(logger_name)
w_logger = common.get_logger(logger_name, log_file)
# Actors map {handle : actor}
actor_map = {}


# -------------- #
# Loop utilities #
# -------------- #


def async_wrap(func):
    """
    Make a decorator that allows to run a regular function asynchronously.

    Parameters
    ----------
    func : callable
        The function, which should be called like as coroutine.

    Returns
    -------
    callable
        Function-decorator.
    """

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


# ---------------------------- #
# Worker event processing loop #
# ---------------------------- #


async def worker_loop():
    """
    Infinite operations processing loop.

    Master or any worker could be the source of an operation.

    Notes
    -----
    The loop exits on special cancelation operation.
    ``unidist.core.backends.mpi.core.common.Operations`` defines a set of supported operations.
    """
    task_store = TaskStore.get_instance()
    object_store = ObjectStore.get_instance()
    request_store = RequestStore.get_instance()
    async_operations = AsyncOperations.get_instance()
    ready_to_shutdown_posted = False
    # Once the worker receives the cancel signal from ``Root`` rank,
    # it is getting to shutdown. All pending requests and communications are cancelled,
    # and the worker sends the ready to shutdown signal to ``Monitor``.
    # Once all workers excluding ``Root`` and ``Monitor`` ranks are ready to shutdown,
    # ``Monitor` sends the shutdown signal to every worker so they can exit the loop.
    while True:
        # Listen receive operation from any source
        operation_type, source_rank = await async_wrap(
            communication.mpi_recv_operation
        )(mpi_state.comm)
        w_logger.debug(
            f"common.Operation processing - {operation_type} from {source_rank} rank"
        )

        # Proceed the request
        if operation_type == common.Operation.EXECUTE:
            request = communication.recv_complex_data(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                # Execute the task if possible
                pending_request = task_store.process_task_request(request)
                if pending_request:
                    task_store.put(pending_request)
                else:
                    # Check pending requests. Maybe some data became available.
                    task_store.check_pending_tasks()

        elif operation_type == common.Operation.GET:
            request = communication.mpi_recv_object(
                mpi_state.comm,
                source_rank,
                cancel_recv=ready_to_shutdown_posted,
            )
            if request is not None and not ready_to_shutdown_posted:
                data_id = object_store.get_unique_data_id(request["id"])
                request_store.process_get_request(
                    request["source"],
                    data_id,
                    request["is_blocking_op"]
                )

        elif operation_type == common.Operation.PUT_DATA:
            request = communication.recv_complex_data(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                w_logger.debug(
                    "PUT (RECV) {} id from {} rank".format(
                        request["id"]._id, source_rank
                    )
                )
                request["id"] = object_store.get_unique_data_id(request["id"])
                object_store.put(request["id"], request["data"])

                # Discard data request to another worker, if data has become available
                request_store.discard_data_request(request["id"])

                # Check pending requests. Maybe some data became available.
                task_store.check_pending_tasks()
                # Check pending actor requests also.
                task_store.check_pending_actor_tasks()

        elif operation_type == common.Operation.PUT_OWNER:
            request = communication.mpi_recv_object(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                request["id"] = object_store.get_unique_data_id(request["id"])
                object_store.put_data_owner(request["id"], request["owner"])

                w_logger.debug(
                    "PUT_OWNER {} id is owned by {} rank".format(
                        request["id"]._id, request["owner"]
                    )
                )

        elif operation_type == common.Operation.PUT_SHARED_DATA:
            result = get_complex_data(mpi_state.comm, source_rank)
            object_store.put(result["id"], result["data"])

            # Clear cached request to another worker, if data_id became available
            request_store.discard_data_request(result["id"])

            # Check pending requests. Maybe some data became available.
            task_store.check_pending_tasks()
            # Check pending actor requests also.
            task_store.check_pending_actor_tasks()

        elif operation_type == common.Operation.WAIT:
            request = communication.mpi_recv_object(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                w_logger.debug("WAIT for {} id".format(request["id"]._id))
                request["id"] = object_store.get_unique_data_id(request["id"])
                request_store.process_wait_request(request["id"])

        elif operation_type == common.Operation.ACTOR_CREATE:
            request = communication.recv_complex_data(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                cls = request["class"]
                args = request["args"]
                kwargs = request["kwargs"]
                handler = request["handler"]
                actor_map[handler] = cls(*args, **kwargs)

        elif operation_type == common.Operation.ACTOR_EXECUTE:
            request = communication.recv_complex_data(mpi_state.comm, source_rank)
            if not ready_to_shutdown_posted:
                # Prepare the data
                method_name = request["task"]
                handler = request["handler"]
                actor_method = getattr(actor_map[handler], method_name)
                request["task"] = actor_method

                # Execute the actor task if possible
                pending_actor_request = task_store.process_task_request(request)
                if pending_actor_request:
                    task_store.put_actor(pending_actor_request)
                else:
                    # Check pending requests. Maybe some data became available.
                    task_store.check_pending_actor_tasks()

        elif operation_type == common.Operation.CLEANUP:
            cleanup_list = communication.recv_serialized_data(
                mpi_state.comm, source_rank
            )
            object_store.clear(cleanup_list)

        elif operation_type == common.Operation.CANCEL:
            task_store.clear_pending_tasks()
            task_store.clear_pending_actor_tasks()
            request_store.clear_get_requests()
            request_store.clear_wait_requests()
            async_operations.finish()
            communication.mpi_send_operation(
                mpi_state.comm,
                common.Operation.READY_TO_SHUTDOWN,
                mpi_state.get_monitor_by_worker_rank(communication.MPIRank.ROOT),
            )
            ready_to_shutdown_posted = True
        elif operation_type == common.Operation.SHUTDOWN and ready_to_shutdown_posted:
            w_logger.debug("Exit worker event loop")
            if not MPI.Is_finalized():
                MPI.Finalize()
            break  # leave event loop and shutdown worker
        else:
            raise ValueError(f"Unsupported operation: {operation_type}")

        # Check completion status of previous async MPI routines
        if not ready_to_shutdown_posted:
            async_operations.check()
