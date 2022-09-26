# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import functools
import inspect
import time

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.worker.object_store import ObjectStore
from unidist.core.backends.mpi.core.worker.request_store import RequestStore


# Logger configuration
# When building documentation we do not have MPI initialized so
# we use the condition to set "worker_0.log" in order to build it succesfully.
log_file = "worker_{}.log".format(
    communication.MPIState.get_instance().rank
    if communication.MPIState.get_instance()
    else 0
)
w_logger = common.get_logger("worker", log_file)


class TaskStore:
    """Class that stores tasks/actor-tasks that couldn't be executed due to data dependencies."""

    __instance = None

    def __init__(self):
        # Incomplete tasks list - arguments not ready yet
        self._pending_tasks_list = []
        # Incomplete actor tasks list - arguments not ready yet
        self._pending_actor_tasks_list = []
        # Event loop for executing coroutines
        self.event_loop = asyncio.get_event_loop()

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``TaskStore``.

        Returns
        -------
        TaskStore
        """
        if cls.__instance is None:
            cls.__instance = TaskStore()
        return cls.__instance

    def put(self, request):
        """
        Save task execution request for later processing.

        Some data dependencies are not resolved yet.

        Parameters
        ----------
        request : dict
            Task execution request with arguments.
        """
        self._pending_tasks_list.append(request)

    def put_actor(self, request):
        """
        Save actor task execution request for later processing.

        Some data dependencies are not resolved yet.

        Parameters
        ----------
        request : dict
            Actor task execution request with arguments.
        """
        self._pending_actor_tasks_list.append(request)

    def check_pending_tasks(self):
        """
        Check a list of pending task execution requests and process all ready tasks.

        Task is ready if all data dependencies are resolved.
        """
        w_logger.debug("Check pending tasks")

        if self._pending_tasks_list:
            updated_list = []
            for request in self._pending_tasks_list:
                pending_request = self.process_task_request(request)
                if pending_request:
                    updated_list.append(pending_request)
            self._pending_tasks_list = updated_list

    def check_pending_actor_tasks(self):
        """
        Check a list of pending actor task execution requests and process all ready tasks.

        Task is ready if all data dependencies are resolved.
        """
        w_logger.debug("Check pending actor tasks")

        if self._pending_actor_tasks_list:
            updated_list = []
            for request in self._pending_actor_tasks_list:
                pending_request = self.process_task_request(request)
                if pending_request:
                    updated_list.append(pending_request)
            self._pending_actor_tasks_list = updated_list

    def request_worker_data(self, dest_rank, data_id):
        """
        Send GET operation with data request to destination worker.

        Parameters
        ----------
        dest_rank : int
            Rank number to request data from.
        data_id : unidist.core.backends.common.data_id.DataID
            `data_id` associated data to request.

        Notes
        -----
        Request is asynchronous, no wait for the data.
        """
        w_logger.debug(
            "Request {} id from {} worker rank".format(data_id._id, dest_rank)
        )

        operation_type = common.Operation.GET
        operation_data = {
            "source": communication.MPIState.get_instance().rank,
            "id": data_id,
        }
        communication.send_simple_operation(
            communication.MPIState.get_instance().comm,
            operation_type,
            operation_data,
            dest_rank,
        )

        # Save request in order to prevent massive communication during pending task checks
        RequestStore.get_instance().put(data_id, dest_rank, RequestStore.REQ_DATA_CACHE)

    def unwrap_local_data_id(self, arg):
        """
        Inspect argument and get the ID associated data from the local object store if available.

        If the object store is missing this data ID, request the data from another worker.

        Parameters
        ----------
        arg : object or unidist.core.backends.common.data_id.DataID
            Data ID or object to inspect.

        Returns
        -------
        tuple
            Same value or data ID associated data and special flag.

        Notes
        -----
        The function returns the success status of data materialization attempt as a flag.
        If the data ID could not be resolved, the function returns ``True``.
        """
        if is_data_id(arg):
            if ObjectStore.get_instance().contains(arg):
                value = ObjectStore.get_instance().get(arg)
                # Data is already local or was pushed from master
                return value, False
            elif ObjectStore.get_instance().contains_data_owner(arg):
                if not RequestStore.get_instance().is_already_requested(arg):
                    # Request the data from an owner worker
                    owner_rank = ObjectStore.get_instance().get_data_owner(arg)
                    if owner_rank != communication.MPIState.get_instance().rank:
                        self.request_worker_data(owner_rank, arg)
                return arg, True
            else:
                raise ValueError("DataID is missing!")
        else:
            return arg, False

    def execute_received_task(self, output_data_ids, task, args, kwargs):
        """
        Execute a task/actor-task and handle results.

        Parameters
        ----------
        output_data_ids : list of unidist.core.backends.common.data_id.DataID
            A list of output data IDs to store the results in local object store.
        task : callable
            Function to be executed.
        args : iterable
            Positional arguments to be passed in the `task`.
        kwargs : dict
            Keyword arguments to be passed in the `task`.

        Notes
        -----
        Exceptions are stored in output data IDs as value.
        """
        try:
            w_logger.debug("- Start task execution -")

            for arg in args:
                if isinstance(arg, Exception):
                    raise arg
            for value in kwargs.values():
                if isinstance(value, Exception):
                    raise value

            start = time.perf_counter()

            # Execute user task
            if inspect.iscoroutinefunction(task):
                f_to_execute = functools.partial(task, *args, **kwargs)
                output_values = self.event_loop.run_until_complete(f_to_execute())
            else:
                output_values = task(*args, **kwargs)

            w_logger.info(
                "Task evaluation time: {}".format(time.perf_counter() - start)
            )
            w_logger.debug("- End task execution -")

        except Exception as e:
            w_logger.debug("Exception - {}".format(e))

            if isinstance(output_data_ids, (list, tuple)) and len(output_data_ids) > 1:
                for output_id in output_data_ids:
                    ObjectStore.get_instance().put(output_id, e)
            else:
                ObjectStore.get_instance().put(output_data_ids, e)
        else:
            # TODO: check if None case works correctly
            if output_values is not None:
                if (
                    isinstance(output_data_ids, (list, tuple))
                    and len(output_data_ids) > 1
                ):
                    for output_id, value in zip(output_data_ids, output_values):
                        ObjectStore.get_instance().put(output_id, value)
                else:
                    ObjectStore.get_instance().put(output_data_ids, output_values)
        # Monitor the task execution
        communication.mpi_send_object(
            communication.MPIState.get_instance().comm,
            common.Operation.TASK_DONE,
            communication.MPIRank.MONITOR,
        )

    def process_task_request(self, request):
        """
        Parse request data and execute the task if possible.

        Data dependencies should be resolved for task execution.

        Parameters
        ----------
        request : dict
            Task related data (args, function, output).

        Returns
        -------
        dict or None
            Same request if the task couldn`t be executed, otherwise ``None``.
        """
        # Parse request
        task = request["task"]
        args = request["args"]
        kwargs = request["kwargs"]
        output_ids = request["output"]

        w_logger.debug("REMOTE args: {}".format(common.unwrapped_data_ids_list(args)))
        w_logger.debug(
            "REMOTE outputs: {}".format(common.unwrapped_data_ids_list(output_ids))
        )

        # DataID -> real data
        args, is_pending = common.materialize_data_ids(args, self.unwrap_local_data_id)
        kwargs, is_kw_pending = common.materialize_data_ids(
            kwargs, self.unwrap_local_data_id
        )

        w_logger.debug("Is pending - {}".format(is_pending))

        if is_pending or is_kw_pending:
            request["args"] = args
            request["kwargs"] = kwargs
            return request
        else:
            self.execute_received_task(output_ids, task, args, kwargs)
            if output_ids is not None:
                RequestStore.get_instance().check_pending_get_requests(output_ids)
                RequestStore.get_instance().check_pending_wait_requests(output_ids)
            return None

    def __del__(self):
        self.event_loop.close()
