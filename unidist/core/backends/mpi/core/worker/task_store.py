# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import functools
import inspect
import time
import weakref

from unidist.core.backends.common.data_id import is_data_id
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.local_object_store import LocalObjectStore
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore
from unidist.core.backends.mpi.core.worker.request_store import RequestStore

mpi_state = communication.MPIState.get_instance()
# Logger configuration
# When building documentation we do not have MPI initialized so
# we use the condition to set "worker_0.log" in order to build it succesfully.
logger_name = "worker_{}".format(mpi_state.global_rank if mpi_state is not None else 0)
log_file = "{}.log".format(logger_name)
w_logger = common.get_logger(logger_name, log_file)


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
        # Started async tasks
        self.background_tasks = set()
        # In some cases output of tasks can take up the memory of task arguments.
        # If these arguments are placed in shared memory, this location should not be overwritten
        # while output is being used, otherwise the output value may be corrupted.
        # {output weak ref: list of argument strong ref}
        self.output_depends = weakref.WeakKeyDictionary()

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

    def clear_pending_tasks(self):
        """
        Clear a list of pending task execution requests.
        """
        w_logger.debug("Clear pending tasks")

        self._pending_tasks_list.clear()

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

    def clear_pending_actor_tasks(self):
        """
        Clear a list of pending actor task execution requests.
        """
        w_logger.debug("Clear pending actor tasks")

        self._pending_actor_tasks_list.clear()

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
            "source": communication.MPIState.get_instance().global_rank,
            "id": data_id,
            "is_blocking_op": False,
        }
        async_operations = AsyncOperations.get_instance()
        h_list = communication.isend_simple_operation(
            communication.MPIState.get_instance().comm,
            operation_type,
            operation_data,
            dest_rank,
        )
        async_operations.extend(h_list)

        # Save request in order to prevent massive communication during pending task checks
        RequestStore.get_instance().put(data_id, dest_rank, RequestStore.DATA)

    def check_local_data_id(self, arg):
        """
        Inspect argument if the ID is available in the local object store.

        If the local object store doesn't contain this data ID,
        request the data from another worker.

        Parameters
        ----------
        arg : object or unidist.core.backends.common.data_id.DataID
            Data ID or object to inspect.

        Returns
        -------
        tuple
            Same value and special flag.

        Notes
        -----
        If the data ID could not be resolved, the function returns ``True``.
        """
        if is_data_id(arg):
            local_store = LocalObjectStore.get_instance()
            arg = local_store.get_unique_data_id(arg)
            if local_store.contains(arg):
                return arg, False
            elif local_store.contains_data_owner(arg):
                if not RequestStore.get_instance().is_data_already_requested(arg):
                    # Request the data from an owner worker
                    owner_rank = local_store.get_data_owner(arg)
                    if owner_rank != communication.MPIState.get_instance().global_rank:
                        self.request_worker_data(owner_rank, arg)
                return arg, True
            else:
                raise ValueError("DataID is missing!")
        else:
            return arg, False

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
            local_store = LocalObjectStore.get_instance()
            arg = local_store.get_unique_data_id(arg)
            if local_store.contains(arg):
                value = LocalObjectStore.get_instance().get(arg)
                # Data is already local or was pushed from master
                return value, False
            elif local_store.contains_data_owner(arg):
                if not RequestStore.get_instance().is_data_already_requested(arg):
                    # Request the data from an owner worker
                    owner_rank = local_store.get_data_owner(arg)
                    if owner_rank != communication.MPIState.get_instance().global_rank:
                        self.request_worker_data(owner_rank, arg)
                return arg, True
            else:
                raise ValueError("DataID is missing!")
        else:
            return arg, False

    def check_output_depends(self, data_ids, depends_id):
        local_store = LocalObjectStore.get_instance()
        if isinstance(data_ids, (list, tuple)):
            for data_id in data_ids:
                value = local_store.get(data_id)
                if check_data_out_of_band(value):
                    self.output_depends[data_id] = depends_id

        else:
            value = local_store.get(data_ids)
            if check_data_out_of_band(value):
                self.output_depends[data_ids] = depends_id

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
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()
        completed_data_ids = []
        if inspect.iscoroutinefunction(task):

            async def execute():
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
                    f_to_execute = functools.partial(task, *args, **kwargs)
                    output_values = await f_to_execute()

                    w_logger.info(
                        "Task evaluation time: {}".format(time.perf_counter() - start)
                    )
                    w_logger.debug("- End task execution -")

                except Exception as e:
                    w_logger.debug("Exception - {}".format(e))

                    if (
                        isinstance(output_data_ids, (list, tuple))
                        and len(output_data_ids) > 1
                    ):
                        for output_id in output_data_ids:
                            local_store.put(output_id, e)
                    else:
                        local_store.put(output_data_ids, e)
                else:
                    if output_data_ids is not None:
                        if (
                            isinstance(output_data_ids, (list, tuple))
                            and len(output_data_ids) > 1
                        ):
                            completed_data_ids = [None] * len(output_data_ids)
                            for idx, (output_id, value) in enumerate(
                                zip(output_data_ids, output_values)
                            ):
                                local_store.put(output_id, value)
                                if shared_store.should_be_shared(value):
                                    shared_store.put(output_id, value)
                                completed_data_ids[idx] = output_id
                        else:
                            local_store.put(output_data_ids, output_values)
                            if shared_store.should_be_shared(output_values):
                                shared_store.put(output_data_ids, output_values)
                            completed_data_ids = [output_data_ids]

                RequestStore.get_instance().check_pending_get_requests(output_data_ids)
                # Monitor the task execution
                # We use a blocking send here because we have to wait for
                # completion of the communication, which is necessary for the pipeline to continue.
                root_monitor = mpi_state.get_monitor_by_worker_rank(
                    communication.MPIRank.ROOT
                )
                communication.send_simple_operation(
                    communication.MPIState.get_instance().comm,
                    common.Operation.TASK_DONE,
                    completed_data_ids,
                    root_monitor,
                )

            async_task = asyncio.create_task(execute())
            # Add task to the set. This creates a strong reference.
            self.background_tasks.add(async_task)
            # To prevent keeping references to finished tasks forever,
            # make each task remove its own reference from the set after
            # completion.
            async_task.add_done_callback(self.background_tasks.discard)
        else:
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
                output_values = task(*args, **kwargs)

                w_logger.info(
                    "Task evaluation time: {}".format(time.perf_counter() - start)
                )
                w_logger.debug("- End task execution -")

            except Exception as e:
                w_logger.debug("Exception - {}".format(e))

                if (
                    isinstance(output_data_ids, (list, tuple))
                    and len(output_data_ids) > 1
                ):
                    for output_id in output_data_ids:
                        local_store.put(output_id, e)
                else:
                    local_store.put(output_data_ids, e)
            else:
                if output_data_ids is not None:
                    if (
                        isinstance(output_data_ids, (list, tuple))
                        and len(output_data_ids) > 1
                    ):
                        completed_data_ids = [None] * len(output_data_ids)
                        for idx, (output_id, value) in enumerate(
                            zip(output_data_ids, output_values)
                        ):
                            local_store.put(output_id, value)
                            if shared_store.should_be_shared(value):
                                shared_store.put(output_id, value)
                            completed_data_ids[idx] = output_id
                    else:
                        local_store.put(output_data_ids, output_values)
                        if shared_store.should_be_shared(output_values):
                            shared_store.put(output_data_ids, output_values)
                        completed_data_ids = [output_data_ids]
            RequestStore.get_instance().check_pending_get_requests(output_data_ids)
            # Monitor the task execution.
            # We use a blocking send here because we have to wait for
            # completion of the communication, which is necessary for the pipeline to continue.
            root_monitor = mpi_state.get_monitor_by_worker_rank(
                communication.MPIRank.ROOT
            )
            communication.send_simple_operation(
                communication.MPIState.get_instance().comm,
                common.Operation.TASK_DONE,
                completed_data_ids,
                root_monitor,
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
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()
        task = local_store.get(request["task"])
        args = request["args"]
        kwargs = request["kwargs"]
        output_ids = request["output"]
        if isinstance(output_ids, (list, tuple)):
            output_ids = [
                local_store.get_unique_data_id(data_id) for data_id in output_ids
            ]
        elif output_ids is not None:
            output_ids = local_store.get_unique_data_id(output_ids)

        w_logger.debug("REMOTE task: {}".format(task))
        w_logger.debug("REMOTE args: {}".format(common.unwrapped_data_ids_list(args)))
        w_logger.debug(
            "REMOTE kwargs: {}".format(common.unwrapped_data_ids_list(kwargs))
        )
        w_logger.debug(
            "REMOTE outputs: {}".format(common.unwrapped_data_ids_list(output_ids))
        )

        # DataID -> real data
        args, is_pending = common.materialize_data_ids(args, self.check_local_data_id)
        kwargs, is_kw_pending = common.materialize_data_ids(
            kwargs, self.check_local_data_id
        )

        w_logger.debug("Is pending - {}".format(is_pending))

        if is_pending or is_kw_pending:
            request["args"] = args
            request["kwargs"] = kwargs
            return request
        else:
            args, is_pending = common.materialize_data_ids(
                args, self.unwrap_local_data_id
            )
            kwargs, is_kw_pending = common.materialize_data_ids(
                kwargs, self.unwrap_local_data_id
            )

            self.execute_received_task(output_ids, task, args, kwargs)

            shared_depends = [
                arg
                for arg in request["args"]
                if is_data_id(arg) and shared_store.contains(arg)
            ]
            if shared_depends:
                self.check_output_depends(output_ids, shared_depends)
            if output_ids is not None:
                RequestStore.get_instance().check_pending_get_requests(output_ids)
                RequestStore.get_instance().check_pending_wait_requests(output_ids)
            return None

    def __del__(self):
        self.event_loop.close()


#################################################
# Check if the data owns the memory it is using #
#################################################


def check_ndarray(ndarray):
    """
    Check if the `np.ndarray` doesn't owns the memory it is using.

    Returns
    -------
    bool
        ``True`` if the `np.ndarray` doesn't own the memory it is using, ``False`` otherwise.
    """
    return not ndarray.flags.owndata


def check_pandas_index(df_index):
    """
    Check if the `pd.Index` doesn't owns the memory it is using.

    Returns
    -------
    bool
        ``True`` if the `pd.Index` doesn't own the memory it is using, ``False`` otherwise.
    """
    if df_index._is_multi:
        if any(
            check_ndarray(df_index.get_level_values(i)._data)
            for i in range(df_index.nlevels)
        ):
            return True
    else:
        if check_ndarray(df_index._data):
            return True
    return False


def check_data_out_of_band(value):
    """
    Check if the data doesn't owns the memory it is using.

    Returns
    -------
    bool
        ``True`` if the data doesn't own the memory it is using, ``False`` otherwise.

    Notes
    -----
    Only validation for `np.ndarray`, `pd.Dataframe` and `pd.Series` is currently supported.
    """
    # check numpy
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            if check_ndarray(value):
                return True
            return False
    except ImportError:
        pass

    # check pandas
    try:
        import pandas as pd

        if isinstance(value, pd.DataFrame):
            if any(block.is_view for block in value._mgr.blocks) or any(
                check_pandas_index(df_index)
                for df_index in [value.index, value.columns]
            ):
                return True
            return False

        if isinstance(value, pd.Series):
            if any(block.is_view for block in value._mgr.blocks) or check_pandas_index(
                value.index
            ):
                return True
            return False

        # TODO: Add like blocks for other pandas classes
    except ImportError:
        pass
    return False
