# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Worker MPI process task processing functionality."""

import time
from collections import defaultdict

from unidist.core.backends.common.data_id import is_data_id

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication

# MPI stuff
comm, rank, world_size = communication.get_mpi_state()

# Logger configuration
log_file = 'worker_{}.log'.format(rank)
w_logger = common.get_logger('worker', log_file)

class ObjectStore:
    """
    Class that stores local objects and provides access to them.

    Notes
    -----
    For now, the storage is local to the current worker process only.
    """

    def __init__(self):
        # Add local data {DataId : Data}
        self._data_map = {}
        # Data owner {DataId : Rank}
        self._data_owner_map = {}

    def put(self, data_id, data):
        """
        Put `data` to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        data : object
            Data to be put.
        """
        self._data_map[data_id] = data

    def put_data_owner(self, data_id, rank):
        """
        Put data location (owner rank) to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        rank : int
            Rank number where the data resides.
        """
        self._data_owner_map[data_id] = rank

    def get(self, data_id):
        """
        Get the data from a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        object
            Return local data associated with `data_id`.
        """
        return self._data_map[data_id]

    def get_data_owner(self, data_id):
        """
        Get the data owner rank.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        int
            Rank number where the data resides.
        """
        return self._data_owner_map[data_id]

    def contains(self, data_id):
        """
        Check if the data associated with `data_id` exists in a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            Return the status if an object exist in local dictionary.
        """
        return data_id in self._data_map

    def contains_data_owner(self, data_id):
        """
        Check if the data location info associated with `data_id` exists in a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            Return the ``True`` status if an object location is known.
        """
        return data_id in self._data_owner_map

    def clear(self, cleanup_list):
        """
        Clear all local dictionary data ID instances from `cleanup_list`.

        Parameters
        ----------
        cleanup_list : list
            List of data IDs.
        """
        for data_id in cleanup_list:
            if data_id in self._data_map:
                w_logger.debug("CLEANUP DataMap id {}".format(data_id._id))
                del self._data_map[data_id]
            if data_id in self._data_owner_map:
                w_logger.debug("CLEANUP DataOwnerMap id {}".format(data_id._id))
                del self._data_owner_map[data_id]

class RequestStore:
    """
    Class that stores data requests that couldn't be satisfied now.

    Notes
    -----
    Supports GET and WAIT requests.
    """
    REQ_DATA = 0
    REQ_WAIT = 1
    REQ_DATA_CACHE = 2

    def __init__(self):
        # Data requests {DataId : [ Set of Ranks ]}
        self._data_request = defaultdict(set)
        # Wait requests {DataId : Rank}
        self._wait_request = {}
        # Cache for already requested ids
        self._data_request_cache = set()

    def put(self, data_id, rank, request_type):
        """
        Save request type for this data ID for later processing.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        rank : int
            Source rank requester.
        request_type : int
            Type of request.
        """
        if request_type == RequestStore.REQ_DATA:
            self._data_request[data_id].add(rank)
        elif request_type == RequestStore.REQ_WAIT:
            self._wait_request[data_id] = rank
        elif request_type == RequestStore.REQ_DATA_CACHE:
            self._data_request_cache.add(data_id)
        else:
            raise ValueError("Unsupported request type option!")

    def is_already_requested(self, data_id):
        """
        Check if particular `data_id` was already requested from another MPI process.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            ``True`` if communnication request was happened for this ID.
        """
        return data_id in self._data_request_cache

    def clear_cache(self, data_id):
        """
        Clear internal cache for happened communication requests.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        """
        self._data_request_cache.discard(data_id)

    def check_pending_get_requests(self, data_ids):
        """
        Check if `GET` event on this `data_ids` is waiting to be processed.

        Process the request if data ID available in local object store.

        Parameters
        ----------
        data_id : iterable or unidist.core.backends.common.data_id.DataID
            An ID or list of IDs to data.
        """
        if isinstance(data_ids, (list, tuple)):
            for data_id in data_ids:
                if data_id in self._data_request:
                    ranks_with_get_request = self._data_request[data_id]
                    for rank_num in ranks_with_get_request:
                        # Data is already in DataMap, so not problem here
                        process_get_request(rank_num, data_id)
                    del self._data_request[data_id]
        else:
            if data_ids in self._data_request:
                ranks_with_get_request = self._data_request[data_ids]
                for rank_num in ranks_with_get_request:
                    # Data is already in DataMap, so not problem here
                    process_get_request(rank_num, data_ids)
                del self._data_request[data_ids]

    def check_pending_wait_requests(self, data_ids):
        """
        Check if `WAIT` event on this `data_ids` is waiting to be processed.

        Process the request if data ID available in local object store.
        Send signal without any data.

        Parameters
        ----------
        data_id : iterable or unidist.core.backends.common.data_id.DataID
            An ID or list of IDs to data.
        """
        if isinstance(data_ids, (list, tuple)):
            for data_id in data_ids:
                if data_id in self._wait_request:
                    # Data is already in DataMap, so not problem here
                    comm.send(data_id, dest=communication.MPIRank.ROOT)
                    del self._wait_request[data_id]
        else:
            if data_ids in self._wait_request:
                comm.send(data_ids, dest=communication.MPIRank.ROOT)
                del self._wait_request[data_ids]

class TaskStore:
    """Class that stores tasks/actor-tasks that couldn't be executed due to data dependencies."""
    
    def __init__(self):
        # Incomplete tasks list - arguments not ready yet
        self._pending_tasks_list = []
        # Incomplete actor tasks list - arguments not ready yet
        self._pending_actor_tasks_list = []

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
                pending_request = process_task_request(request)
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
                pending_request = process_task_request(request)
                if pending_request:
                    updated_list.append(pending_request)
            self._pending_actor_tasks_list = updated_list

class AsyncOperations:
    """
    Class that stores MPI async communication handlers.

    Class holds a reference to sending data to prolong data lifetime during send operation.
    """
    
    def __init__(self):
        # I-prefixed mpi call handlers
        self._send_async_handlers = []

    def extend(self, handlers_list):
        """
        Extend internal list with `handler_list`.

        Parameters
        ----------
        handler_list : list
            A list of pairs with handler and data reference.
        """
        self._send_async_handlers.extend(handlers_list)

    def check(self):
        """Check all MPI async send requests readiness and remove a reference to sending data."""
        def is_ready(handler):
            is_ready = handler.Test()
            if is_ready:
                w_logger.debug("CHECK ASYNC HANDLER {} - ready".format(handler))
            else:
                w_logger.debug("CHECK ASYNC HANDLER {} - not ready".format(handler))
            return is_ready

        # tup[0] - mpi async send handler object
        self._send_async_handlers[:] = [tup for tup in self._send_async_handlers if not is_ready(tup[0])]

    def finish(self):
        """Cancel all MPI async send requests."""
        for handler, data in self._send_async_handlers:
            w_logger.debug("WAIT ASYNC HANDLER {}".format(handler))
            handler.Cancel()
            handler.Wait()
        self._send_async_handlers.clear()

# Local data

object_store = ObjectStore()
request_store = RequestStore()
pending_store = TaskStore()

async_operations = AsyncOperations()

# Actors map {handle : actor}
ActorsMap = {}

# --------------------------------------------------------------------------------------
# Operations implementation
# --------------------------------------------------------------------------------------

def process_wait_request(data_id):
    """
    Satisfy WAIT operation request from another process.

    Save request for later processing if `data_id` is not available currently.

    Parameters
    ----------
    data_id : unidist.core.backends.common.data_id.DataID
        Chech if `data_id` is available in object store.

    Notes
    -----
    Only ROOT rank is supported for now, therefore no rank argument needed.
    """
    if object_store.contains(data_id):
        # Executor wait just for signal
        comm.send(data_id, dest=communication.MPIRank.ROOT)
        w_logger.debug("Wait data {} id is ready".format(data_id._id))
    else:
        request_store.put(data_id, communication.MPIRank.ROOT, RequestStore.REQ_WAIT)
        w_logger.debug("Pending wait request {} id".format(data_id._id))

def process_get_request(source_rank, data_id):
    """
    Satisfy GET operation request from another process.

    Save request for later processing if `data_id` is not available currently.

    Parameters
    ----------
    source_rank : int
        Rank number to send data to.
    data_id: unidist.core.backends.common.data_id.DataID
        `data_id` associated data to request

    Notes
    -----
    Request is asynchronous, no wait for the data sending.
    """
    if object_store.contains(data_id):
        if source_rank == communication.MPIRank.ROOT:
            # Master is blocked by request and has no event loop, no need for OP type
            operation_data = object_store.get(data_id)
            communication.send_complex_data(comm, operation_data, source_rank)
        else:
            operation_type = common.Operation.PUT_DATA
            operation_data = {
                "id": data_id,
                "source": rank,
                "data": object_store.get(data_id),
            }
            # Async send to avoid possible dead-lock between workers
            h_list = communication.isend_complex_operation(comm, operation_type, operation_data, source_rank)
            async_operations.extend(h_list)

        w_logger.debug("Send requested {} id to {} rank - PROCESSED".format(data_id._id, source_rank))
    else:
        w_logger.debug("Pending request {} id to {} rank".format(data_id._id, source_rank))
        request_store.put(data_id, source_rank, RequestStore.REQ_DATA)

def request_worker_data(dest_rank, data_id):
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
    w_logger.debug("Request {} id from {} worker rank".format(data_id._id, dest_rank))

    operation_type = common.Operation.GET
    operation_data = {"source": rank, "id": data_id}
    communication.send_simple_operation(comm, operation_type, operation_data, dest_rank)

    # Save request in order to prevent massive communication during pending task checks
    request_store.put(data_id, dest_rank, RequestStore.REQ_DATA_CACHE)

def execute_received_task(output_data_ids, task, args, kwargs):
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
        output_values = task(*args, **kwargs)

        w_logger.info("Task evaluation time: {}".format(time.perf_counter() - start))
        w_logger.debug("- End task execution -")

    except Exception as e:
        w_logger.debug("Exception - {}".format(e))

        if isinstance(output_data_ids, (list, tuple)) and len(output_data_ids) > 1:
            for output_id in output_data_ids:
                object_store.put(output_id, e)
        else:
            object_store.put(output_data_ids, e)
    else:
        # TODO: check if None case works correctly
        if output_values is not None:
            if isinstance(output_data_ids, (list, tuple)) and len(output_data_ids) > 1:
                for output_id, value in zip(output_data_ids, output_values):
                    object_store.put(output_id, value)
            else:
                object_store.put(output_data_ids, output_values)
    # Monitor the task execution
    communication.mpi_send_object(comm, common.Operation.TASK_DONE, communication.MPIRank.MONITOR)


def unwrap_local_data_id(arg):
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
        if object_store.contains(arg):
            value = object_store.get(arg)
            # Data is already local or was pushed from master
            return value, False
        elif object_store.contains_data_owner(arg):
            if not request_store.is_already_requested(arg):
                # Request the data from an owner worker
                owner_rank = object_store.get_data_owner(arg)
                if owner_rank != rank:
                    request_worker_data(owner_rank, arg)
            return arg, True
        else:
            raise ValueError("DataID is missing!")
    else:
        return arg, False

def process_task_request(request):
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
    w_logger.debug("REMOTE outputs: {}".format(common.unwrapped_data_ids_list(output_ids)))

    # DataID -> real data
    args, is_pending = common.materialize_data_ids(args, unwrap_local_data_id)
    kwargs, is_kw_pending = common.materialize_data_ids(kwargs, unwrap_local_data_id)

    w_logger.debug("Is pending - {}".format(is_pending))

    if is_pending or is_kw_pending:
        request["args"] = args
        request["kwargs"] = kwargs
        return request
    else:
        execute_received_task(output_ids, task, args, kwargs)
        if output_ids is not None:
            request_store.check_pending_get_requests(output_ids)
            request_store.check_pending_wait_requests(output_ids)
        return None

# --------------------------------------------------------------------------------------
# Worker event processing loop
# --------------------------------------------------------------------------------------

def event_loop():
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
        operation_type, source_rank = communication.recv_operation_type(comm)
        w_logger.debug("common.Operation processing - {}".format(operation_type))

        # Proceed the request
        if operation_type == common.Operation.EXECUTE:
            request = communication.recv_complex_operation(comm, source_rank)

            # Execute the task if possible
            pending_request = process_task_request(request)
            if pending_request:
                pending_store.put(pending_request)
            else:
                # Check pending requests. Maybe some data became available.
                pending_store.check_pending_tasks()

        elif operation_type == common.Operation.GET:
            request = communication.recv_simple_operation(comm, source_rank)
            process_get_request(request["source"], request["id"])

        elif operation_type == common.Operation.PUT_DATA:
            request = communication.recv_complex_operation(comm, source_rank)
            w_logger.debug("PUT (RECV) {} id from {} rank".format(request["id"]._id, request["source"]))
            object_store.put(request["id"], request["data"])

            # Clear cached request to another worker, if data_id became available
            request_store.clear_cache(request["id"])

            # Check pending requests. Maybe some data became available.
            pending_store.check_pending_tasks()
            # Check pending actor requests also.
            pending_store.check_pending_actor_tasks()

        elif operation_type == common.Operation.PUT_OWNER:
            request = communication.recv_simple_operation(comm, source_rank)
            object_store.put_data_owner(request["id"], request["owner"])

            w_logger.debug("PUT_OWNER {} id is owned by {} rank".format(request["id"]._id, request["owner"]))

        elif operation_type == common.Operation.WAIT:
            request = communication.recv_simple_operation(comm, source_rank)
            w_logger.debug("WAIT for {} id".format(request["id"]._id))
            process_wait_request(request["id"])

        elif operation_type == common.Operation.ACTOR_CREATE:
            request = communication.recv_complex_operation(comm, source_rank)
            cls = request["class"]
            args = request["args"]
            kwargs = request["kwargs"]
            handler = request["handler"]

            ActorsMap[handler] = cls(*args, **kwargs)

        elif operation_type == common.Operation.ACTOR_EXECUTE:
            request = communication.recv_complex_operation(comm, source_rank)

            # Prepare the data
            method_name = request["task"]
            handler = request["handler"]
            actor_method = getattr(ActorsMap[handler], method_name)
            request["task"] = actor_method

            # Execute the actor task if possible
            pending_actor_request = process_task_request(request)
            if pending_actor_request:
                pending_store.put_actor(pending_actor_request)
            else:
                # Check pending requests. Maybe some data became available.
                pending_store.check_pending_actor_tasks()

        elif operation_type == common.Operation.CLEANUP:
            cleanup_list = communication.recv_simple_operation(comm, source_rank)
            object_store.clear(cleanup_list)

        elif operation_type == common.Operation.CANCEL:
            async_operations.finish()
            w_logger.debug("Exit worker event loop")
            break  # leave event loop and shutdown worker
        else:
            raise ValueError("Unsupported operation!")

        # Check completion status of previous async MPI routines
        async_operations.check()

# Process requests in an infinite loop
if rank != communication.MPIRank.ROOT:
    event_loop()

