# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of MPI backend."""

import weakref
import itertools
import time
from collections import defaultdict


import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication

from unidist.core.backends.mpi.core.serialization import SimpleDataSerializer
from unidist.core.backends.common.data_id import is_data_id

# MPI stuff
comm, rank, world_size = communication.get_mpi_state()


class ObjectStore:
    """
    Class that stores objects and provides access to these from master process.

    Notes
    -----
    Currently, the storage is local to the current process only.
    """

    def __init__(self):
        # Add local data {DataID : Data}
        self._data_map = weakref.WeakKeyDictionary()
        # Data owner {DataID : Rank}
        self._data_owner_map = weakref.WeakKeyDictionary()
        # Data was already sent to this ranks {DataID : [ranks]}
        self._sent_data_map = defaultdict(set)
        # Data id generator
        self._data_id_counter = 0
        # Data serialized cache
        self._serialization_cache = {}

    def put(self, data_id, data):
        """
        Put data to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data.

        Returns
        -------
        bool
            Return the status if an object location is known.
        """
        return data_id in self._data_owner_map

    def clear(self, cleanup_list):
        """
        Clear all local dictionary data ID instances from `cleanup_list`.

        Parameters
        ----------
        cleanup_list : list
            List of ``DataID``-s.

        Notes
        -----
        Only cache of sent data can be cleared - the rest are weakreferenced.
        """
        for data_id in cleanup_list:
            self._sent_data_map.pop(data_id, None)
        self._serialization_cache.clear()

    def generate_data_id(self, gc):
        """
        Generate unique ``MasterDataID`` instance.

        Parameters
        ----------
        gc : unidist.core.backends.mpi.core.executor.GarbageCollector
            Local garbage collector reference.

        Returns
        -------
        unidist.core.backends.mpi.core.common.MasterDataID
            Unique data ID instance.
        """
        data_id = self._data_id_counter
        self._data_id_counter += 1
        return common.MasterDataID(data_id, gc)

    def generate_output_data_id(self, dest_rank, gc, num_returns=1):
        """
        Generate unique list of ``unidist.core.backends.mpi.core.common.MasterDataID`` instance.

        Parameters
        ----------
        dest_rank : int
            Ranks number where generated list will be located.
        gc : unidist.core.backends.mpi.core.executor.GarbageCollector
            Local garbage collector reference.
        num_returns : int, default: 1
            Generated list size.

        Returns
        -------
        list
            A list of unique ``MasterDataID`` instances.
        """
        if num_returns == 1:
            output_ids = self.generate_data_id(gc)
            self.put_data_owner(output_ids, dest_rank)
        elif num_returns == 0:
            output_ids = None
        else:
            output_ids = []
            for _ in range(num_returns):
                output_id = self.generate_data_id(gc)
                output_ids.append(output_id)
                self.put_data_owner(output_id, dest_rank)
        return output_ids

    def cache_send_info(self, data_id, rank):
        """
        Save communication event for this `data_id` and rank.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ``ID`` to data.
        rank : int
            Rank number where the data was sent.
        """
        self._sent_data_map[data_id].add(rank)

    def is_already_sent(self, data_id, rank):
        """
        Check if communication event on this `data_id` and rank happened.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data
        rank : int
            Rank number to check.

        Returns
        -------
        bool
            ``True`` if communication event already happened.
        """
        return (data_id in self._sent_data_map) and (
            rank in self._sent_data_map[data_id]
        )

    def cache_serialized_data(self, data_id, data):
        """
        Save communication event for this `data_id` and `data`.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data.
        data : object
            Serialized data to cache.
        """
        self._serialization_cache[data_id] = data

    def is_already_serialized(self, data_id):
        """
        Check if the data on this `data_id` is already serialized.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            ``True`` if the data is already serialized.
        """
        return data_id in self._serialization_cache

    def get_serialized_data(self, data_id):
        """
        Get serialized data on this `data_id`.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        object
            Cached serialized data associated with `data_id`.
        """
        return self._serialization_cache[data_id]


class GarbageCollector:
    """
    Class that tracks deleted data IDs and cleans worker's object storage.

    Parameters
    ----------
    object_store : unidist.core.backends.mpi.executor.ObjectStore
        Reference to the local object storage.

    Notes
    -----
    Cleanup relies on internal threshold settings.
    """

    def __init__(self, object_store):
        # Cleanup frequency settings
        self._cleanup_counter = 1
        self._cleanup_threshold = 5
        self._time_threshold = 1  # seconds
        self._timestamp = 0  # seconds
        # Cleanup list of DataIDs
        self._cleanup_list = []
        self._cleanup_list_threshold = 10
        # Reference to the global object store
        self._object_store = object_store
        # Task submitted counter
        self._task_counter = 0

    def _send_cleanup_request(self, cleanup_list):
        """
        Send a list of data IDs to be deleted for each worker to cleanup local storages.

        Parameters
        ----------
        cleanup_list : list
            List of data IDs.
        """
        e_logger.debug(
            "Send cleanup list - {}".format(
                common.unwrapped_data_ids_list(cleanup_list)
            )
        )
        # Cache serialized list of data IDs
        s_cleanup_list = SimpleDataSerializer().serialize_pickle(cleanup_list)
        for rank_id in range(2, world_size):
            communication.send_serialized_operation(
                comm, common.Operation.CLEANUP, s_cleanup_list, rank_id
            )

    def increment_task_counter(self):
        """
        Track task submission number.

        Notes
        -----
        For cleanup purpose.
        """
        self._task_counter += 1

    def collect(self, data_id):
        """
        Track ID destruction.

        This ID is out of scope, it's associated data should be cleared.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data
        """
        self._cleanup_list.append(data_id)

    def regular_cleanup(self):
        """
        Cleanup all garbage collected IDs from local and all workers object storages.

        Cleanup triggers based on internal threshold settings.
        """
        e_logger.debug("Cleanup list len - {}".format(len(self._cleanup_list)))
        e_logger.debug(
            "Cleanup counter {}, threshold reached - {}".format(
                self._cleanup_counter,
                (self._cleanup_counter % self._cleanup_threshold) == 0,
            )
        )
        if len(self._cleanup_list) > self._cleanup_list_threshold:
            if self._cleanup_counter % self._cleanup_threshold == 0:

                timestamp_snapshot = time.perf_counter()
                if (timestamp_snapshot - self._timestamp) > self._time_threshold:
                    e_logger.debug("Cleanup counter {}".format(self._cleanup_counter))

                    # Compare submitted and executed tasks
                    communication.mpi_send_object(
                        comm,
                        common.Operation.GET_TASK_COUNT,
                        communication.MPIRank.MONITOR,
                    )
                    executed_task_counter = communication.recv_simple_operation(
                        comm, communication.MPIRank.MONITOR
                    )

                    e_logger.debug(
                        "Submitted task count {} vs executed task count {}".format(
                            self._task_counter, executed_task_counter
                        )
                    )
                    if executed_task_counter == self._task_counter:
                        self._send_cleanup_request(self._cleanup_list)
                        # Clear the remaining references
                        self._object_store.clear(self._cleanup_list)
                        self._cleanup_list.clear()
                        self._cleanup_counter += 1
                        self._timestamp = time.perf_counter()
            else:
                self._cleanup_counter += 1


object_store = ObjectStore()
garbage_collector = GarbageCollector(object_store)

e_logger = common.get_logger("executor", "executor.log")

# Internal functions
# -----------------------------------------------------------------------------

# Current rank for schedule (exclude MPIRank.ROOT and monitor ranks)
round_robin = itertools.cycle(range(2, world_size))


def schedule_rank():
    """
    Find the next rank for task/actor-task execution.

    Returns
    -------
    int
        A rank number.
    """
    global round_robin
    return next(round_robin)


def request_worker_data(data_id):
    """
    Get an object(s) associated with `data_id` from the object storage.

    Parameters
    ----------
    data_id : unidist.core.backends.common.data_id.DataID
        An ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """
    owner_rank = object_store.get_data_owner(data_id)

    e_logger.debug("GET {} id from {} rank".format(data_id._id, owner_rank))

    # Worker request
    operation_type = common.Operation.GET
    operation_data = {"source": rank, "id": data_id.base_data_id()}
    communication.send_simple_operation(
        comm, operation_type, operation_data, owner_rank
    )

    # Blocking get
    data = communication.recv_complex_data(comm, owner_rank)

    # Caching the result, check the protocol correctness here
    object_store.put(data_id, data)

    return data


def push_local_data(dest_rank, data_id):
    """
    Send local data associated with passed ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    data_id : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    # Check if data was already pushed
    if not object_store.is_already_sent(data_id, dest_rank):
        e_logger.debug("PUT LOCAL {} id to {} rank".format(data_id._id, dest_rank))

        # Push the local master data to the target worker directly
        operation_type = common.Operation.PUT_DATA
        if object_store.is_already_serialized(data_id):
            serialized_data = object_store.get_serialized_data(data_id)
            communication.send_operation(
                comm, operation_type, serialized_data, dest_rank, is_serialized=True
            )
        else:
            operation_data = {"id": data_id, "data": object_store.get(data_id)}
            serialized_data = communication.send_operation(
                comm, operation_type, operation_data, dest_rank, is_serialized=False
            )
            object_store.cache_serialized_data(data_id, serialized_data)
        #  Remember pushed id
        object_store.cache_send_info(data_id, dest_rank)


def push_data_owner(dest_rank, data_id):
    """
    Send data location associated with data ID to target rank.

    Parameters
    ----------
    dest_rank : int
        Target rank.
    value : unidist.core.backends.mpi.core.common.MasterDataID
        An ID to data.
    """
    operation_type = common.Operation.PUT_OWNER
    operation_data = {
        "id": data_id,
        "owner": object_store.get_data_owner(data_id),
    }
    communication.send_simple_operation(comm, operation_type, operation_data, dest_rank)


def push_data(dest_rank, value):
    """
    Parse and send all values to destination rank.

    Process all arguments recursivelly and send all ID associated data or it's location
    to the target rank.

    Parameters
    ----------
    dest_rank : int
        Rank where task data is needed.
    value : iterable or dict or object
        Arguments to be sent.
    """
    if isinstance(value, (list, tuple)):
        for v in value:
            push_data(dest_rank, v)
    elif isinstance(value, (dict)):
        for v in value.values():
            push_data(dest_rank, v)
    elif is_data_id(value):
        if object_store.contains(value):
            push_local_data(dest_rank, value)
        elif object_store.contains_data_owner(value):
            push_data_owner(dest_rank, value)
        else:
            raise ValueError("Unknown DataID!")


# Control API
# -----------------------------------------------------------------------------

# TODO: implement
def init():
    """
    Initialize MPI processes.

    Notes
    -----
    Currently does nothing.
    """
    pass


# TODO: cleanup before shutdown?
def shutdown():
    """
    Shutdown all MPI processes.

    Notes
    -----
    Sends cancelation operation to all workers and monitor processes.
    """
    # Send shutdown commands to all ranks
    for rank_id in range(1, world_size):
        communication.mpi_send_object(comm, common.Operation.CANCEL, rank_id)
        e_logger.debug("Shutdown rank {}".format(rank_id))


# Data API
# -----------------------------------------------------------------------------


def put(data):
    """
    Put the data into object storage.

    Parameters
    ----------
    data : object
        Data to be put.

    Returns
    -------
    unidist.core.backends.mpi.core.common.MasterDataID
        An ID of an object in object storage.
    """
    data_id = object_store.generate_data_id(garbage_collector)
    object_store.put(data_id, data)

    e_logger.debug("PUT {} id".format(data_id._id))

    return data_id


def get(data_ids):
    """
    Get an object(s) associated with `data_ids` from the object storage.

    Parameters
    ----------
    data_ids : unidist.core.backends.common.data_id.DataID or list
        An ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """

    def get_impl(data_id):
        if object_store.contains(data_id):
            value = object_store.get(data_id)
        else:
            value = request_worker_data(data_id)

        if isinstance(value, Exception):
            raise value

        return value

    e_logger.debug("GET {} ids".format(common.unwrapped_data_ids_list(data_ids)))

    is_list = isinstance(data_ids, list)
    if not is_list:
        data_ids = [data_ids]

    values = [get_impl(data_id) for data_id in data_ids]

    # Initiate reference count based cleaup
    # if all the tasks were completed
    garbage_collector.regular_cleanup()

    return values if is_list else values[0]


def wait(data_ids, num_returns=1):
    """
    Wait until `data_ids` are finished.

    This method returns two lists. The first list consists of
    ``DataID``-s that correspond to objects that completed computations.
    The second list corresponds to the rest of the ``DataID``-s (which may or may not be ready).

    Parameters
    ----------
    data_ids : unidist.core.backends.mpi.core.common.MasterDataID or list
        ``DataID`` or list of ``DataID``-s to be waited.
    num_returns : int, default: 1
        The number of ``DataID``-s that should be returned as ready.

    Returns
    -------
    tuple
        List of data IDs that are ready and list of the remaining data IDs.
    """

    def wait_impl(data_id):
        if object_store.contains(data_id):
            return

        owner_rank = object_store.get_data_owner(data_id)

        operation_type = common.Operation.WAIT
        operation_data = {"id": data_id.base_data_id()}
        communication.send_simple_operation(
            comm, operation_type, operation_data, owner_rank
        )

        e_logger.debug("WAIT {} id from {} rank".format(data_id._id, owner_rank))

        communication.mpi_busy_wait_recv(comm, owner_rank)

    e_logger.debug("WAIT {} ids".format(common.unwrapped_data_ids_list(data_ids)))

    if not isinstance(data_ids, list):
        data_ids = [data_ids]

    ready = []
    not_ready = data_ids

    while len(ready) != num_returns:
        first = not_ready.pop(0)
        wait_impl(first)
        ready.append(first)

    # Initiate reference count based cleaup
    # if all the tasks were completed
    garbage_collector.regular_cleanup()

    return ready, not_ready


# Task API
# -----------------------------------------------------------------------------


def remote(task, *args, num_returns=1, **kwargs):
    """
    Execute function on a worker process.

    Parameters
    ----------
    task : callable
        Function to be executed in the worker.
    *args : iterable
        Positional arguments to be passed in the `task`.
    num_returns : int, default: 1
        Number of results to be returned from `task`.
    **kwargs : dict
        Keyword arguments to be passed in the `task`.

    Returns
    -------
    unidist.core.backends.mpi.core.common.MasterDataID or list or None
        Type of returns depends on `num_returns` value:

        * if `num_returns == 1`, ``DataID`` will be returned.
        * if `num_returns > 1`, list of ``DataID``-s will be returned.
        * if `num_returns == 0`, ``None`` will be returned.
    """
    # Initiate reference count based cleanup
    # if all the tasks were completed
    garbage_collector.regular_cleanup()

    dest_rank = schedule_rank()

    output_ids = object_store.generate_output_data_id(
        dest_rank, garbage_collector, num_returns
    )

    e_logger.debug("REMOTE OPERATION")
    e_logger.debug(
        "REMOTE args to {} rank: {}".format(
            dest_rank, common.unwrapped_data_ids_list(args)
        )
    )
    e_logger.debug(
        "REMOTE outputs to {} rank: {}".format(
            dest_rank, common.unwrapped_data_ids_list(output_ids)
        )
    )

    unwrapped_args = [common.unwrap_data_ids(arg) for arg in args]
    unwrapped_kwargs = {k: common.unwrap_data_ids(v) for k, v in kwargs.items()}

    push_data(dest_rank, unwrapped_args)
    push_data(dest_rank, unwrapped_kwargs)

    operation_type = common.Operation.EXECUTE
    operation_data = {
        "task": task,
        "args": unwrapped_args,
        "kwargs": unwrapped_kwargs,
        "output": common.master_data_ids_to_base(output_ids),
    }
    communication.send_remote_task_operation(
        comm, operation_type, operation_data, dest_rank
    )

    # Track the task execution
    garbage_collector.increment_task_counter()

    return output_ids


# Actor API
# -----------------------------------------------------------------------------


class ActorMethod:
    """
    Class responsible to execute method of an actor.

    Execute `method_name` of `actor` in a separate worker process, where ``Actor`` object is created.

    Parameters
    ----------
    actor : unidist.core.backends.mpi.core.executor.Actor
        Actor object.
    method_name : str
        The name of the method to be called.
    """

    def __init__(self, actor, method_name):
        self._actor = actor
        self._method_name = method_name

    def __call__(self, *args, num_returns=1, **kwargs):
        output_id = object_store.generate_output_data_id(
            self._actor._owner_rank, garbage_collector, num_returns
        )

        unwrapped_args = [common.unwrap_data_ids(arg) for arg in args]
        unwrapped_kwargs = {k: common.unwrap_data_ids(v) for k, v in kwargs.items()}

        push_data(self._actor._owner_rank, unwrapped_args)
        push_data(self._actor._owner_rank, unwrapped_kwargs)

        operation_type = common.Operation.ACTOR_EXECUTE
        operation_data = {
            "task": self._method_name,
            "args": unwrapped_args,
            "kwargs": unwrapped_kwargs,
            "output": common.master_data_ids_to_base(output_id),
            "handler": self._actor._handler_id.base_data_id(),
        }
        communication.send_complex_operation(
            comm, operation_type, operation_data, self._actor._owner_rank
        )

        return output_id


class Actor:
    """
    Class to execute methods of a wrapped class in a separate worker process.

    Parameters
    ----------
    cls : object
        Class to be an actor class.
    *args : iterable
        Positional arguments to be passed in `cls` constructor.
    **kwargs : dict
        Keyword arguments to be passed in `cls` constructor.

    Notes
    -----
    Instance of the `cls` will be created on the worker.
    """

    def __init__(self, cls, *args, **kwargs):
        self._owner_rank = schedule_rank()
        self._handler_id = object_store.generate_data_id(garbage_collector)

        operation_type = common.Operation.ACTOR_CREATE
        operation_data = {
            "class": cls,
            "args": args,
            "kwargs": kwargs,
            "handler": self._handler_id.base_data_id(),
        }
        communication.send_complex_operation(
            comm, operation_type, operation_data, self._owner_rank
        )

    def __getattr__(self, name):
        return ActorMethod(self, name)


# --------------------------------------------------------------------------------------
# unidist termination handling
# --------------------------------------------------------------------------------------

if rank == communication.MPIRank.ROOT:
    import atexit
    import signal

    def termination_handler():
        shutdown()

    atexit.register(termination_handler)
    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGINT, termination_handler)
