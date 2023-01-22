# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of MPI backend."""

import sys
import atexit
import signal
import asyncio
from collections import defaultdict

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.core.backends.mpi.core.controller.object_store import object_store
from unidist.core.backends.mpi.core.controller.garbage_collector import (
    garbage_collector,
)
from unidist.core.backends.mpi.core.controller.common import (
    request_worker_data,
    push_data,
    RoundRobin,
)
import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.config import (
    CpuCount,
    IsMpiSpawnWorkers,
    MpiHosts,
    ValueSource,
    MpiPickleThreshold,
)


# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


logger = common.get_logger("api", "api.log")

# The topology of MPI cluster gets available when MPI initialization in `init`
topology = dict()
# The global variable is responsible for if MPI backend has already been initialized
is_mpi_initialized = False


def init():
    """
    Initialize MPI processes.

    Notes
    -----
    When initialization collect the MPI cluster topology.
    """
    is_init = MPI.Is_initialized()
    if is_init:
        thread_level = MPI.Query_thread()
        if thread_level != MPI.THREAD_MULTIPLE:
            raise RuntimeError(
                f"MPI backend supports {MPI.THREAD_MULTIPLE} thread level only, got {thread_level}"
            )
    else:
        thread_level = MPI.Init_thread()
        if thread_level < MPI.THREAD_MULTIPLE:
            raise RuntimeError(
                f"MPI backend supports {MPI.THREAD_MULTIPLE} thread level only, "
                f"but installed MPI version uses {thread_level}."
                "Please use a thread-safe MPI implementation"
            )

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    parent_comm = MPI.Comm.Get_parent()

    # path to dynamically spawn MPI processes
    if rank == 0 and parent_comm == MPI.COMM_NULL:
        if IsMpiSpawnWorkers.get():
            nprocs_to_spawn = CpuCount.get() + 1  # +1 for monitor process
            args = ["-c"]
            py_str = [
                "import unidist",
                "import unidist.config as cfg",
                "cfg.Backend.put('mpi')",
            ]
            if IsMpiSpawnWorkers.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.IsMpiSpawnWorkers.put({IsMpiSpawnWorkers.get()})"]
            if MpiHosts.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.MpiHosts.put('{MpiHosts.get()}')"]
            if CpuCount.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.CpuCount.put({CpuCount.get()})"]
            if MpiPickleThreshold.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.MpiPickleThreshold.put({MpiPickleThreshold.get()})"]
            py_str += ["unidist.init()"]
            py_str = "; ".join(py_str)
            args += [py_str]

            hosts = MpiHosts.get()
            info = MPI.Info.Create()
            if hosts:
                info.Set("hosts", hosts)

            intercomm = MPI.COMM_SELF.Spawn(
                sys.executable,
                args,
                maxprocs=nprocs_to_spawn,
                info=info,
                root=rank,
            )
            comm = intercomm.Merge(high=False)
        # path for processes to be started by mpiexec -n <N>, where N > 1
        else:
            comm = MPI.COMM_WORLD

    # path for spawned MPI processes to be merged with the parent communicator
    if parent_comm != MPI.COMM_NULL:
        comm = parent_comm.Merge(high=True)

    mpi_state = communication.MPIState.get_instance(
        comm, comm.Get_rank(), comm.Get_size()
    )

    global topology
    if not topology:
        topology = communication.get_topology()

    global is_mpi_initialized
    if not is_mpi_initialized:
        is_mpi_initialized = True

    if mpi_state.rank == communication.MPIRank.ROOT:
        atexit.register(_termination_handler)
        signal.signal(signal.SIGTERM, _termination_handler)
        signal.signal(signal.SIGINT, _termination_handler)
        return

    if mpi_state.rank == communication.MPIRank.MONITOR:
        from unidist.core.backends.mpi.core.monitor import monitor_loop

        monitor_loop()
        return

    if mpi_state.rank not in (
        communication.MPIRank.ROOT,
        communication.MPIRank.MONITOR,
    ):
        from unidist.core.backends.mpi.core.worker.loop import worker_loop

        asyncio.run(worker_loop())
        return


def is_initialized():
    """
    Check if MPI backend has already been initialized.

    Returns
    -------
    bool
        True or False.
    """
    global is_mpi_initialized
    return is_mpi_initialized


# TODO: cleanup before shutdown?
def shutdown():
    """
    Shutdown all MPI processes.

    Notes
    -----
    Sends cancelation operation to all workers and monitor processes.
    """
    mpi_state = communication.MPIState.get_instance()
    # Send shutdown commands to all ranks
    for rank_id in range(communication.MPIRank.MONITOR, mpi_state.world_size):
        communication.mpi_send_object(mpi_state.comm, common.Operation.CANCEL, rank_id)
        logger.debug("Shutdown rank {}".format(rank_id))
    if not MPI.Is_finalized():
        MPI.Finalize()


def cluster_resources():
    """
    Get resources of MPI cluster.

    Returns
    -------
    dict
        Dictionary with cluster nodes info in the form
        `{"node_ip0": {"CPU": x0}, "node_ip1": {"CPU": x1}, ...}`.
    """
    global topology
    if not topology:
        raise RuntimeError("'unidist.init()' has not been called yet")

    cluster_resources = defaultdict(dict)
    for host, ranks_list in topology.items():
        cluster_resources[host]["CPU"] = len(ranks_list)

    return dict(cluster_resources)


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

    logger.debug("PUT {} id".format(data_id._id))

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

    logger.debug("GET {} ids".format(common.unwrapped_data_ids_list(data_ids)))

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
    mpi_state = communication.MPIState.get_instance()

    def wait_impl(data_id):
        if object_store.contains(data_id):
            return

        owner_rank = object_store.get_data_owner(data_id)

        operation_type = common.Operation.WAIT
        operation_data = {"id": data_id.base_data_id()}
        communication.send_simple_operation(
            mpi_state.comm,
            operation_type,
            operation_data,
            owner_rank,
        )

        logger.debug("WAIT {} id from {} rank".format(data_id._id, owner_rank))

        communication.mpi_busy_wait_recv(mpi_state.comm, owner_rank)

    logger.debug("WAIT {} ids".format(common.unwrapped_data_ids_list(data_ids)))

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


def submit(task, *args, num_returns=1, **kwargs):
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

    dest_rank = RoundRobin.get_instance().schedule_rank()

    output_ids = object_store.generate_output_data_id(
        dest_rank, garbage_collector, num_returns
    )

    logger.debug("REMOTE OPERATION")
    logger.debug(
        "REMOTE args to {} rank: {}".format(
            dest_rank, common.unwrapped_data_ids_list(args)
        )
    )
    logger.debug(
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
        communication.MPIState.get_instance().comm,
        operation_type,
        operation_data,
        dest_rank,
    )

    # Track the task execution
    garbage_collector.increment_task_counter()

    return output_ids


# ---------------------------- #
# unidist termination handling #
# ---------------------------- #


def _termination_handler():
    shutdown()
