# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of MPI backend."""

import os
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

from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore
from unidist.core.backends.mpi.core.local_object_store import LocalObjectStore
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
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.config import (
    CpuCount,
    IsMpiSpawnWorkers,
    MpiHosts,
    ValueSource,
    MpiPickleThreshold,
    MpiBackoff,
    MpiLog,
    MpiSharedObjectStore,
    MpiSharedObjectStoreMemory,
    MpiSharedObjectStoreThreshold,
)


# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


logger = common.get_logger("api", "api.log")

# The global variable is responsible for if MPI backend has already been initialized
is_mpi_initialized = False
# The global variable is responsible for if MPI backend has already been shutdown
is_mpi_shutdown = False

# This should be in line with https://docs.python.org/3/library/sys.html#sys.flags
_PY_FLAGS_MAP = {
    "debug": "d",
    "inspect": "i",
    "interactive": "i",
    "isolated": "I",
    "optimize": "O",
    "dont_write_bytecode": "B",
    "no_user_site": "s",
    "no_site": "S",
    "ignore_environment": "E",
    "verbose": "v",
    "bytes_warning": "b",
    "quiet": "q",
    "hash_randomization": "R",
    "safe_path": "P",
    # The flags below are handled separately using sys._xoptions.
    # See more in https://docs.python.org/3/library/sys.html#sys._xoptions
    # 'dev_mode': 'Xdev',
    # 'utf8_mode': 'Xutf8',
    # 'int_max_str_digits': 'Xint_max_str_digits',
}


def _get_py_flags():
    """
    Get a list of the flags passed in to python.

    Returns
    -------
    list

    Notes
    -----
    This function is used to get the python flags
    in order to pass them to the workers initialization.
    """
    args = []
    for flag, opt in _PY_FLAGS_MAP.items():
        val = getattr(sys.flags, flag, 0)
        # We do not want workers to get into interactive mode
        # so the value should be 0
        val = val if opt[0] != "i" else 0
        if val > 0:
            args.append("-" + opt * val)
    for opt in sys.warnoptions:
        args.append("-W" + opt)
    sys_xoptions = getattr(sys, "_xoptions", {})
    for opt, val in sys_xoptions.items():
        args.append("-X" + opt if val is True else "-X" + opt + "=" + val)
    return args


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
            args = _get_py_flags()
            args += ["-c"]
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
            if MpiBackoff.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.MpiBackoff.put({MpiBackoff.get()})"]
            if MpiLog.get_value_source() != ValueSource.DEFAULT:
                py_str += [f"cfg.MpiLog.put({MpiLog.get()})"]
            if MpiSharedObjectStore.get_value_source() != ValueSource.DEFAULT:
                py_str += [
                    f"cfg.MpiSharedObjectStore.put({MpiSharedObjectStore.get()})"
                ]
            if MpiSharedObjectStoreMemory.get_value_source() != ValueSource.DEFAULT:
                py_str += [
                    f"cfg.MpiSharedObjectStoreMemory.put({MpiSharedObjectStoreMemory.get()})"
                ]
            if MpiSharedObjectStoreThreshold.get_value_source() != ValueSource.DEFAULT:
                py_str += [
                    f"cfg.MpiSharedObjectStoreThreshold.put({MpiSharedObjectStoreThreshold.get()})"
                ]
            py_str += ["unidist.init()"]
            py_str = "; ".join(py_str)
            args += [py_str]

            cpu_count = CpuCount.get()
            hosts = MpiHosts.get()
            info = MPI.Info.Create()
            lib_version = MPI.Get_library_version()
            if "Intel" in lib_version:
                # To make dynamic spawn of MPI processes work properly
                # we should set this environment variable.
                # See more about Intel MPI environment variables in
                # https://www.intel.com/content/www/us/en/docs/mpi-library/developer-reference-linux/2021-8/other-environment-variables.html.
                os.environ["I_MPI_SPAWN"] = "1"

            host_list = hosts.split(",") if hosts is not None else ["localhost"]
            host_count = len(host_list)
            if hosts is not None and host_count == 1:
                raise ValueError(
                    "MpiHosts cannot include only one host. If you want to run on a single node, just run the program without this option."
                )

            if common.is_shared_memory_supported():
                # +host_count to add monitor process on each host
                nprocs_to_spawn = cpu_count + host_count
            else:
                # +1 for just a single process monitor
                nprocs_to_spawn = cpu_count + 1
            if host_count > 1:
                if "Open MPI" in lib_version:
                    # +1 to take into account the current root process
                    # to correctly allocate slots
                    slot_count = nprocs_to_spawn + 1
                    slots_per_host = [
                        int(slot_count / host_count)
                        + (1 if i < slot_count % host_count else 0)
                        for i in range(host_count)
                    ]
                    hosts = ",".join(
                        [
                            f"{host_list[i]}:{slots_per_host[i]}"
                            for i in range(host_count)
                        ]
                    )
                    info.Set("add-host", hosts)
                else:
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

    mpi_state = communication.MPIState.get_instance(comm)

    global is_mpi_initialized
    if not is_mpi_initialized:
        is_mpi_initialized = True

    # Initalize shared memory
    SharedObjectStore.get_instance()

    if mpi_state.is_root_process():
        atexit.register(_termination_handler)
        signal.signal(signal.SIGTERM, _termination_handler)
        signal.signal(signal.SIGINT, _termination_handler)
        return
    elif mpi_state.is_monitor_process():
        from unidist.core.backends.mpi.core.monitor.loop import monitor_loop

        monitor_loop()
        # If the user executes a program in SPMD mode,
        # we do not want workers to continue the flow after `unidist.init()`
        # so just killing them.
        if not IsMpiSpawnWorkers.get():
            sys.exit()
        return
    else:
        from unidist.core.backends.mpi.core.worker.loop import worker_loop

        asyncio.run(worker_loop())
        # If the user executes a program in SPMD mode,
        # we do not want workers to continue the flow after `unidist.init()`
        # so just killing them.
        if not IsMpiSpawnWorkers.get():
            sys.exit()
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
    global is_mpi_shutdown
    if not is_mpi_shutdown:
        async_operations = AsyncOperations.get_instance()
        async_operations.finish()
        mpi_state = communication.MPIState.get_instance()
        # Send shutdown commands to all ranks
        for rank_id in mpi_state.workers:
            # We use a blocking send here because we have to wait for
            # completion of the communication, which is necessary for the pipeline to continue.
            communication.mpi_send_operation(
                mpi_state.comm,
                common.Operation.CANCEL,
                rank_id,
            )
            logger.debug("Shutdown rank {}".format(rank_id))
        # Make sure that monitor has sent the shutdown signal to all workers.
        op_type = communication.mpi_recv_object(
            mpi_state.comm,
            communication.MPIRank.MONITOR,
        )
        if op_type != common.Operation.SHUTDOWN:
            raise ValueError(f"Got wrong operation type {op_type}.")
        SharedObjectStore.get_instance().finalize()
        if not MPI.Is_finalized():
            MPI.Finalize()

        logger.debug("Shutdown root")
        is_mpi_shutdown = True


def cluster_resources():
    """
    Get resources of MPI cluster.

    Returns
    -------
    dict
        Dictionary with cluster nodes info in the form
        `{"node_ip0": {"CPU": x0}, "node_ip1": {"CPU": x1}, ...}`.
    """
    mpi_state = communication.MPIState.get_instance()
    if mpi_state is None:
        raise RuntimeError("'unidist.init()' has not been called yet")

    cluster_resources = defaultdict(dict)
    for host in mpi_state.topology:
        cluster_resources[host]["CPU"] = len(
            [
                r
                for r in mpi_state.topology[host].values()
                if not mpi_state.is_root_process(r)
                and not mpi_state.is_monitor_process(r)
            ]
        )

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
    local_store = LocalObjectStore.get_instance()
    shared_store = SharedObjectStore.get_instance()
    data_id = local_store.generate_data_id(garbage_collector)
    local_store.put(data_id, data)
    shared_store.put(data_id, data)

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
    local_store = LocalObjectStore.get_instance()

    def get_impl(data_id):
        if local_store.contains(data_id):
            value = local_store.get(data_id)
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
    if not isinstance(data_ids, list):
        data_ids = [data_ids]
    # Since the controller should operate MasterDataID(s),
    # we use this map to retrieve and return them
    # instead of DataID(s) received from workers.
    data_id_map = dict(zip(data_ids, data_ids))
    not_ready = data_ids
    pending_returns = num_returns
    ready = []
    local_store = LocalObjectStore.get_instance()

    logger.debug("WAIT {} ids".format(common.unwrapped_data_ids_list(data_ids)))
    for data_id in not_ready:
        if local_store.contains(data_id):
            ready.append(data_id)
            not_ready.remove(data_id)
            pending_returns -= 1
            if len(ready) == num_returns:
                return ready, not_ready

    operation_type = common.Operation.WAIT
    not_ready = [common.unwrap_data_ids(arg) for arg in not_ready]
    operation_data = {
        "data_ids": not_ready,
        "num_returns": pending_returns,
    }
    mpi_state = communication.MPIState.get_instance()
    root_monitor = mpi_state.get_monitor_by_worker_rank(communication.MPIRank.ROOT)
    # We use a blocking send and recv here because we have to wait for
    # completion of the communication, which is necessary for the pipeline to continue.
    communication.send_simple_operation(
        mpi_state.comm,
        operation_type,
        operation_data,
        root_monitor,
    )
    data = communication.mpi_recv_object(
        mpi_state.comm,
        root_monitor,
    )
    ready.extend(data["ready"])
    not_ready = data["not_ready"]
    # We have to retrieve and return MasterDataID(s)
    # in order for the controller to operate them in further operations.
    ready = [data_id_map[data_id] for data_id in ready]
    not_ready = [data_id_map[data_id] for data_id in not_ready]

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

    local_store = LocalObjectStore.get_instance()
    output_ids = local_store.generate_output_data_id(
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

    push_data(dest_rank, common.master_data_ids_to_base(task))
    push_data(dest_rank, unwrapped_args)
    push_data(dest_rank, unwrapped_kwargs)

    operation_type = common.Operation.EXECUTE
    operation_data = {
        "task": task,
        "args": unwrapped_args,
        "kwargs": unwrapped_kwargs,
        "output": common.master_data_ids_to_base(output_ids),
    }
    async_operations = AsyncOperations.get_instance()
    h_list, _ = communication.isend_complex_operation(
        communication.MPIState.get_instance().comm,
        operation_type,
        operation_data,
        dest_rank,
    )
    async_operations.extend(h_list)

    # Track the task execution
    garbage_collector.increment_task_counter()

    return output_ids


# ---------------------------- #
# unidist termination handling #
# ---------------------------- #


def _termination_handler():
    shutdown()
