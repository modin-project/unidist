# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of MultiProcessing backend."""

import cloudpickle as pkl

from unidist.config import CpuCount
from unidist.core.backends.multiprocessing.core.object_store import ObjectStore, Delayed
from unidist.core.backends.multiprocessing.core.process_manager import (
    ProcessManager,
    Task,
)


# The global variable is responsible for if Multiprocessing backend has already been initialized
is_multiprocessing_initialized = False


def init(num_workers=CpuCount.get()):
    """
    Initialize shared object storage and workers pool.

    Parameters
    ----------
    num_workers : int, default: number of CPUs
        Number of worker-processes to start.

    Notes
    -----
    Run initialization of singleton objects ``unidist.core.backends.multiprocessing.core.object_store.ObjectStore``
    and ``unidist.core.backends.multiprocessing.core.process_manager.ProcessManager``.
    """
    ObjectStore.get_instance()
    ProcessManager.get_instance(num_workers=num_workers)
    global is_multiprocessing_initialized
    if not is_multiprocessing_initialized:
        is_multiprocessing_initialized = True


def is_initialized():
    """
    Check if Multiprocessing backend has already been initialized.

    Returns
    -------
    bool
        True or False.
    """
    global is_multiprocessing_initialized
    return is_multiprocessing_initialized


def put(data):
    """
    Put data into shared object storage.

    Parameters
    ----------
    data : object
        Data to be put.

    Returns
    -------
    unidist.core.backends.common.data_id.DataID
        An ID of object in shared object storage.
    """
    return ObjectStore.get_instance().put(data)


def get(data_ids):
    """
    Get a object(s) associated with `data_ids` from the shared object storage.

    Parameters
    ----------
    data_ids : unidist.core.backends.common.data_id.DataID or list
        An ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """
    return ObjectStore.get_instance().get(data_ids)


def wait(data_ids, num_returns=1):
    """
    Wait until `data_ids` are finished.

    This method returns two lists. The first list consists of
    ``DataID``-s that correspond to objects that completed computations.
    The second list corresponds to the rest of the ``DataID``-s (which may or may not be ready).

    Parameters
    ----------
    data_ids : unidist.core.backends.common.data_id.DataID or list
        ``DataID`` or list of ``DataID``-s to be waited.
    num_returns : int, default: 1
        The number of ``DataID``-s that should be returned as ready.

    Returns
    -------
    tuple
        List of data IDs that are ready and list of the remaining data IDs.
    """
    return ObjectStore.get_instance().wait(data_ids, num_returns=num_returns)


def submit(func, *args, num_returns=1, **kwargs):
    """
    Execute function in a worker process.

    Parameters
    ----------
    func : callable
        Function to be executed in the worker.
    *args : iterable
        Positional arguments to be passed in the `func`.
    num_returns : int, default: 1
        Number of results to be returned from `func`.
    **kwargs : dict
        Keyword arguments to be passed in the `func`.

    Returns
    -------
    unidist.core.backends.common.data_id.DataID, list or None
        Type of returns depends on `num_returns` value:

        * if `num_returns == 1`, ``DataID`` will be returned.
        * if `num_returns > 1`, list of ``DataID``-s will be returned.
        * if `num_returns == 0`, ``None`` will be returned.
    """
    obj_store = ObjectStore.get_instance()

    if num_returns == 0:
        data_ids = None
    elif num_returns > 1:
        data_ids = [obj_store.put(Delayed()) for _ in range(num_returns)]
    else:
        data_ids = obj_store.put(Delayed())

    task = Task(func, data_ids, obj_store, *args, **kwargs)

    ProcessManager.get_instance().submit(pkl.dumps(task))

    return data_ids
