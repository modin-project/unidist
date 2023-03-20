# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of Python backend."""


from unidist.core.backends.common.data_id import DataID
from unidist.core.backends.python.core.object_store import ObjectStore


# The global variable is responsible for if Python backend has already been initialized
is_python_initialized = False


def init():
    """
    Initialize an object storage.

    Notes
    -----
    Run initialization of singleton object ``unidist.core.backends.python.core.object_store.ObjectStore``.
    """
    ObjectStore.get_instance()
    global is_python_initialized
    if not is_python_initialized:
        is_python_initialized = True


def is_initialized():
    """
    Check if Python backend has already been initialized.

    Returns
    -------
    bool
        True or False.
    """
    global is_python_initialized
    return is_python_initialized


def put(data):
    """
    Put data into object storage.

    Parameters
    ----------
    data : object
        Data to be put.

    Returns
    -------
    unidist.core.backends.common.data_id.DataID
        An ID of object in object storage.
    """
    return ObjectStore.get_instance().put(data)


def get(data_ids):
    """
    Get object(s) associated with `data_ids` from the object storage.

    Parameters
    ----------
    data_ids : unidist.core.backends.common.data_id.DataID or list
        ID(s) to object(s) to get data from.

    Returns
    -------
    object
        A Python object.
    """
    return ObjectStore.get_instance().get(data_ids)


def submit(func, *args, num_returns=1, **kwargs):
    """
    Execute function.

    Parameters
    ----------
    func : callable
        Function to be executed.
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

    materialized_args = [
        obj_store.get(arg) if isinstance(arg, DataID) else arg for arg in args
    ]
    materialized_kwargs = {
        key: obj_store.get(value) if isinstance(value, DataID) else value
        for key, value in kwargs.items()
    }

    try:
        result = func(*materialized_args, **materialized_kwargs)
    except Exception as e:
        result = [e] * num_returns if num_returns > 1 else e

    if num_returns == 0:
        data_ids = None
    elif num_returns > 1:
        data_ids = [obj_store.put(result[idx]) for idx in range(num_returns)]
    else:
        data_ids = obj_store.put(result)

    return data_ids
