# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API of Python backend."""


from unidist.core.backends.common.data_id import DataID


def wrap(data):
    """
    Wrap data in ``DataID``.

    Parameters
    ----------
    data : object
        Data to be wrapped.

    Returns
    -------
    unidist.core.backends.common.data_id.DataID
    """
    return DataID(data)


def unwrap(data_ids):
    """
    Unwrap object(s) from `data_ids`.

    Parameters
    ----------
    data_ids : unidist.core.backends.common.data_id.DataID or list
        ID(s) of object(s) to be unwrapped.

    Returns
    -------
    object
        A Python object.
    """
    is_list = isinstance(data_ids, list)
    if not is_list:
        data_ids = [data_ids]
    if not all(isinstance(data_id, DataID) for data_id in data_ids):
        raise ValueError("`data_ids` must either be a data ID or a list of data IDs.")

    def check_exception(value):
        if isinstance(value, Exception):
            raise value
        return value

    values = [check_exception(data_id._id) for data_id in data_ids]

    return values if is_list else values[0]


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
    materialized_args = [
        unwrap(arg) if isinstance(arg, DataID) else arg for arg in args
    ]
    materialized_kwargs = {
        key: unwrap(value) if isinstance(value, DataID) else value
        for key, value in kwargs.items()
    }

    try:
        result = func(*materialized_args, **materialized_kwargs)
    except Exception as e:
        result = [e] * num_returns if num_returns > 1 else e

    if num_returns == 0:
        data_ids = None
    elif num_returns > 1:
        data_ids = [DataID(result[idx]) for idx in range(num_returns)]
    else:
        data_ids = DataID(result)

    return data_ids
