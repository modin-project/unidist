# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Commons used by core base functionality."""

from .object_ref import ObjectRef


class BackendName:
    """String representations of unidist backends."""

    RAY = "ray"
    MPI = "mpi"
    DASK = "dask"
    MP = "multiprocessing"
    PY = "python"


def filter_arguments(*args, **kwargs):
    """
    Filter `args` and `kwargs` so that a backend itself is able to materialize data.

    Parameters
    ----------
    *args : list or tuple
        Positional arguments to be filtered.
    **kwargs : dict
        Keyword arguments to be filtered.

    Returns
    -------
    list
        Filtered positional arguments.
    dict
        Filtered keyword arguments.

    Notes
    -----
    The method unwraps one level of nesting for `args` and `kwargs` so that
    an underlying backend is able to materialize data.
    Data materialization of the next levels of nesting is a user's burden.
    """
    args = [] if args is None else args
    kwargs = {} if kwargs is None else kwargs

    args = [arg._ref if isinstance(arg, ObjectRef) else arg for arg in args]
    kwargs = {k: (v._ref if isinstance(v, ObjectRef) else v) for k, v in kwargs.items()}
    return args, kwargs
