# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize execution backend."""

from unidist.config import Backend
from .backend import BackendProxy


def init_backend():
    """
    Initialize an execution backend.

    Notes
    -----
    The concrete execution backend can be set via
    `UNIDIST_BACKEND` environment variable or ``Backend`` config value.
    Ray backend is used by default.
    """
    backend_name = Backend.get()

    if backend_name == "Ray":
        from unidist.core.backends.ray.backend import RayBackend
        from unidist.core.backends.ray.utils import initialize_ray

        initialize_ray()
        backend_cls = RayBackend()
    elif backend_name == "Dask":
        import threading

        if threading.current_thread() is threading.main_thread():
            from unidist.core.backends.dask.backend import DaskBackend
            from unidist.core.backends.dask.utils import initialize_dask

            initialize_dask()
            backend_cls = DaskBackend()
    elif backend_name == "Mpi":
        from unidist.core.backends.mpi.backend import MPIBackend
        from unidist.core.backends.mpi.utils import initialize_mpi

        initialize_mpi()
        backend_cls = MPIBackend()
    elif backend_name == "Multiprocessing":
        from unidist.core.backends.multiprocessing.backend import MultiProcessingBackend
        from unidist.core.backends.multiprocessing.utils import (
            initialize_multiprocessing,
        )

        initialize_multiprocessing()
        backend_cls = MultiProcessingBackend()
    elif backend_name == "Python":
        from unidist.core.backends.python.backend import PythonBackend
        from unidist.core.backends.python.utils import initialize_python

        initialize_python()
        backend_cls = PythonBackend()
    else:
        raise ImportError("Unrecognized execution backend.")

    BackendProxy.get_instance(backend_cls=backend_cls)


def get_backend_proxy():
    """
    Get proxy object of the backend through which operations will be performed.

    Returns
    -------
    Backend
        The ``Backend`` instance that is considered as the proxy object.
    """
    backend = BackendProxy.get_instance()

    if backend is None:

        backend_name = Backend.get()
        if backend_name == "Ray":
            from unidist.core.backends.ray.backend import RayBackend

            backend_cls = RayBackend()
        elif backend_name == "Dask":
            from unidist.core.backends.dask.backend import DaskBackend

            backend_cls = DaskBackend()
        elif backend_name == "Mpi":
            from unidist.core.backends.mpi.backend import MPIBackend

            backend_cls = MPIBackend()
        elif backend_name == "Multiprocessing":
            from unidist.core.backends.multiprocessing.backend import (
                MultiProcessingBackend,
            )

            backend_cls = MultiProcessingBackend()
        elif backend_name == "Python":
            from unidist.core.backends.python.backend import PythonBackend

            backend_cls = PythonBackend()
        else:
            raise ValueError("Unrecognized execution backend.")

        backend = BackendProxy.get_instance(backend_cls=backend_cls)

    return backend
