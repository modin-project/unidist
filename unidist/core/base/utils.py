# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize execution backend."""

from unidist.config import Backend
from unidist.core.base.common import BackendName
from .backend import BackendProxy


def init_backend():
    """
    Initialize an execution backend.

    Notes
    -----
    The concrete execution backend can be set via
    `UNIDIST_BACKEND` environment variable or ``Backend`` config value.
    MPI backend is used by default.
    """
    backend_name = Backend.get()

    if backend_name == BackendName.MPI:
        from unidist.core.backends.mpi.backend import MPIBackend
        from unidist.core.backends.mpi.utils import initialize_mpi

        initialize_mpi()
        backend_cls = MPIBackend()
    elif backend_name == BackendName.DASK:
        import threading

        if threading.current_thread() is threading.main_thread():
            from unidist.core.backends.dask.backend import DaskBackend
            from unidist.core.backends.dask.utils import initialize_dask

            initialize_dask()
            backend_cls = DaskBackend()
    elif backend_name == BackendName.RAY:
        from unidist.core.backends.ray.backend import RayBackend
        from unidist.core.backends.ray.utils import initialize_ray

        initialize_ray()
        backend_cls = RayBackend()
    elif backend_name == BackendName.PYMP:
        from unidist.core.backends.pymp.backend import PyMpBackend
        from unidist.core.backends.pymp.utils import (
            initialize_pymp,
        )

        initialize_pymp()
        backend_cls = PyMpBackend()
    elif backend_name == BackendName.PYSEQ:
        from unidist.core.backends.pyseq.backend import PySeqBackend
        from unidist.core.backends.pyseq.utils import initialize_pyseq

        initialize_pyseq()
        backend_cls = PySeqBackend()
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
        if backend_name == BackendName.MPI:
            from unidist.core.backends.mpi.backend import MPIBackend

            backend_cls = MPIBackend()
        elif backend_name == BackendName.DASK:
            from unidist.core.backends.dask.backend import DaskBackend

            backend_cls = DaskBackend()
        elif backend_name == BackendName.RAY:
            from unidist.core.backends.ray.backend import RayBackend

            backend_cls = RayBackend()
        elif backend_name == BackendName.PYMP:
            from unidist.core.backends.pymp.backend import (
                PyMpBackend,
            )

            backend_cls = PyMpBackend()
        elif backend_name == BackendName.PYSEQ:
            from unidist.core.backends.pyseq.backend import PySeqBackend

            backend_cls = PySeqBackend()
        else:
            raise ValueError("Unrecognized execution backend.")

        backend = BackendProxy.get_instance(backend_cls=backend_cls)

    return backend
