..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MPIRemoteFunction
"""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using MPI.

The :py:class:`~unidist.core.backends.mpi.remote_function.MPIRemoteFunction` implements
internal method :py:meth:`~unidist.core.backends.mpi.remote_function.MPIRemoteFunction._remote`
that transmites execution of operations to MPI.

API
===

.. autoclass:: unidist.core.backends.mpi.remote_function.MPIRemoteFunction
  :members:
  :private-members:
