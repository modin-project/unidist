..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

DaskRemoteFunction
""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using Dask.

The :py:class:`~unidist.core.backends.dask.remote_function.DaskRemoteFunction` implements internal
method :py:meth:`~unidist.core.backends.dask.remote_function.DaskRemoteFunction._remote`
that transmites execution of operations to Dask.

API
===

.. autoclass:: unidist.core.backends.dask.remote_function.DaskRemoteFunction
  :members:
  :private-members:
