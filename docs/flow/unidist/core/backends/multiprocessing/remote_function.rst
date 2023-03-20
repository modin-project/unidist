..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MultiProcessingRemoteFunction
"""""""""""""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using Multiprocessing.

The :py:class:`~unidist.core.backends.multiprocessing.remote_function.MultiProcessingRemoteFunction` implements
internal method :py:meth:`~unidist.core.backends.multiprocessing.remote_function.MultiProcessingRemoteFunction._remote`
that transmites execution of operations to Multiprocessing.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.remote_function.MultiProcessingRemoteFunction
  :members:
  :private-members:
