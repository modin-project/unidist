..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PyMpRemoteFunction
""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using Python Multiprocessing backend.

The :py:class:`~unidist.core.backends.pymp.remote_function.PyMpRemoteFunction` implements
internal method :py:meth:`~unidist.core.backends.pymp.remote_function.PyMpRemoteFunction._remote`
that transmites execution of operations to Python Multiprocessing.

API
===

.. autoclass:: unidist.core.backends.pymp.remote_function.PyMpRemoteFunction
  :members:
  :private-members:
