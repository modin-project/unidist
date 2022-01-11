..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PythonRemoteFunction
""""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using
native Python functionality.

The :py:class:`~unidist.core.backends.python.remote_function.PythonRemoteFunction` implements
internal method :py:meth:`~unidist.core.backends.python.remote_function.PythonRemoteFunction._remote`
that transmites execution of operations to core part of Python backend.

API
===

.. autoclass:: unidist.core.backends.python.remote_function.PythonRemoteFunction
  :members:
  :private-members:
