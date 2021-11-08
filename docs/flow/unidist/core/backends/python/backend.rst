..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PythonBackend
"""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.backend.Backend` interface using
native Python functionality. The Python backend provides sequential execution in main process, that
can be used for debug purposes.

API
===

.. autoclass:: unidist.core.backends.python.backend.PythonBackend
  :members:
