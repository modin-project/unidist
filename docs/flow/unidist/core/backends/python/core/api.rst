..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Python High-level API
"""""""""""""""""""""

Python API module provides high-level functions for wrapping/unwrapping objects and
submitting tasks.

API
===

Functions :py:func:`~unidist.core.backends.python.core.api.init` creates an instance of singleton
class :py:class:`~unidist.core.backends.python.core.object_store.ObjectStore`.

Functions :py:func:`~unidist.core.backends.python.core.api.unwrap` and
:`~unidist.core.backends.python.core.api.wrap` are responsible for
unwrap/wrap, respectively, objects from/in :py:class:`~unidist.backends.common.data_id.DataID`.

.. autofunction:: unidist.core.backends.python.core.api.unwrap
.. autofunction:: unidist.core.backends.python.core.api.wrap

:py:func:`~unidist.core.backends.python.core.api.submit` executes a task, which result will be wrapped
in :py:class:`~unidist.backends.common.data_id.DataID`-(s).

.. autofunction:: unidist.core.backends.python.core.api.submit
