..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Python High-level API
"""""""""""""""""""""

Python API module provides high-level functions for initialization of the backend,
for working with object storage and submitting tasks.

API
===

Function :py:func:`~unidist.core.backends.python.core.api.init` creates an instance of singleton
class :py:class:`~unidist.core.backends.python.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.python.core.api.init

Functions :py:func:`~unidist.core.backends.python.core.api.get` and
:py:func:`~unidist.core.backends.python.core.api.put` are responsible for
read/write, respectively, objects from/to :py:class:`~unidist.core.backends.python.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.python.core.api.get
.. autofunction:: unidist.core.backends.python.core.api.put

:py:func:`~unidist.core.backends.python.core.api.submit` executes a task, which result will be put into
:py:class:`~unidist.core.backends.python.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.python.core.api.submit
