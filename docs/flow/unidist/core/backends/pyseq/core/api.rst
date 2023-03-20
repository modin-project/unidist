..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Python Sequential High-level API
""""""""""""""""""""""""""""""""

Python Sequential API module provides high-level functions for initialization of the backend,
for working with object storage and submitting tasks.

API
===

Function :py:func:`~unidist.core.backends.pyseq.core.api.is_initialized` allows to check
if the execution backend has been initialized.

.. autofunction:: unidist.core.backends.pyseq.core.api.is_initialized

Function :py:func:`~unidist.core.backends.pyseq.core.api.init` creates an instance of singleton
class :py:class:`~unidist.core.backends.pyseq.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.pyseq.core.api.init

Functions :py:func:`~unidist.core.backends.pyseq.core.api.get` and
:py:func:`~unidist.core.backends.pyseq.core.api.put` are responsible for
read/write, respectively, objects from/to :py:class:`~unidist.core.backends.pyseq.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.pyseq.core.api.get
.. autofunction:: unidist.core.backends.pyseq.core.api.put

:py:func:`~unidist.core.backends.pyseq.core.api.submit` executes a task, which result will be put into
:py:class:`~unidist.core.backends.pyseq.core.object_store.ObjectStore`.

.. autofunction:: unidist.core.backends.pyseq.core.api.submit
