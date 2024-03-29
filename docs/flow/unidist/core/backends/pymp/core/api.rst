..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Python Multiprocessing High-level API
"""""""""""""""""""""""""""""""""""""

Python Multiprocessing API module provides high-level functions for initialization of the backend,
for working with object storage and submitting tasks.

API
===

Function :py:func:`~unidist.core.backends.pymp.core.api.is_initialized` allows to check
if the execution backend has been initialized.

.. autofunction:: unidist.core.backends.pymp.core.api.is_initialized

Function :py:func:`~unidist.core.backends.pymp.core.api.init` creates instances of singleton
classes :py:class:`~unidist.core.backends.pymp.core.object_store.ObjectStore` and
:py:class:`~unidist.core.backends.pymp.core.process_manager.ProcessManager`.

.. autofunction:: unidist.core.backends.pymp.core.api.init

Functions :py:func:`~unidist.core.backends.pymp.core.api.get` and
:py:func:`~unidist.core.backends.pymp.core.api.put` are responsible for
read/write, respectively, objects from/to :py:class:`~unidist.core.backends.pymp.core.object_store.ObjectStore`.
Both of the functions block execution until read/write finishes.

.. autofunction:: unidist.core.backends.pymp.core.api.get
.. autofunction:: unidist.core.backends.pymp.core.api.put

:py:func:`~unidist.core.backends.pymp.core.api.wait` carries out blocking of execution
until a requested number of :py:class:`~unidist.core.backends.common.data_id.DataID` isn't ready.

.. autofunction:: unidist.core.backends.pymp.core.api.wait

:py:func:`~unidist.core.backends.pymp.core.api.submit` wraps an operation to :py:class:`~unidist.core.backends.pymp.core.process_manager.Task` and adds it
to task queue of one of the workers. Specific worker will be chosen by :py:class:`~unidist.core.backends.pymp.core.process_manager.ProcessManager`.

.. autofunction:: unidist.core.backends.pymp.core.api.submit
