..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Workers related functionality API
"""""""""""""""""""""""""""""""""

Worker
======

Each MPI worker process starts infinite :py:func:`~unidist.core.backends.mpi.core.worker.loop.worker_loop`,
which accepts and processes incoming operations.

API
===

Cancel operation from :py:class:`~unidist.core.backends.mpi.core.common.Operations` class breaks the loop
and leaves all internal storages in their current state.

.. autofunction:: unidist.core.backends.mpi.core.worker.loop.worker_loop

.. autofunction:: unidist.core.backends.mpi.core.worker.request_store.RequestStore.process_get_request
  :noindex:
.. autofunction:: unidist.core.backends.mpi.core.worker.request_store.RequestStore.process_wait_request
  :noindex:
.. autofunction:: unidist.core.backends.mpi.core.worker.task_store.TaskStore.process_task_request
  :noindex:
.. autofunction:: unidist.core.backends.mpi.core.worker.task_store.TaskStore.request_worker_data
  :noindex:

Local Object Storage
====================

MPI :py:class:`~unidist.core.backends.mpi.core.worker.object_store.ObjectStore` stores the data for each process in a local dict.
:py:class:`~unidist.core.backends.mpi.core.worker.async_operations.AsyncOperations` stores ``MPI_Isend`` asynchronous handlers and holds
a reference to the sending data to prolong lifetime until the operation completed.

API
===

.. autoclass:: unidist.core.backends.mpi.core.worker.object_store.ObjectStore
  :members:
.. autoclass:: unidist.core.backends.mpi.core.worker.async_operations.AsyncOperations
  :members:

Request Storage
===============

:py:class:`~unidist.core.backends.mpi.core.worker.request_store.RequestStore` stores ``unidist.get`` and ``unidist.wait`` requests for the current worker,
which couldn't be satisied right now due to data dependencies. :py:class:`~unidist.core.backends.mpi.core.worker.task_store.TaskStore` stores task execution
requests that couldn't be satisied right now due to data dependencies.

API
===

.. autoclass:: unidist.core.backends.mpi.core.worker.request_store.RequestStore
  :members:
.. autoclass:: unidist.core.backends.mpi.core.worker.task_store.TaskStore
  :members:
