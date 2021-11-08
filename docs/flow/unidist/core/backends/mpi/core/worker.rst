..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Workers related functionality API
"""""""""""""""""""""""""""""""""

Worker
======

Each worker MPI process starts infinite :py:func:`~unidist.core.backends.mpi.core.worker.event_loop`,
which accepts and processes incoming operations.

API
===

Cancel operation from :py:class:`~unidist.core.backends.mpi.core.common.Operations` class breaks the loop
and leaves all internal storages in their current state.

.. autofunction:: unidist.core.backends.mpi.core.worker.event_loop

.. autofunction:: unidist.core.backends.mpi.core.worker.process_get_request
.. autofunction:: unidist.core.backends.mpi.core.worker.process_wait_request
.. autofunction:: unidist.core.backends.mpi.core.worker.process_task_request
.. autofunction:: unidist.core.backends.mpi.core.worker.request_worker_data

Local Object Storage
====================

MPI :py:class:`~unidist.core.backends.mpi.core.worker.ObjectStore` stores the data for each process in a local dict.
:py:class:`~unidist.core.backends.mpi.core.worker.AsyncOperations` stores ``MPI_Isend`` asynchronous handlers and holds
a reference to the sending data to prolong lifetime until the operation completed.

API
===

.. autoclass:: unidist.core.backends.mpi.core.worker.ObjectStore
  :members:
.. autoclass:: unidist.core.backends.mpi.core.worker.AsyncOperations
  :members:

Request Storage
===============

:py:class:`~unidist.core.backends.mpi.core.worker.RequestStore` stores `unidist.get` and `unidist.wait` requests for the current worker,
which couldn't be satisied right now due to data dependencies. :py:class:`~unidist.core.backends.mpi.core.worker.TaskStore` stores task execution
requests that couldn't be satisied right now due to data dependencies.

API
===

.. autoclass:: unidist.core.backends.mpi.core.worker.RequestStore
  :members:
.. autoclass:: unidist.core.backends.mpi.core.worker.TaskStore
  :members:
