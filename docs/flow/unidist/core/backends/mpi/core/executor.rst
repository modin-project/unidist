..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MPI High-level API
""""""""""""""""""

MPI executor API module provides high-level functions for initialization of the backend,
for working with object storage and submitting tasks.

API
===

.. autofunction:: unidist.core.backends.mpi.core.executor.init

Function :py:func:`~unidist.core.backends.mpi.core.executor.shutdown` sends cancelation signal to all MPI processes.
After that, MPI backend couldn't be restarted.

.. autofunction:: unidist.core.backends.mpi.core.executor.shutdown

Functions :py:func:`~unidist.core.backends.mpi.core.executor.get` and
:py:func:`~unidist.core.backends.mpi.core.executor.put` are responsible for read/write operations from/to object storage.
Both of the functions block execution until read/write finishes.

.. autofunction:: unidist.core.backends.mpi.core.executor.get
.. autofunction:: unidist.core.backends.mpi.core.executor.put

:py:func:`~unidist.core.backends.mpi.core.executor.wait` carries out blocking of execution
until a requested number of :py:class:`~unidist.core.backends.mpi.core.common.MasterDataID` isn't ready.

.. autofunction:: unidist.core.backends.mpi.core.executor.wait

:py:func:`~unidist.core.backends.mpi.core.executor.remote` submits a task execution request to a worker.
Specific worker will be chosen by :py:func:`~unidist.core.backends.mpi.core.executor.schedule_rank` scheduling function.

.. autofunction:: unidist.core.backends.mpi.core.executor.remote

Scheduler
=========

Currently, scheduling happens in a simple round-robing fashion. :py:func:`~unidist.core.backends.mpi.core.executor.schedule_rank` function
just returns the next rank number in a loop.

.. autofunction:: unidist.core.backends.mpi.core.executor.schedule_rank

Local Object Storage
====================

MPI :py:class:`~unidist.core.backends.mpi.core.executor.ObjectStore` stores the data for master process in a local dict.
:py:class:`~unidist.core.backends.mpi.core.executor.GarbageCollector` controls memory footprint and sends cleanup requests for all workers,
if certain amount of data IDs is out-of-scope.

.. autoclass:: unidist.core.backends.mpi.core.executor.ObjectStore
  :members:
.. autoclass:: unidist.core.backends.mpi.core.executor.GarbageCollector
  :members:
