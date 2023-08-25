..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Local Object Store
============

MPI :py:class:`~unidist.core.backends.mpi.core.local_object_store.LocalObjectStore` stores the data either in the shared memory or in each single process in a local dict in depend on data size.

API
===

.. autoclass:: unidist.core.backends.mpi.core.local_object_store.LocalObjectStore
  :members:
