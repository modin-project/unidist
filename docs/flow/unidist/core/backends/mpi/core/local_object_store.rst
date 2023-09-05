..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Local Object Store
==================

MPI :py:class:`~unidist.core.backends.mpi.core.local_object_store.LocalObjectStore` stores data in-process memory in a local dict.
In depend on :class:`~unidist.config.backends.mpi.envvars.MpiSharedObjectStoreThreshold``,
data can be stored in :py:class:`~unidist.core.backends.mpi.core.shared_object_store.SharedObjectStore`.

API
===

.. autoclass:: unidist.core.backends.mpi.core.local_object_store.LocalObjectStore
  :members:
