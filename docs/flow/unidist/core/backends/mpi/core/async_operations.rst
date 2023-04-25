..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Async Operations Storage
===============

:py:class:`~unidist.core.backends.mpi.core.async_operations.AsyncOperations` stores ``MPI_Isend`` asynchronous handlers and holds
a reference to the sending data to prolong lifetime until the operation completed.

.. autoclass:: unidist.core.backends.mpi.core.async_operations.AsyncOperations
  :members: