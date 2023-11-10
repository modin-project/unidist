..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Common interfaces
"""""""""""""""""

Functions and classes used by MPI backend modules.

API
===

.. autoclass:: unidist.core.backends.mpi.core.common.Operation
  :members:

.. autoclass:: unidist.core.backends.mpi.core.common.MpiDataID
  :members:

.. autofunction:: unidist.core.backends.mpi.core.common.get_logger
.. autofunction:: unidist.core.backends.mpi.core.common.unwrapped_data_ids_list
.. autofunction:: unidist.core.backends.mpi.core.common.materialize_data_ids
.. autofunction:: unidist.core.backends.mpi.core.common.check_mpich_version
.. autofunction:: unidist.core.backends.mpi.core.common.is_shared_memory_supported
