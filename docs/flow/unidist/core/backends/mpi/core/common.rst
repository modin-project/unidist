..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Common interfaces
"""""""""""""""""

Functions and classes used by MPI backend modules.

API
===

.. autoclass:: unidist.core.backends.mpi.core.common.Operation
  :members:

.. autoclass:: unidist.core.backends.mpi.core.common.MasterDataID
  :members:

.. autofunction:: unidist.core.backends.mpi.core.common.get_logger
.. autofunction:: unidist.core.backends.mpi.core.common.master_data_ids_to_base
.. autofunction:: unidist.core.backends.mpi.core.common.unwrap_data_ids
.. autofunction:: unidist.core.backends.mpi.core.common.materialize_data_ids
