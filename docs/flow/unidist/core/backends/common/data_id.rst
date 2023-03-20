..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unique Data Identifier
""""""""""""""""""""""

The backends that do not have own implementation of future object like ``ray.ObjectRef`` use :py:class:`~unidist.core.backends.common.data_id.DataID` objects
to associate with data objects in backend-specific object storages.

.. autoclass:: unidist.core.backends.common.data_id.DataID
  :members:
