..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Shared Object Storage
"""""""""""""""""""""

Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.object_store.ObjectStore` stores
shared between processes data in a dict implemented using Python `multiprocessing.Manager`_.

.. autoclass:: unidist.core.backends.pymp.core.object_store.ObjectStore
  :members:

.. _`multiprocessing.Manager`: https://docs.python.org/3/library/multiprocessing.html#multiprocessing.sharedctypes.pymp.Manager
