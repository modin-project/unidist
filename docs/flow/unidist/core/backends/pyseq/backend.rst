..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PySeqBackend
""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.backend.Backend` interface using
native Python Sequential functionality. The Python Sequential backend provides sequential execution in main process, that
can be used for debug purposes.

API
===

.. autoclass:: unidist.core.backends.pyseq.backend.PySeqBackend
  :members:
