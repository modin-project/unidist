..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PySeqRemoteFunction
"""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using
native Python Sequential functionality.

The :py:class:`~unidist.core.backends.pyseq.remote_function.PySeqRemoteFunction` implements
internal method :py:meth:`~unidist.core.backends.pyseq.remote_function.PySeqRemoteFunction._remote`
that transmites execution of operations to core part of Python Sequential backend.

API
===

.. autoclass:: unidist.core.backends.pyseq.remote_function.PySeqRemoteFunction
  :members:
  :private-members:
