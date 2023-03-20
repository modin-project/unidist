..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Monitor related functionality API
"""""""""""""""""""""""""""""""""

Infinite :py:func:`~unidist.core.backends.mpi.core.monitor.monitor_loop` function tracks
MPI backend statistics: executed tasks counter.

API
===

Cancel operation from :py:class:`~unidist.core.backends.mpi.core.common.Operations` class breaks the loop.

.. autofunction:: unidist.core.backends.mpi.core.monitor.monitor_loop
