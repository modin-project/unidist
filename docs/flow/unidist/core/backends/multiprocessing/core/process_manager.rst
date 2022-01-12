..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Workers related functionality API
"""""""""""""""""""""""""""""""""

Worker
======

MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Worker`
represents a Python process that has a task queue. Tasks from the queue are run sequentially.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.core.process_manager.Worker
  :members:

Task
====

MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Task`
is an object-wrapper for a free functions and actor methods.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.core.process_manager.Task
  :members:

ProcessManager
==============

MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.ProcessManager`
schedules operations (:py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Task` objects)
to free :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Worker`-s by round-robin algorithm.
Free :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Worker` is a worker, that isn't used
by MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor`.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.core.process_manager.ProcessManager
  :members:
