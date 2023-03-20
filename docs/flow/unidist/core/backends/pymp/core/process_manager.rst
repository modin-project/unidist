..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Workers related functionality API
"""""""""""""""""""""""""""""""""

Worker
======

Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.process_manager.Worker`
represents a Python process that has a task queue. Tasks from the queue are run sequentially.

API
===

.. autoclass:: unidist.core.backends.pymp.core.process_manager.Worker
  :members:

Task
====

Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.process_manager.Task`
is an object-wrapper for a free functions and actor methods.

API
===

.. autoclass:: unidist.core.backends.pymp.core.process_manager.Task
  :members:

ProcessManager
==============

Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.process_manager.ProcessManager`
schedules operations (:py:class:`~unidist.core.backends.pymp.core.process_manager.Task` objects)
to free :py:class:`~unidist.core.backends.pymp.core.process_manager.Worker`-s by round-robin algorithm.
Free :py:class:`~unidist.core.backends.pymp.core.process_manager.Worker` is a worker, that isn't used
by Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor`.

API
===

.. autoclass:: unidist.core.backends.pymp.core.process_manager.ProcessManager
  :members:
