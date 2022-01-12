..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Actor
"""""

MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor` class is
intended to transform a user-defined class to the class shared between processes (using `multiprocessing.managers.BaseManager`_),
which methods will be executed in a separate MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Worker`.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.core.actor.Actor
  :members:

ActorMethod
"""""""""""

The :py:class:`~unidist.core.backends.multiprocessing.core.actor.ActorMethod` class is a wrapper over the method of
the shared between processes class, stored in MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor`.
Method :py:meth:`~unidist.core.backends.multiprocessing.core.actor.ActorMethod.submit` wraps a method of the shared between processes
class object to :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Task` and adds it
to task queue of MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.process_manager.Worker`, used by
MultiProcessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor`.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.core.actor.ActorMethod
  :members:

.. _`multiprocessing.managers.BaseManager`: https://docs.python.org/3/library/multiprocessing.html#multiprocessing.managers.BaseManager
