..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Actor
"""""

Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor` class is
intended to transform a user-defined class to the class shared between processes (using `multiprocessing.managers.BaseManager`_),
which methods will be executed in a separate Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.process_manager.Worker`.

API
===

.. autoclass:: unidist.core.backends.pymp.core.actor.Actor
  :members:

ActorMethod
"""""""""""

The :py:class:`~unidist.core.backends.pymp.core.actor.ActorMethod` class is a wrapper over the method of
the shared between processes class, stored in Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor`.
Method :py:meth:`~unidist.core.backends.pymp.core.actor.ActorMethod.submit` wraps a method of the shared between processes
class object to :py:class:`~unidist.core.backends.pymp.core.process_manager.Task` and adds it
to task queue of Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.process_manager.Worker`, used by
Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor`.

API
===

.. autoclass:: unidist.core.backends.pymp.core.actor.ActorMethod
  :members:

.. _`multiprocessing.managers.BaseManager`: https://docs.python.org/3/library/multiprocessing.html#multiprocessing.managers.BaseManager
