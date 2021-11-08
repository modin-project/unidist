..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MultiProcessingActor
""""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using Multiprocessing.

The :py:class:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActor.__getattr__` -- transmits an access responsibility
  to the methods of native Multiprocessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor`,
  held by this class, to :py:class:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActorMethod` class.
* :py:meth:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActor._remote` -- creates native
  Multiprocessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor` object to be held by this class.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.actor.MultiProcessingActor
  :members:
  :private-members:

MultiProcessingActorMethod
""""""""""""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using Multiprocessing.

The :py:class:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActorMethod` implements
internal method :py:meth:`~unidist.core.backends.multiprocessing.actor.MultiProcessingActorMethod._remote`
that is responsible for calls of native Multiprocessing :py:class:`~unidist.core.backends.multiprocessing.core.actor.Actor`
class methods.

API
===

.. autoclass:: unidist.core.backends.multiprocessing.actor.MultiProcessingActorMethod
  :members:
  :private-members:
