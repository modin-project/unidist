..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

RayActor
""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using Ray.

The :py:class:`~unidist.core.backends.ray.actor.RayActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.ray.actor.RayActor.__getattr__` -- transmits an access responsibility
  to the methods of native `Ray actor`_, held by this class, to :py:class:`~unidist.core.backends.ray.actor.RayActorMethod` class.
* :py:meth:`~unidist.core.backends.ray.actor.RayActor._remote` -- creates native `Ray actor`_ object
  to be held by this class.

API
===

.. autoclass:: unidist.core.backends.ray.actor.RayActor
  :members:
  :private-members:

RayActorMethod
""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using Ray.

The :py:class:`~unidist.core.backends.ray.actor.RayActorMethod` implements internal method
:py:meth:`~unidist.core.backends.ray.actor.RayActorMethod._remote` that is responsible for
calls of native `Ray actor`_ class methods.

API
===

.. autoclass:: unidist.core.backends.ray.actor.RayActorMethod
  :members:
  :private-members:

.. _`Ray actor`: https://docs.ray.io/en/master/actors.html
