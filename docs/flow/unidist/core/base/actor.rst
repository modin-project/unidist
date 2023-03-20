..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

ActorClass
""""""""""


As soon as a user wraps a class with :py:func:`~unidist.api.remote` decorator,
the class will be an instance of :py:class:`~unidist.core.base.actor.ActorClass` class.
The class is a factory creating base :py:class:`~unidist.core.base.actor.Actor` class
that wraps the actor class specific for the backend when calling :py:meth:`~unidist.core.base.actor.ActorClass.remote` method.

API
===

.. autoclass:: unidist.core.base.actor.ActorClass
  :members:


Actor
"""""

The class is a wrapper over an actor class specific for the backend. All calls of the methods are transmitted
to the actor class of the concrete backend. The :py:class:`~unidist.core.base.actor.Actor` implements the dunder method
:py:meth:`~unidist.core.base.actor.Actor.__getattr__` that transmits an access responsibility to the methods
of the native actor for the backend, held by the concrete actor class of the backend,
to :py:class:`~unidist.core.base.actor.ActorMethod` class by calling :py:meth:`~unidist.core.base.actor.ActorMethod.remote` on it.

API
===

.. autoclass:: unidist.core.base.actor.Actor
  :members:

ActorMethod
"""""""""""

The class is a wrapper over an actor method class specific for the backend. All calls of the methods are transimitted
to the actor class method of the concrete backend.

API
===

.. autoclass:: unidist.core.base.actor.ActorMethod
  :members:
