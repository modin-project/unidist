..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PythonActor
"""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using
native Python functionality.

The :py:class:`~unidist.core.backends.python.actor.PythonActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.python.actor.PythonActor.__getattr__` -- transmits an access responsibility
  to the methods of class object, held by this class,
  to :py:class:`~unidist.core.backends.python.actor.PythonActorMethod` class.
* :py:meth:`~unidist.core.backends.python.actor.PythonActor._remote` -- creates an object of
  the required class to be held by this class.


API
===

.. autoclass:: unidist.core.backends.python.actor.PythonActor
  :members:
  :private-members:

PythonActorMethod
"""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using
native Python functionality.

The :py:class:`~unidist.core.backends.python.actor.PythonActorMethod` implements
internal method :py:meth:`~unidist.core.backends.python.actor.PythonActorMethod._remote`
that is responsible for method calls of the wrapped class object using :py:func:`~unidist.core.backends.python.core.api.submit`.

API
===

.. autoclass:: unidist.core.backends.python.actor.PythonActorMethod
  :members:
  :private-members:
