..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PyMpActor
"""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using Python Multiprocessing backend.

The :py:class:`~unidist.core.backends.pymp.actor.PyMpActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.pymp.actor.PyMpActor.__getattr__` -- transmits an access responsibility
  to the methods of native Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor`,
  held by this class, to :py:class:`~unidist.core.backends.pymp.actor.PyMpActorMethod` class.
* :py:meth:`~unidist.core.backends.pymp.actor.PyMpActor._remote` -- creates native
  Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor` object to be held by this class.

API
===

.. autoclass:: unidist.core.backends.pymp.actor.PyMpActor
  :members:
  :private-members:

PyMpActorMethod
"""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using Python Multiprocessing backend.

The :py:class:`~unidist.core.backends.pymp.actor.PyMpActorMethod` implements
internal method :py:meth:`~unidist.core.backends.pymp.actor.PyMpActorMethod._remote`
that is responsible for calls of native Python Multiprocessing :py:class:`~unidist.core.backends.pymp.core.actor.Actor`
class methods.

API
===

.. autoclass:: unidist.core.backends.pymp.actor.PyMpActorMethod
  :members:
  :private-members:
