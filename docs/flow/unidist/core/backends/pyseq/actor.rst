..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

PySeqActor
""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using
native Python Sequential functionality.

The :py:class:`~unidist.core.backends.pyseq.actor.PySeqActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.pyseq.actor.PySeqActor.__getattr__` -- transmits an access responsibility
  to the methods of class object, held by this class,
  to :py:class:`~unidist.core.backends.pyseq.actor.PySeqActorMethod` class.
* :py:meth:`~unidist.core.backends.pyseq.actor.PySeqActor._remote` -- creates an object of
  the required class to be held by this class.

API
===

.. autoclass:: unidist.core.backends.pyseq.actor.PySeqActor
  :members:
  :private-members:

PySeqActorMethod
""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using
native Python Sequential functionality.

The :py:class:`~unidist.core.backends.pyseq.actor.PySeqActorMethod` implements
internal method :py:meth:`~unidist.core.backends.pyseq.actor.PySeqActorMethod._remote`
that is responsible for method calls of the wrapped class object using :py:func:`~unidist.core.backends.pyseq.core.api.submit`.

API
===

.. autoclass:: unidist.core.backends.pyseq.actor.PySeqActorMethod
  :members:
  :private-members:
