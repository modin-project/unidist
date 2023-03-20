..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MPIActor
""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using MPI.

The :py:class:`~unidist.core.backends.mpi.actor.MPIActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.mpi.actor.MPIActor.__getattr__` -- transmits an access responsibility
  to the methods of native MPI :py:class:`~unidist.core.backends.mpi.core.actor.Actor`,
  held by this class, to :py:class:`~unidist.core.backends.mpi.actor.MPIActorMethod` class.
* :py:meth:`~unidist.core.backends.mpi.actor.MPIActor._remote` -- creates native
  MPI :py:class:`~unidist.core.backends.mpi.core.actor.Actor` object to be held by this class.

API
===

.. autoclass:: unidist.core.backends.mpi.actor.MPIActor
  :members:
  :private-members:

MPIActorMethod
""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using MPI.

The :py:class:`~unidist.core.backends.mpi.actor.MPIActorMethod` implements
internal method :py:meth:`~unidist.core.backends.mpi.actor.MPIActorMethod._remote`
that is responsible for calls of native MPI :py:class:`~unidist.core.backends.mpi.core.actor.Actor`
class methods.

API
===

.. autoclass:: unidist.core.backends.mpi.actor.MPIActorMethod
  :members:
  :private-members:
