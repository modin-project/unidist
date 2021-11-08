..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

DaskActor
"""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.Actor` class using Dask.

The :py:class:`~unidist.core.backends.dask.actor.DaskActor` implements 2 internal methods:

* :py:meth:`~unidist.core.backends.dask.actor.DaskActor.__getattr__` -- transmits an access responsibility
  to the methods of native `Dask actor`_, held by this class, to :py:class:`~unidist.core.backends.dask.actor.DaskActorMethod`
  class.
* :py:meth:`~unidist.core.backends.dask.actor.DaskActor._remote` -- creates native `Dask actor`_ object to be
  held by this class.

API
===

.. autoclass:: unidist.core.backends.dask.actor.DaskActor
  :members:
  :private-members:

DaskActorMethod
"""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.actor.ActorMethod` class using Dask.

The :py:class:`~unidist.core.backends.dask.actor.DaskActorMethod` implements internal method
:py:meth:`~unidist.core.backends.dask.actor.DaskActorMethod._remote` that is responsible for
calls of native `Dask actor`_ class methods.

API
===

.. autoclass:: unidist.core.backends.dask.actor.DaskActorMethod
  :members:
  :private-members:

.. _`Dask actor`: https://distributed.dask.org/en/latest/actors.html
