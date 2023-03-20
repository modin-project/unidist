..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

ObjectRef
"""""""""

The class is user-facing object reference that is returned from
:py:class:`~unidist.core.base.remote_function.RemoteFunction` or :py:class:`~unidist.core.base.actor.ActorMethod`.
The class is a wrapper over an original future object of the concrete backend (``ray.ObjectRef``, ``dask.distributed.Future``, etc.).

API
===

.. autoclass:: unidist.core.base.object_ref.ObjectRef
  :members:
