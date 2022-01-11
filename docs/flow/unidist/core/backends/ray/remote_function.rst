..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

RayRemoteFunction
"""""""""""""""""

The class is specific implementation of :py:class:`~unidist.core.base.remote_function.RemoteFunction` class using Ray.

The :py:class:`~unidist.core.backends.ray.remote_function.RayRemoteFunction` implements internal method
:py:meth:`~unidist.core.backends.ray.remote_function.RayRemoteFunction._remote` that transmites
execution of operations to Ray.

API
===

.. autoclass:: unidist.core.backends.ray.remote_function.RayRemoteFunction
  :members:
  :private-members:
