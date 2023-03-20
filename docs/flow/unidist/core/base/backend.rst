..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Backend
"""""""

The interface defining operations that should be overridden by the concrete backend classes
and :py:class:`~unidist.core.base.backend.BackendProxy` as well.

API
===

.. autoclass:: unidist.core.base.backend.Backend
  :members:


BackendProxy
""""""""""""

A singleton class which instance is created during unidist initialiation using
:py:func:`~unidist.api.init` function. As soon as the instance is created, any operation called by a user
can be dispatched to the concrete backend correctly. After an operation is performed by the concrete backend,
the result hands over back to this class to postprocess it if necessary and return to the user.

API
===

.. autoclass:: unidist.core.base.backend.BackendProxy
  :members:
