..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

RemoteFunction
""""""""""""""

The class is a wrapper over a remote function specific for the backend. As soos as a user wraps a function
with :py:func:`~unidist.api.remote` decorator, the function will be an instance of
:py:class:`~unidist.core.base.remote_function.RemoteFunction` class. Then, the user can call the remote function using
:py:meth:`~unidist.core.base.remote_function.RemoteFunction.remote` method of the class.

API
===

.. autoclass:: unidist.core.base.remote_function.RemoteFunction
  :members:
