..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Serialization API
"""""""""""""""""

Class :py:class:`~unidist.core.backends.mpi.core.serialization.ComplexSerializer` supports data serialization of complex types
with lambdas, functions, member functions. Moreover, the class support out-of-band data serialization
for a set of pickle 5 protocol compatible libraries - pandas and NumPy, specifically ``pandas.DataFrame``, ``pandas.Series`` and ``np.ndarray`` types.
Class :py:class:`~unidist.core.backends.mpi.core.serialization.SimpleSerializer` supports data serialization of simple data types, including
lambdas, functions, member functions, but with no performance optimization for raw data.

API
===

.. autoclass:: unidist.core.backends.mpi.core.serialization.ComplexSerializer
   :private-members:
   :members:

.. autoclass:: unidist.core.backends.mpi.core.serialization.SimpleSerializer
   :private-members:
   :members:
