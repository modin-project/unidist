..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Serialization API
"""""""""""""""""

Class :py:class:`~unidist.core.backends.mpi.core.serialization.ComplexDataSerializer` supports data serialization of complex types
with lambdas, functions, member functions and large data arrays. In particular, the class supports out-of-band data serialization
for a set of pickle 5 protocol compatible libraries - pandas and NumPy, specifically ``pandas.DataFrame``, ``pandas.Series`` and ``np.ndarray`` types.
Use this class in case serialization of compound objects with different unknown types, poissibly large arrays.
Class :py:class:`~unidist.core.backends.mpi.core.serialization.SimpleDataSerializer` supports data serialization of simple data types.
Also, the class has an API for lambdas, functions, member functions serialization (and uses cloudpickle library for that purpose),
but with no performance optimization for large raw data.  Use this class when you know exact object type for serialization and it`s not cosists of large datasets.

API
===

.. autoclass:: unidist.core.backends.mpi.core.serialization.ComplexDataSerializer
   :private-members:
   :members:

.. autoclass:: unidist.core.backends.mpi.core.serialization.SimpleDataSerializer
   :private-members:
   :members:
