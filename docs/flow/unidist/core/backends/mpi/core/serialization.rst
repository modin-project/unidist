..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Serialization API
"""""""""""""""""

Class :py:class:`~unidist.core.backends.mpi.core.serialization.ComplexDataSerializer` supports data serialization of complex types
with lambdas, functions, member functions and large data arrays. In particular, the class supports out-of-band data serialization
for a set of pickle 5 protocol compatible libraries - pandas and NumPy, specifically ``pandas.DataFrame``, ``pandas.Series`` and ``np.ndarray`` types.
This class is used in case of serialization of compound objects with different unknown types, possibly large arrays.
Class :py:class:`~unidist.core.backends.mpi.core.serialization.SimpleDataSerializer` supports data serialization of simple data types.
Also, the class has an API for serialization of lambdas, functions, member functions (and uses ``cloudpickle`` library for that purpose),
but with no performance optimization for large raw data. This class is used when exact object type is known for serialization and it doesn't consist of large datasets.

API
===

.. autoclass:: unidist.core.backends.mpi.core.serialization.ComplexDataSerializer
   :private-members:
   :members:

.. autoclass:: unidist.core.backends.mpi.core.serialization.SimpleDataSerializer
   :private-members:
   :members:
