..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Serialization API
"""""""""""""""""

Class :py:class:`~unidist.core.backends.mpi.core.serialization.MPISerializer` supports data serialization of complex types
with lambdas, functions, member functions. Moreover, the class support out-of-band data serialization
for a set of pickle 5 protocol compatible libraries - *pandas* and *numpy*, specifically *pandas.DataFrame* and *np.ndarray* types.

API
===

.. autoclass:: unidist.core.backends.mpi.core.serialization.MPISerializer
   :private-members:
   :members:
