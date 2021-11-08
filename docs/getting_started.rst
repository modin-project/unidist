..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

Getting Started
"""""""""""""""

Using Unidist
=============

.. code-block:: python

   if __name__ == "__main__":
      import unidist

      # Initialize unidist's backend. The Ray backend is used by default.
      unidist.init()

      # Apply decorator to make `f` remote function.
      @unidist.remote
      def f(x):
         return x * x

      # Asynchronously execute remote function.
      refs = [f.remote(i) for i in range(4)]

      # Get materialized data.
      print(unidist.get(refs)) # [0, 1, 4, 9]

      # Apply decorator to make `Counter` actor class.
      @unidist.remote
      class Counter:
         def __init__(self):
               self.n = 0

         def increment(self):
               self.n += 1

         def read(self):
               return self.n

      # Create instances of the actor class.
      counters = [Counter.remote() for i in range(4)]
      # Asynchronously execute methods of the actor class.
      [c.increment.remote() for c in counters]
      refs = [c.read.remote() for c in counters]

      # Get materialized data.
      print(unidist.get(refs)) # [1, 1, 1, 1]

Choosing unidist's backend
===========================

If you want to choose a specific unidist's backend to run on, you can set the environment variable
``UNIDIST_BACKEND``. unidist will perform execution with that backend:

.. code-block:: bash

   export UNIDIST_BACKEND=Ray  # unidist will use Ray
   export UNIDIST_BACKEND=Dask  # unidist will use Dask
   export UNIDIST_BACKEND=MPI  # unidist will use MPI
   export UNIDIST_BACKEND=MultiProcessing  # unidist will use MultiProcessing
   export UNIDIST_BACKEND=Python  # unidist will use Python
