..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

Getting Started
"""""""""""""""

unidist provides :doc:`the high-level API</flow/unidist/api>` to make distributed applications. To tune
unidist's behavior the user has several methods described in :doc:`unidist configuration settings</flow/unidist/config>`
section.

Using unidist API
=================

The example below shows how to use unidist API to make parallel execution for
functions (tasks) and classes (actors).

.. code-block:: python

   # script.py
   if __name__ == "__main__":
      import unidist.config as cfg
      import unidist

      # Initialize unidist's backend. The Ray backend is used by default.
      unidist.init()

      # Apply decorator to make `square` a remote function.
      @unidist.remote
      def square(x):
         return x * x

      # Asynchronously execute remote function.
      square_refs = [square.remote(i) for i in range(4)]

      # Apply decorator to make `Counter` actor class.
      @unidist.remote
      class Cube:
         def __init__(self):
               self.volume = None

         def compute_volume(self, square):
               self.volume = square ** 1.5

         def read(self):
               return self.volume

      # Create instances of the actor class.
      cubes = [Cube.remote() for _ in range(len(square_refs))]
      # Asynchronously execute methods of the actor class.
      [cube.compute_volume.remote(square) for cube, square in zip(cubes, square_refs)]
      cube_refs = [cube.read.remote() for cube in cubes]

      # Get materialized results.
      print(unidist.get(square_refs)) # [0, 1, 4, 9]
      print(unidist.get(cube_refs)) # [0.0, 1.0, 8.0, 27.0]

Choosing unidist's backend
==========================

The recommended way to choose a concrete execution backend is to use the argument of :doc:`unidist CLI </using_cli>`
when running your python script:

.. code-block:: bash

    # Running the script with unidist on Ray backend
    $ unidist script.py --backend ray
    # Running the script with unidist on MPI backend
    $ unidist script.py --backend mpi
    # Running the script with unidist on Dask backend
    $ unidist script.py --backend dask
    # Running the script with unidist on Python Multiprocessing backend
    $ unidist script.py --backend multiprocessing
    # Running the script with unidist on sequential Python backend
    $ unidist script.py --backend python

For more options on how to choose a concrete execution backend
see :doc:`Using Unidist </using_unidist/index>` section.

Running unidist application
===========================

To run the script mentioned above the unidist CLI should be used:

.. code-block:: bash

    # Running the script in a single node with `Ray` backend on `4` workers:
    $ unidist script.py -num_cpus 4

To find more options for running refer to :doc:`unidist CLI </using_cli>` documentation page.
