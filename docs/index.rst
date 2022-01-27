..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

What is unidist?
""""""""""""""""

unidist (`Unified Distributed Execution`) provides the unified API for distributed execution by supporting various performant execution backends.
At the moment the following backends are supported under the hood:

* `Ray`_
* `MPI`_
* `Dask Distributed`_
* `Python Multiprocessing`_

Also, the framework provides a sequential :doc:`Python backend <flow/unidist/core/backends/python/backend>`,
that can be used for debugging.

unidist is designed to work in a `task-based parallel model`_. The framework mimics `Ray`_ API and expands the existing frameworks
(`Ray`_ and `Dask Distributed`_) with additional features.

Installation
============

unidist can be installed from sources using ``pip``:

.. code-block:: bash

   # Dependencies for all the execution backends will be installed
   $ pip install git+https://github.com/modin-project/unidist#egg=unidist[all]

For more information about installation and supported OS platforms see the installation page.

Usage
=====

The example below describes squaring the numbers from a list using unidist:

.. code-block:: python

   # script.py
   if __name__ == "__main__":
      import unidist

      unidist.init() # Initialize unidist's backend.

      @unidist.remote # Apply a decorator to make `foo` a remote function.
      def foo(x):
         return x * x

      # This will run `foo` on a pool of workers in parallel;
      # `refs` will contain object references to actual data.
      refs = [foo.remote(i) for i in range(4)]

      # Get materialized result.
      print(unidist.get(refs)) # [0, 1, 4, 9]

To run the `script.py` use :doc:`unidist CLI </using_cli>`:

.. code-block:: bash

    # Running the script in a single node with `mpi` backend on `4` workers:
    $ unidist script.py --backend mpi --num_cpus 4


.. toctree::
   :hidden:

   getting_started
   using_cli
   developer/architecture
   developer/contributing

To get started with unidist refer to the :doc:`getting started <getting_started>` page.

To deep dive into unidist internals refer to :doc:`the framework architecture </developer/architecture>`.

.. _`Ray`: https://docs.ray.io/en/master/index.html
.. _`Dask Distributed`: https://distributed.dask.org/en/latest/
.. _`Python Multiprocessing`: https://docs.python.org/3/library/multiprocessing.html
.. _`MPI`: https://www.mpi-forum.org/
.. _`task-based parallel model`: https://en.wikipedia.org/wiki/Task_parallelism
