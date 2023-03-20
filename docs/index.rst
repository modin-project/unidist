..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

.. image:: img/unidist-logo-simple-628x128.png
   :alt: unidist logo
   :align: center

What is unidist?
''''''''''''''''

unidist (`Unified Distributed Execution`) is a framework that is intended to provide the unified API for distributed
execution by supporting various performant execution backends. At the moment the following backends are supported under the hood:

* `Ray`_
* `MPI`_
* `Dask Distributed`_
* `Python Multiprocessing`_

Also, the framework provides a sequential :doc:`Python backend <flow/unidist/core/backends/python/backend>`,
that can be used for debugging.

unidist is designed to work in a `task-based parallel model`_. The framework mimics `Ray`_ API and expands the existing frameworks
(`Ray`_ and `Dask Distributed`_) with additional features.

Quick Start Guide
'''''''''''''''''

Installation
""""""""""""

To install the most recent stable release for unidist run the following:

.. code-block:: bash

  pip install unidist[all] # Install unidist with dependencies for all the backends

For further instructions on how to install unidist with concrete execution backends or
using ``conda`` see our :doc:`Installation <installation>` section.

Usage
"""""

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

      # Get materialized data.
      print(unidist.get(refs)) # [0, 1, 4, 9]

Run the `script.py` with:

.. code-block:: bash

    $ python script.py

.. toctree::
   :hidden:

   installation
   getting_started
   using_unidist/index
   developer/architecture
   developer/contributing

To get started with unidist refer to the :doc:`getting started <getting_started>` page.

To deep dive into unidist internals refer to :doc:`the framework architecture </developer/architecture>`.

.. _`Ray`: https://docs.ray.io/en/master/index.html
.. _`Dask Distributed`: https://distributed.dask.org/en/latest/
.. _`Python Multiprocessing`: https://docs.python.org/3/library/multiprocessing.html
.. _`MPI`: https://www.mpi-forum.org/
.. _`task-based parallel model`: https://en.wikipedia.org/wiki/Task_parallelism
