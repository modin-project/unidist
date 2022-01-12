..
      Copyright (C) 2021 Modin authors

      SPDX-License-Identifier: Apache-2.0

Welcome to Unidist's documentation!
===================================

The framework is intended to provide the unified API for distributed execution
by supporting various distributed task schedulers. At the moment the following schedulers
are supported under the hood:

* `Ray`_
* `Dask Distributed`_
* `Python Multiprocessing`_
* `MPI`_

Also, the framework provides a sequential :doc:`Python backend <flow/unidist/core/backends/python/backend>`,
that can be used for debug purposes.

The unidist is designed to work in a `task-based parallel model`_.

.. toctree::
   :hidden:

   getting_started
   developer/architecture
   developer/contributing

To get started with unidist refer to the getting started page.

* :doc:`Getting Started <getting_started>`

To dipe dive into unidist internals refer to the framework architecture.

* :doc:`unidist Architecture </developer/architecture>`

.. _`Ray`: https://docs.ray.io/en/master/index.html
.. _`Dask Distributed`: https://distributed.dask.org/en/latest/
.. _`Python Multiprocessing`: https://docs.python.org/3/library/multiprocessing.html
.. _`MPI`: https://www.mpi-forum.org/
.. _`task-based parallel model`: https://en.wikipedia.org/wiki/Task_parallelism
