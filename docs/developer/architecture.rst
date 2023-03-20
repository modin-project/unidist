..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

Unidist Architecture
""""""""""""""""""""

High-Level Execution View
=========================

The diagram below outlines the high-level view to the execution flow of unidist.

.. image:: /img/execution_flow.svg
   :align: center

.. toctree::
   :hidden:

   /flow/unidist/api

When calling an operation of the :doc:`API </flow/unidist/api>` provided by the framework unidist appeals to
the :py:class:`~unidist.core.base.backend.BackendProxy` object that dispatches the call to
the concrete backend class instance (:py:class:`~unidist.core.backends.ray.backend.RayBackend`,
:py:class:`~unidist.core.backends.dask.backend.DaskBackend`,
:py:class:`~unidist.core.backends.multiprocessing.backend.MultiProcessingBackend`,
:py:class:`~unidist.core.backends.python.backend.PythonBackend` or :py:class:`~unidist.core.backends.mpi.backend.MPIBackend`).
These classes are childs of the :py:class:`~unidist.core.base.backend.Backend` interface and should override
operations declared in it. Then, the concrete backend performs passed operation and hands over the result back to
the :py:class:`~unidist.core.base.backend.BackendProxy` that postprocesses it if necessary and returns it to the user.

Class View
==========

unidist performs operations using the following key base classes:

* :py:class:`~unidist.core.base.backend.BackendProxy`
* :py:class:`~unidist.core.base.remote_function.RemoteFunction`
* :py:class:`~unidist.core.base.actor.ActorClass`
* :py:class:`~unidist.core.base.actor.Actor`
* :py:class:`~unidist.core.base.actor.ActorMethod`
* :py:class:`~unidist.core.base.object_ref.ObjectRef`

Module View
===========

unidist modules layout is shown below. To deep dive into unidist internal implementation
details just pick module you are interested in.

.. parsed-literal::
   └───unidist
       ├─── :doc:`api </flow/unidist/api>`
       ├─── :doc:`config </flow/unidist/config>`
       └───core
           ├───backends
           |   ├───common
           |   │   └─── :doc:`data_id </flow/unidist/core/backends/common/data_id>`
           |   ├───dask
           |   │   ├─── :doc:`actor </flow/unidist/core/backends/dask/actor>`
           |   │   ├─── :doc:`backend </flow/unidist/core/backends/dask/backend>`
           |   │   └─── :doc:`remote_function </flow/unidist/core/backends/dask/remote_function>`
           |   ├───mpi
           |   |   ├───core
           |   │   │    ├─── :doc:`common </flow/unidist/core/backends/mpi/core/common>`
           |   │   │    ├─── :doc:`communication </flow/unidist/core/backends/mpi/core/communication>`
           |   │   │    ├─── :doc:`controller </flow/unidist/core/backends/mpi/core/controller>`
           |   │   │    ├─── :doc:`monitor </flow/unidist/core/backends/mpi/core/monitor>`
           |   │   │    ├─── :doc:`serialization </flow/unidist/core/backends/mpi/core/serialization>`
           |   │   │    └─── :doc:`worker </flow/unidist/core/backends/mpi/core/worker>`
           |   │   ├─── :doc:`actor </flow/unidist/core/backends/mpi/actor>`
           |   │   ├─── :doc:`backend </flow/unidist/core/backends/mpi/backend>`
           |   │   └─── :doc:`remote_function </flow/unidist/core/backends/mpi/remote_function>`
           |   ├───multiprocessing
           │   |   ├───core
           │   │   │    ├─── :doc:`actor </flow/unidist/core/backends/multiprocessing/core/actor>`
           │   │   │    ├─── :doc:`api </flow/unidist/core/backends/multiprocessing/core/api>`
           │   │   │    ├─── :doc:`object_store </flow/unidist/core/backends/multiprocessing/core/object_store>`
           │   │   │    └─── :doc:`process_manager </flow/unidist/core/backends/multiprocessing/core/process_manager>`
           │   │   ├─── :doc:`actor </flow/unidist/core/backends/multiprocessing/actor>`
           │   │   ├─── :doc:`backend </flow/unidist/core/backends/multiprocessing/backend>`
           │   │   └─── :doc:`remote_function </flow/unidist/core/backends/multiprocessing/remote_function>`
           │   ├───python
           │   |   ├───core
           │   │   │    ├─── :doc:`api </flow/unidist/core/backends/python/core/api>`
           │   │   │    └─── :doc:`object_store </flow/unidist/core/backends/python/core/object_store>`
           │   │   ├─── :doc:`actor </flow/unidist/core/backends/python/actor>`
           │   │   ├─── :doc:`backend </flow/unidist/core/backends/python/backend>`
           │   │   └─── :doc:`remote_function </flow/unidist/core/backends/python/remote_function>`
           │   └───ray
           │       ├─── :doc:`actor </flow/unidist/core/backends/ray/actor>`
           │       ├─── :doc:`backend </flow/unidist/core/backends/ray/backend>`
           │       └─── :doc:`remote_function </flow/unidist/core/backends/ray/remote_function>`
           └───base
               ├─── :doc:`actor </flow/unidist/core/base/actor>`
               ├─── :doc:`backend </flow/unidist/core/base/backend>`
               ├─── :doc:`common </flow/unidist/core/base/common>`
               ├─── :doc:`object_ref </flow/unidist/core/base/object_ref>`
               └─── :doc:`remote_function </flow/unidist/core/base/remote_function>`
