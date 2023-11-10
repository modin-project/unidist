..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist Configuration Settings
""""""""""""""""""""""""""""""

Using these configuration settings, the user can tune unidist's behavior. Below you can find
configuration settings currently provided by unidist.

Public API
''''''''''

Potentially, the source of configuration settings can be any, but for now only environment
variables are implemented. Any environment variable originates from
:class:`~unidist.config.parameter.EnvironmentVariable`, which contains most of
the config API implementation.

.. autoclass:: unidist.config.parameter.EnvironmentVariable
  :members: put, get

Unidist Configuration Settings List
'''''''''''''''''''''''''''''''''''

+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| **Config Name**               | **Env. Variable Name**                    | **Description**                                                          |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| Backend                       | UNIDIST_BACKEND                           | Distribution backend to run queries by                                   |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| CpuCount                      | UNIDIST_CPUS                              | How many CPU cores to use during initialization of the unidist backend   |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| RayGpuCount                   | UNIDIST_RAY_GPUS                          | How many GPU devices to use during initialization of the Ray backend     |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| IsRayCluster                  | UNIDIST_RAY_CLUSTER                       | Whether Ray is running on pre-initialized Ray cluster                    |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| RayRedisAddress               | UNIDIST_RAY_REDIS_ADDRESS                 | Redis address to connect to when running in Ray cluster                  |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| RayRedisPassword              | UNIDIST_RAY_REDIS_PASSWORD                | What password to use for connecting to Redis                             |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| RayObjectStoreMemory          | UNIDIST_RAY_OBJECT_STORE_MEMORY           | How many bytes of memory to start the Ray object store with              |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| DaskMemoryLimit               | UNIDIST_DASK_MEMORY_LIMIT                 | How many bytes of memory that Dask worker should use                     |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| IsDaskCluster                 | UNIDIST_DASK_CLUSTER                      | Whether Dask is running on pre-initialized Dask cluster                  |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| DaskSchedulerAddress          | UNIDIST_DASK_SCHEDULER_ADDRESS            | Dask Scheduler address to connect to when running in Dask cluster        |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiSpawn                      | UNIDIST_MPI_SPAWN                         | Whether to enable MPI spawn or not                                       |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiHosts                      | UNIDIST_MPI_HOSTS                         | MPI hosts to run unidist on                                              |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiPickleThreshold            | UNIDIST_MPI_PICKLE_THRESHOLD              | Minimum buffer size for serialization with pickle 5 protocol             |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiBackoff                    | UNIDIST_MPI_BACKOFF                       | Backoff time for preventing the "busy wait" in loops exchanging messages |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiLog                        | UNIDIST_MPI_LOG                           | Whether to enable logging for MPI backend or not                         |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiSharedObjectStore          | UNIDIST_MPI_SHARED_OBJECT_STORE           | Whether to enable shared object store or not                             |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiSharedObjectStoreMemory    | UNIDIST_MPI_SHARED_OBJECT_STORE_MEMORY    | How many bytes of memory to start the shared object store with           |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiSharedServiceMemory        | UNIDIST_MPI_SHARED_SERVICE_MEMORY         | How many bytes of memory to start the shared service memory with         |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiSharedObjectStoreThreshold | UNIDIST_MPI_SHARED_OBJECT_STORE_THRESHOLD | Minimum size of data to put into the shared object store                 |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+
| MpiRuntimeEnv                 | Only the config API is available          | Runtime environment for MPI worker processes                             |
+-------------------------------+-------------------------------------------+--------------------------------------------------------------------------+

Usage Guide
'''''''''''

As it can be seen below a config value can be set either by setting the environment variable or
by using config API.

.. code-block:: python

    import os

    # Setting `UNIDIST_BACKEND` environment variable.
    # Also can be set outside the script.
    os.environ["UNIDIST_BACKEND"] = "mpi"

    import unidist.config as cfg

    # Checking initially set `Backend` config,
    # which corresponds to `UNIDIST_BACKEND` environment variable
    print(cfg.Backend.get()) # prints 'mpi'

    # Checking default value of `CpuCount`
    print(cfg.CpuCount.get()) # prints the number of CPUs on your machine

    # Changing value of `CpuCount`
    cfg.CpuCount.put(16)
    print(cfg.CpuCount.get()) # prints '16'

.. note::
   Make sure that setting configuration values happens before unidist initialization
   (:py:func:`~unidist.api.init` call)! Otherwise, unidist will opt for the default settings.
