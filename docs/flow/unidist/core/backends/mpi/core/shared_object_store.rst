..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Shared Object Store
"""""""""""""""""""

MPI :py:class:`~unidist.core.backends.mpi.core.shared_object_store.SharedObjectStore` stores data in the shared object store.
In depend on :class:`~unidist.config.backends.mpi.envvars.MpiSharedObjectStoreThreshold`,
data can be stored in :py:class:`~unidist.core.backends.mpi.core.local_object_store.LocalObjectStore`.

Topology changes
----------------

If shared object store is enabled on a cluster, unidist has some changes in the number of service processes.
Monitor processes are created and assigned on each host. 
All monitoring processes can be divided into root monitor and non-root monitor.
The non-root monitor is only responsible for managing shared object store on its host, 
while the root monitor also performs the main work of the monitor.

Service buffer
--------------

Shared object store uses an additional service buffer to store the number of references to stored data by processes on the host 
and check whether the data has been written to shared memory or not.

A service buffer is an array of long integers that stores service information for each data in the shared object store.
Service information consists of 4 numbers:

* Worker ID - the first part of the DataID.
* Data number - the second part of the DataID.
* First data index - the first shared memory index where the data is located.
* References number - the number of data references, which shows how many processes are using this data.

Shared memory size
------------------

Memory for shared object store is allocated and managed by the monitor process, 
and other processes on the same host have read and write access to it.
By default, shared object store uses 95% of all available virtual memory. 
You can control the size of shared memory using configuration settings:
:class:`~unidist.config.backends.mpi.envvars.MpiSharedObjectStoreMemory` and
:class:`~unidist.config.backends.mpi.envvars.MpiSharedServiceMemory`.

Shared memory management
------------------------

All workers on the same host can write to shared memory, but the monitor process manages it and determines 
where data will be written and when it will be deleted. If a process wants to write some data to shared memory, 
it asks the monitor to reserve memory in shared object store of the desired size and then writes it to shared memory.

When the monitor receives a request to delete data from shared memory, it checks the number of references from all processes 
to this data. If the number of references is 0, the data will be deleted and the shared memory will be freed for further use.

All shared storage management (memory reservation and deallocation) is defined in
:class:`unidist.core.backends.mpi.core.monitor.shared_memory_manager.SharedMemoryManager`.


API
===

.. autoclass:: unidist.core.backends.mpi.core.shared_object_store.SharedObjectStore
  :members: