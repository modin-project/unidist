# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for MPI backend which can be used for unidist behavior tuning."""

from unidist.config.parameter import EnvironmentVariable, ExactStr


class IsMpiSpawnWorkers(EnvironmentVariable, type=bool):
    """Whether to enable MPI spawn or not."""

    default = True
    varname = "UNIDIST_IS_MPI_SPAWN_WORKERS"


class MpiHosts(EnvironmentVariable, type=ExactStr):
    """MPI hosts to run unidist on."""

    varname = "UNIDIST_MPI_HOSTS"


class MpiPickleThreshold(EnvironmentVariable, type=int):
    """
    Minimum buffer size for serialization with pickle 5 protocol.

    Notes
    -----
    If the shared object store is enabled, ``MpiSharedObjectStoreThreshold`` takes
    precedence on this configuration value and the threshold gets overridden.
    It is done intentionally to prevent multiple copies when putting an object
    into the local object store or into the shared object store.
    Data copy happens once when doing in-band serialization in depend on the threshold.
    In some cases output of a remote task can take up the memory of the task arguments.
    If those arguments are placed in the shared object store, this location should not be overwritten
    while output is being used, otherwise the output value may be corrupted.
    """

    default = 1024**2 // 4  # 0.25 MiB
    varname = "UNIDIST_MPI_PICKLE_THRESHOLD"

    @classmethod
    def get(cls) -> int:
        """
        Get minimum buffer size for serialization with pickle 5 protocol.

        Returns
        -------
        int
        """
        if MpiSharedObjectStore.get():
            mpi_pickle_threshold = MpiSharedObjectStoreThreshold.get()
        else:
            mpi_pickle_threshold = super().get()
        return mpi_pickle_threshold


class MpiBackoff(EnvironmentVariable, type=float):
    """
    Backoff time for preventing the "busy wait" in loops exchanging messages.

    Notes
    -----
    Use it carefully and set to a value different from the default
    in depend on a specific workload because this may slightly improve
    performance or, in contrary, deteriorate it.
    """

    default = 0.0001
    varname = "UNIDIST_MPI_BACKOFF"


class MpiLog(EnvironmentVariable, type=bool):
    """Whether to enable logging for MPI backend or not."""

    default = False
    varname = "UNIDIST_MPI_LOG"


class MpiSharedObjectStore(EnvironmentVariable, type=bool):
    """Whether to enable shared object store or not."""

    default = False
    varname = "UNIDIST_MPI_SHARED_OBJECT_STORE"


class MpiSharedObjectStoreMemory(EnvironmentVariable, type=int):
    """How many bytes of memory to start the shared object store with."""

    varname = "UNIDIST_MPI_SHARED_OBJECT_STORE_MEMORY"


class MpiSharedObjectStoreThreshold(EnvironmentVariable, type=int):
    """Minimum size of data to put into the shared object store."""

    default = 10 ** 5  # 100 KB
    varname = "UNIDIST_MPI_SHARED_OBJECT_STORE_THRESHOLD"
