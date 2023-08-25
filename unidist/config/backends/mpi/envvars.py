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
    """Minimum buffer size for serialization with pickle 5 protocol."""

    default = 1024**2 // 4  # 0.25 MiB
    varname = "UNIDIST_MPI_PICKLE_THRESHOLD"


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


class MpiSharedMemoryThreshold(EnvironmentVariable, type=int):
    """Minimum size of data to put into the shared memory."""

    default = 1024**2  # 1 MiB
    varname = "UNIDIST_MPI_SHARED_MEMORY_THRESHOLD"


class MpiUsingSharedMemory(EnvironmentVariable, type=bool):
    """Whether to enable shared memory using for MPI backend or not."""

    default = True
    varname = "UNIDIST_MPI_USING_SHARED_MEMORY"
