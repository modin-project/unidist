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
    """Minimum buffer size for serialization with pickle 5 protocol"""

    default = 1024**2 // 4  # 0.25 MiB
    varname = "UNIDIST_MPI_PICKLE_THRESHOLD"


class BackOff(EnvironmentVariable, type=int):
    """Minimum buffer size for serialization with pickle 5 protocol"""

    default = 0.001
    varname = "BackOff"
