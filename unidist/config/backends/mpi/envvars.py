# Copyright (C) 2021-2022 Modin authors
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
