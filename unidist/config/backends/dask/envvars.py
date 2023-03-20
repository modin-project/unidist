# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for Dask backend which can be used for unidist behavior tuning."""

from unidist.config.parameter import EnvironmentVariable, ExactStr


class DaskMemoryLimit(EnvironmentVariable, type=int):
    """How many bytes of memory that Dask worker should use."""

    varname = "UNIDIST_DASK_MEMORY_LIMIT"


class IsDaskCluster(EnvironmentVariable, type=bool):
    """Whether Dask is running on pre-initialized Dask cluster."""

    varname = "UNIDIST_DASK_CLUSTER"


class DaskSchedulerAddress(EnvironmentVariable, type=ExactStr):
    """Dask Scheduler address to connect to when running in Dask cluster."""

    varname = "UNIDIST_DASK_SCHEDULER_ADDRESS"
