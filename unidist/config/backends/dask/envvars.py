# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for Dask backend which can be used for unidist behavior tuning."""

from unidist.config.parameter import EnvironmentVariable


class DaskMemoryLimit(EnvironmentVariable, type=int):
    """How many bytes of memory that Dask worker should use."""

    varname = "UNIDIST_DASK_MEMORY_LIMIT"
