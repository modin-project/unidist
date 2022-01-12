# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for Ray backend which can be used for unidist behavior tuning."""

import secrets

from unidist.config.parameter import EnvironmentVariable, ExactStr


class RayGpuCount(EnvironmentVariable, type=int):
    """How many GPU devices to use during initialization of the Ray backend."""

    varname = "UNIDIST_RAY_GPUS"


class IsRayCluster(EnvironmentVariable, type=bool):
    """Whether Ray is running on pre-initialized Ray cluster."""

    varname = "UNIDIST_RAY_CLUSTER"


class RayRedisAddress(EnvironmentVariable, type=ExactStr):
    """Redis address to connect to when running in Ray cluster."""

    varname = "UNIDIST_RAY_REDIS_ADDRESS"


class RayRedisPassword(EnvironmentVariable, type=ExactStr):
    """What password to use for connecting to Redis."""

    varname = "UNIDIST_RAY_REDIS_PASSWORD"
    default = secrets.token_hex(32)


class RayObjectStoreMemory(EnvironmentVariable, type=int):
    """How many bytes of memory to start the Ray object store with."""

    varname = "UNIDIST_RAY_OBJECT_STORE_MEMORY"
