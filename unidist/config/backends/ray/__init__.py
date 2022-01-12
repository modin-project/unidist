# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for Ray backend which can be used for unidist behavior tuning."""

from .envvars import (
    RayGpuCount,
    IsRayCluster,
    RayRedisAddress,
    RayRedisPassword,
    RayObjectStoreMemory,
)

__all__ = [
    "RayGpuCount",
    "IsRayCluster",
    "RayRedisAddress",
    "RayRedisPassword",
    "RayObjectStoreMemory",
]
