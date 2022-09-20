# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Ray execution backend."""

import os
from packaging import version
import psutil
import ray
import sys
from typing import Optional
import warnings

from unidist.config import (
    CpuCount,
    RayGpuCount,
    IsRayCluster,
    RayRedisAddress,
    RayRedisPassword,
    RayObjectStoreMemory,
    ValueSource,
)

_OBJECT_STORE_TO_SYSTEM_MEMORY_RATIO = 0.6
# This constant should be in sync with the limit in ray, which is private,
# not exposed to users, and not documented:
# https://github.com/ray-project/ray/blob/4692e8d8023e789120d3f22b41ffb136b50f70ea/python/ray/_private/ray_constants.py#L57-L62
_MAC_OBJECT_STORE_LIMIT_BYTES = 2 * 2**30


def initialize_ray():
    """
    Initialize the Ray execution backend.

    Notes
    -----
    Number of workers for Ray is equal to number of CPUs used by the backend.
    """
    if not ray.is_initialized() or IsRayCluster.get():
        cluster = IsRayCluster.get()
        redis_address = RayRedisAddress.get()
        redis_password = (
            (
                ray.ray_constants.REDIS_DEFAULT_PASSWORD
                if cluster
                else RayRedisPassword.get()
            )
            if RayRedisPassword.get_value_source() == ValueSource.DEFAULT
            else RayRedisPassword.get()
        )

        if cluster:
            # We only start ray in a cluster setting for the head node.
            ray.init(
                address=redis_address or "auto",
                include_dashboard=False,
                ignore_reinit_error=True,
                _redis_password=redis_password,
            )
        else:
            object_store_memory = _get_object_store_memory()
            ray_init_kwargs = {
                "num_cpus": CpuCount.get(),
                "num_gpus": RayGpuCount.get(),
                "include_dashboard": False,
                "ignore_reinit_error": True,
                "object_store_memory": object_store_memory,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
            }
            ray.init(**ray_init_kwargs)


def _get_object_store_memory() -> Optional[int]:
    """
    Get the object store memory we should start Ray with, in bytes.

    - If the ``Memory`` config variable is set, return that.
    - On Linux, take system memory from /dev/shm. On other systems use total
      virtual memory.
    - On Mac, never return more than Ray-specified upper limit.

    Returns
    -------
    Optional[int]
        The object store memory size in bytes, or None if we should use the Ray
        default.
    """
    object_store_memory = RayObjectStoreMemory.get()
    if object_store_memory is not None:
        return object_store_memory
    virtual_memory = psutil.virtual_memory().total
    if sys.platform.startswith("linux"):
        shm_fd = os.open("/dev/shm", os.O_RDONLY)
        try:
            shm_stats = os.fstatvfs(shm_fd)
            system_memory = shm_stats.f_bsize * shm_stats.f_bavail
            if system_memory / (virtual_memory / 2) < 0.99:
                warnings.warn(
                    f"The size of /dev/shm is too small ({system_memory} bytes). The required size "
                    + f"at least half of RAM ({virtual_memory // 2} bytes). Please, delete files in /dev/shm or "
                    + "increase size of /dev/shm with --shm-size in Docker. Also, you can can override the memory "
                    + "size for each Ray worker (in bytes) to the MODIN_MEMORY environment variable."
                )
        finally:
            os.close(shm_fd)
    else:
        system_memory = virtual_memory
    bytes_per_gb = 1e9
    object_store_memory = int(
        _OBJECT_STORE_TO_SYSTEM_MEMORY_RATIO
        * system_memory
        // bytes_per_gb
        * bytes_per_gb
    )
    if object_store_memory == 0:
        return None
    # Newer versions of ray don't allow us to initialize ray with object store
    # size larger than that _MAC_OBJECT_STORE_LIMIT_BYTES. It seems that
    # object store > the limit is too slow even on ray 1.0.0. However, limiting
    # the object store to _MAC_OBJECT_STORE_LIMIT_BYTES only seems to start
    # helping at ray version 1.3.0. So if ray version is at least 1.3.0, cap
    # the object store at _MAC_OBJECT_STORE_LIMIT_BYTES.
    # For background on the ray bug see:
    # - https://github.com/ray-project/ray/issues/20388
    # - https://github.com/modin-project/modin/issues/4872
    if sys.platform == "darwin" and version.parse(ray.__version__) >= version.parse(
        "1.3.0"
    ):
        object_store_memory = min(object_store_memory, _MAC_OBJECT_STORE_LIMIT_BYTES)
    return object_store_memory
