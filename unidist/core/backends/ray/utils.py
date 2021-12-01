# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Ray execution backend."""

import os
import psutil
import ray
import sys
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
            object_store_memory = RayObjectStoreMemory.get()
            # In case anything failed above, we can still improve the memory for unidist.
            if object_store_memory is None:
                virtual_memory = psutil.virtual_memory().total
                if sys.platform.startswith("linux"):
                    shm_fd = os.open("/dev/shm", os.O_RDONLY)
                    try:
                        shm_stats = os.fstatvfs(shm_fd)
                        system_memory = shm_stats.f_bsize * shm_stats.f_bavail
                        if system_memory / (virtual_memory / 2) < 0.99:
                            warnings.warn(
                                f"The size of /dev/shm is too small ({system_memory} bytes). The required size "
                                f"at least half of RAM ({virtual_memory // 2} bytes). Please, delete files in /dev/shm or "
                                "increase size of /dev/shm with --shm-size in Docker. Also, you can set "
                                "the required memory size for each Ray worker in bytes to UNIDIST_RAY_OBJECT_STORE_MEMORY environment variable."
                            )
                    finally:
                        os.close(shm_fd)
                else:
                    system_memory = virtual_memory
                object_store_memory = int(0.6 * system_memory // 1e9 * 1e9)
                # If the memory pool is smaller than 2GB, just use the default in ray.
                if object_store_memory == 0:
                    object_store_memory = None
            else:
                object_store_memory = int(object_store_memory)

            ray_init_kwargs = {
                "num_cpus": CpuCount.get(),
                "num_gpus": RayGpuCount.get(),
                "include_dashboard": False,
                "ignore_reinit_error": True,
                "object_store_memory": object_store_memory,
                "address": redis_address,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
            }
            ray.init(**ray_init_kwargs)
