# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Dask execution backend."""

from unidist.config import CpuCount, DaskMemoryLimit


def initialize_dask():
    """
    Initialize the Dask execution backend.

    Notes
    -----
    Number of workers for Dask Client is equal to number of CPUs used by the backend.
    """
    from distributed.client import get_client

    try:
        get_client()
    except ValueError:
        from distributed.client import Client

        num_cpus = CpuCount.get()
        memory_limit = DaskMemoryLimit.get()
        worker_memory_limit = memory_limit if memory_limit else "auto"
        Client(n_workers=num_cpus, memory_limit=worker_memory_limit)
