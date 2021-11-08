# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Dask execution backend."""


def initialize_dask():
    """
    Initialize the Dask execution backend.

    Notes
    -----
    A number of workers in Dask Client will be equal
    to the number of CPUs on the head node.
    """
    from distributed.client import get_client

    try:
        get_client()
    except ValueError:
        import multiprocessing
        from distributed.client import Client

        cpu_count = multiprocessing.cpu_count()
        Client(n_workers=cpu_count)
