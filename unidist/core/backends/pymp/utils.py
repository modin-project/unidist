# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Python Multiprocessing execution backend."""

from unidist.config import CpuCount


def initialize_pymp():
    """
    Initialize the Python Multiprocessing execution backend.

    Notes
    -----
    Number of workers for Python Multiprocessing is equal to number of CPUs used by the backend.
    """
    from unidist.core.backends.pymp.core import init

    init(num_workers=CpuCount.get())
