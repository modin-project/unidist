# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MultiProcessing execution backend."""

from unidist.config import CpuCount


def initialize_multiprocessing():
    """
    Initialize the MultiProcessing execution backend.

    Notes
    -----
    Number of workers for MultiProcessing is equal to number of CPUs used by the backend.
    """
    from unidist.core.backends.multiprocessing.core import init

    init(num_workers=CpuCount.get())
