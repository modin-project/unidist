# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MultiProcessing execution backend."""

from unidist.config import CpuCount


def initialize_multiprocessing(num_cpus=None):
    """
    Initialize the MultiProcessing execution backend.

    Parameters
    ----------
    num_cpus : int, optional
        Number of CPUs that should be used by backend. If ``None``, ``CpuCount`` is used.

    Notes
    -----
    Number of workers for MultiProcessing is equal to number of CPUs used by the backend.
    """
    from unidist.core.backends.multiprocessing.core import init

    init(num_workers=num_cpus or CpuCount.get())
