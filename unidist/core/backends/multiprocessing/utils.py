# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MultiProcessing execution backend."""


def initialize_multiprocessing():
    """
    Initialize the MultiProcessing execution backend.

    Notes
    -----
    A number of workers will be equal
    to the number of CPUs on the head node.
    """
    from multiprocessing import cpu_count

    from unidist.core.backends.multiprocessing.core import init

    init(num_workers=cpu_count())
