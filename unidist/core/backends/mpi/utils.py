# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MPI execution backend."""


def initialize_mpi():
    """Initialize the MPI execution backend."""
    from unidist.core.backends.mpi.core import init

    init()
