# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MPI execution backend."""


def initialize_mpi():
    """Initialize the MPI execution backend."""
    from unidist.core.backends.mpi.core import init

    init()


def is_namedTuple_instance(obj):
    t = type(obj)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)
