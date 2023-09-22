# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MPI execution backend."""


def initialize_mpi():
    """Initialize the MPI execution backend."""
    from unidist.core.backends.mpi.core import init

    init()


class ImmutableDict(dict):
    __readonly_exception = TypeError("Cannot modify `ImmutableDict`")

    def __setitem__(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def __delitem__(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def pop(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def popitem(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def clear(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def update(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception

    def setdefault(self, *args, **kwargs):
        raise ImmutableDict.__readonly_exception
