# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Python execution backend."""


def initialize_python():
    """
    Initialize the Python execution backend.

    Notes
    -----
    All execution will happen sequentially.
    """
    from unidist.core.backends.python.core import init

    init()
