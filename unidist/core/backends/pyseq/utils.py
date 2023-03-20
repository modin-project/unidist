# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize Python Sequential execution backend."""


def initialize_pyseq():
    """
    Initialize the Python Sequential execution backend.

    Notes
    -----
    All execution will happen sequentially.
    """
    from unidist.core.backends.pyseq.core import init

    init()
