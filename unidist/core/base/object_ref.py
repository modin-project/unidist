# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Core base object ref specific functionality."""


class ObjectRef:
    """
    A class that wraps an object ref specific for the backend.

    Parameters
    ----------
    ref : object ref
        An object ref specific for the backend.
    """

    def __init__(self, ref):
        self._ref = ref
