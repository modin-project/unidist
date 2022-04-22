# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""MultiProcessing backend core functionality."""

from .actor import Actor
from .api import put, wait, get, submit, init, shutdown

__all__ = ["Actor", "put", "wait", "get", "submit", "init", "shutdown"]
