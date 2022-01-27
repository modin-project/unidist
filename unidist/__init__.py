# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API."""

from .api import (
    init,
    shutdown,
    remote,
    put,
    get,
    wait,
    is_object_ref,
    get_ip,
    num_cpus,
)
from ._version import get_versions

__all__ = [
    "init",
    "shutdown",
    "remote",
    "put",
    "get",
    "wait",
    "is_object_ref",
    "get_ip",
    "num_cpus",
]

__version__ = get_versions()["version"]
del get_versions
