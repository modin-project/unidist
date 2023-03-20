# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API."""

from .api import (
    init,
    is_initialized,
    shutdown,
    remote,
    put,
    get,
    wait,
    is_object_ref,
    get_ip,
    num_cpus,
    cluster_resources,
)
from ._version import get_versions

__all__ = [
    "init",
    "is_initialized",
    "shutdown",
    "remote",
    "put",
    "get",
    "wait",
    "is_object_ref",
    "get_ip",
    "num_cpus",
    "cluster_resources",
]

__version__ = get_versions()["version"]
del get_versions
