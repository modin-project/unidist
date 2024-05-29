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
from . import _version

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

__version__ = _version.get_versions()["version"]
