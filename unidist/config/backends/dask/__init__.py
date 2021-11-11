# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for Dask backend which can be used for unidist behavior tuning."""

from .envvars import DaskMemoryLimit

__all__ = ["DaskMemoryLimit"]
