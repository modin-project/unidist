# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities which can be used for common unidist behavior tuning."""

from .envvars import Backend, CpuCount

__all__ = ["Backend", "CpuCount"]
