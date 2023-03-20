# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities specific for MPI backend which can be used for unidist behavior tuning."""

from .envvars import IsMpiSpawnWorkers, MpiHosts, MpiPickleThreshold

__all__ = ["IsMpiSpawnWorkers", "MpiHosts", "MpiPickleThreshold"]
