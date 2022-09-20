# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""MPI backend functionality related to `worker` concept."""

from unidist.core.backends.mpi.core.worker.worker import worker_loop

__all__ = ["worker_loop"]
