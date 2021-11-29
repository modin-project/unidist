# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``MultiProcessingRunner`` class functionality."""

import os
import warnings

from unidist.cli.base.runner import BackendRunner
from unidist.cli.base.utils import Defaults, validate_num_cpus
from unidist.core.base.common import BackendName


class MultiProcessingRunner(BackendRunner):
    """
    An implementation of unidist ``BackendRunner`` for MultiProcessing backend.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.backend = BackendName.MP
        self.hosts = cli_kwargs.get("hosts", Defaults.HOSTS)
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for MultiProcessing backend."""
        num_cpus = kwargs.get("num_cpus", self.num_cpus)
        if self.hosts == Defaults.HOSTS:
            if (
                num_cpus == Defaults.NUM_CPUS
                or isinstance(num_cpus, list)
                and len(num_cpus) == 1
            ):
                self.num_cpus = validate_num_cpus(num_cpus)[0]
            else:
                raise RuntimeError(
                    f"`num_cpus` must have a single value for {self.backend} backend."
                )
        else:
            raise RuntimeError(f"`hosts` isn't supported by {self.backend} backend.")

        if (
            kwargs.get("redis_password", Defaults.REDIS_PASSWORD)
            != Defaults.REDIS_PASSWORD
        ):
            warnings.warn(
                f"`redis_password` isn't supported for {self.backend} backend.",
                RuntimeWarning,
            )

    def prepare_env(self):
        """Setup unidist environment variables for MultiProcessing backend."""
        super().prepare_env()
        os.environ["UNIDIST_CPUS"] = self.num_cpus
