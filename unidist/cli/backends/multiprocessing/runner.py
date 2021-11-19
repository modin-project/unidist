# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``MultiProcessingRunner`` class functionality."""

import os
import warnings

from unidist.cli.backends.base.runner import BackendRunner
from unidist.cli.backends.utils import Defaults, BackendName, validate_num_cpus


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
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check the support of `kwargs` combination for MultiProcessing backend."""
        hosts = kwargs.get("hosts", Defaults.HOSTS)
        num_cpus = kwargs.get("num_cpus", Defaults.NUM_CPUS)
        if hosts == Defaults.HOSTS:
            if num_cpus == Defaults.NUM_CPUS:
                self.num_cpus = validate_num_cpus([num_cpus])
            elif isinstance(num_cpus, list) and len(num_cpus) == 1:
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
                f"`redis_password` isn't supported for {self.backend} backend, ignored.",
                RuntimeWarning,
            )

    def prepare_env(self):
        """Setup unidist environment variables for MultiProcessing backend."""
        super().prepare_env()
        os.environ["UNIDIST_CPUS"] = self.num_cpus