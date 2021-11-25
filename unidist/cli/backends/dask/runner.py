# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``DaskRunner`` class functionality."""

import os
import warnings

from unidist.cli.base.runner import BackendRunner
from unidist.cli.base.utils import Defaults, BackendName, validate_num_cpus


class DaskRunner(BackendRunner):
    """
    An implementation of unidist ``BackendRunner`` for Dask backend.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.backend = BackendName.DASK
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for Dask backend."""
        hosts = kwargs.get("hosts", self.hosts)
        num_cpus = kwargs.get("num_cpus", self.num_cpus)
        if hosts == Defaults.HOSTS:
            self.hosts = None
            if num_cpus == Defaults.NUM_CPUS:
                self.num_cpus = validate_num_cpus([num_cpus])[0]
            elif isinstance(num_cpus, list) and len(num_cpus) == 1:
                self.num_cpus = validate_num_cpus(num_cpus)[0]
            else:
                raise RuntimeError(
                    f"`num_cpus` must have a single value for {self.backend} backend."
                )
        elif isinstance(hosts, list) and len(hosts) == 1:
            self.hosts = hosts[0]
            if isinstance(num_cpus, list):
                warnings.warn(
                    f"`num_cpus` isn't supported for existing {self.backend} cluster, ignored.",
                    RuntimeWarning,
                )
            self.num_cpus = None
        else:
            raise RuntimeError(
                f"`hosts` must have a single value with existing cluster address for {self.backend} backend."
            )

        if (
            kwargs.get("redis_password", Defaults.REDIS_PASSWORD)
            != Defaults.REDIS_PASSWORD
        ):
            warnings.warn(
                f"`redis_password` isn't supported for {self.backend} backend, ignored.",
                RuntimeWarning,
            )

    def prepare_env(self):
        """Setup unidist environment variables for Dask backend."""
        super().prepare_env()

        if self.hosts is not None:
            os.environ["UNIDIST_DASK_CLUSTER"] = "True"
            os.environ["UNIDIST_DASK_SCHEDULER_ADDRESS"] = self.hosts
        else:
            os.environ["UNIDIST_CPUS"] = self.num_cpus
