# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``RayRunner`` class functionality."""

import os
import warnings

from ray import ray_constants

from unidist.cli.base.runner import BackendRunner
from unidist.cli.base.utils import Defaults, validate_num_cpus
from unidist.core.base.common import BackendName


class RayRunner(BackendRunner):
    """
    An implementation of unidist ``BackendRunner`` for Ray backend.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.backend = BackendName.RAY
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for Ray backend."""
        hosts = kwargs.get("hosts", self.hosts)
        num_cpus = kwargs.get("num_cpus", self.num_cpus)
        self.redis_password = (
            self.redis_password
            if kwargs.get("redis_password", Defaults.REDIS_PASSWORD)
            != Defaults.REDIS_PASSWORD
            else ray_constants.REDIS_DEFAULT_PASSWORD
        )
        if hosts == Defaults.HOSTS:
            self.hosts = None
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

    def prepare_env(self):
        """Setup unidist environment variables for Ray backend."""
        super().prepare_env()

        if self.hosts is not None:
            os.environ["UNIDIST_RAY_CLUSTER"] = "True"
            os.environ["UNIDIST_RAY_REDIS_ADDRESS"] = self.hosts
            os.environ["UNIDIST_RAY_REDIS_PASSWORD"] = self.redis_password
        else:
            os.environ["UNIDIST_CPUS"] = self.num_cpus
