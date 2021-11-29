# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``PythonRunner`` class functionality."""

import warnings

from unidist.cli.base.runner import BackendRunner
from unidist.cli.base.utils import Defaults
from unidist.core.base.common import BackendName


class PythonRunner(BackendRunner):
    """
    An implementation of unidist ``BackendRunner`` for Python backend.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.backend = BackendName.PY
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for Python backend."""
        if kwargs.get("num_cpus", Defaults.NUM_CPUS) != Defaults.NUM_CPUS:
            warnings.warn(
                f"`num_cpus` isn't supported for {self.backend} backend.",
                RuntimeWarning,
            )
        if kwargs.get("hosts", Defaults.HOSTS) != Defaults.HOSTS:
            warnings.warn(
                f"`hosts` isn't supported for {self.backend} backend.",
                RuntimeWarning,
            )
        if (
            kwargs.get("redis_password", Defaults.REDIS_PASSWORD)
            != Defaults.REDIS_PASSWORD
        ):
            warnings.warn(
                f"`redis_password` isn't supported for {self.backend} backend.",
                RuntimeWarning,
            )
