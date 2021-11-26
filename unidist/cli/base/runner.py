# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``BackendRunner`` class functionality."""

import os
import subprocess

from unidist.cli.base.utils import Defaults


class BackendRunner:
    """
    Base class for any unidist backend runner.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.script = cli_kwargs.pop("script", "")
        self.executor = cli_kwargs.pop("executor", Defaults.EXECUTOR)
        self.backend = cli_kwargs.pop("backend", Defaults.BACKEND)
        self.hosts = cli_kwargs.get("hosts", Defaults.HOSTS)
        self.num_cpus = cli_kwargs.get("hosts", Defaults.NUM_CPUS)
        self.redis_password = cli_kwargs.get("redis_password", Defaults.REDIS_PASSWORD)
        self.check_kwargs_support(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for a specific backend."""
        raise NotImplementedError

    def prepare_env(self):
        """Setup unidist environment variables for a specific backend."""
        os.environ["UNIDIST_BACKEND"] = self.backend

    def get_command(self):
        """
        Get a command to be run in a subprocess.

        Returns
        -------
        list
            List of strings with the command representation.
        """
        return [self.executor, self.script]

    def run(self):
        """Run a command in a subprocess."""
        self.prepare_env()
        command = self.get_command()
        subprocess.run(command)
