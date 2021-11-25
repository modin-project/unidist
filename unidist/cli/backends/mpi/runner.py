# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``MPIRunner`` class functionality."""

import os
import warnings

from unidist.cli.base.runner import BackendRunner
from unidist.cli.base.utils import (
    Defaults,
    validate_num_cpus,
    validate_hosts,
    get_localhost_ip,
    get_unidist_root,
)
from unidist.core.base.common import BackendName


class MPIRunner(BackendRunner):
    """
    An implementation of unidist ``BackendRunner`` for MPI backend.

    Parameters
    ----------
    **cli_kwargs : dict
        Keyword arguments supported by unidist CLI.
    """

    def __init__(self, **cli_kwargs):
        self.backend = BackendName.MPI
        super().__init__(**cli_kwargs)

    def check_kwargs_support(self, **kwargs):
        """Check support for `kwargs` combination for MPI backend."""
        hosts = kwargs.get("hosts", self.hosts)
        num_cpus = kwargs.get("num_cpus", self.num_cpus)

        if isinstance(hosts, list) and not isinstance(num_cpus, list):
            # If `num_cpus` isn't provided all workers will use `default` value
            num_cpus = [num_cpus] * len(hosts)

        self.hosts = validate_hosts(hosts)
        self.num_cpus = validate_num_cpus(num_cpus)

        if len(self.hosts) != len(self.num_cpus):
            raise RuntimeError(
                "`num_cpus` and `hosts` parameters must have an equal number of values."
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
        """Setup unidist environment variables for MPI backend."""
        super().prepare_env()
        os.environ["UNIDIST_CPUS"] = str(sum([int(val) for val in self.num_cpus]))

    def get_command(self):
        """
        Get a command to be run in a subprocess.

        Returns
        -------
        list
            List of strings with the command representation.
        """
        unidist_root = get_unidist_root()
        workers_dir = "/tmp"
        command = ["mpiexec", "-hosts"]
        hosts_str = f"{get_localhost_ip()}:1,{get_localhost_ip()}:1,"
        for host, n in zip(self.hosts, self.num_cpus):
            hosts_str += host + ":" + n + ","
        command_executor = ["-n", "1"] + super().get_command()
        command_monitor = [
            "-n",
            "1",
            "-wdir",
            workers_dir,
            "python",
            os.path.join(
                unidist_root, "unidist", "core", "backends", "mpi", "core", "monitor.py"
            ),
        ]

        def get_worker_command(num_cpus):
            return [
                "-n",
                num_cpus,
                "-wdir",
                workers_dir,
                "python",
                os.path.join(
                    unidist_root,
                    "unidist",
                    "core",
                    "backends",
                    "mpi",
                    "core",
                    "worker.py",
                ),
            ]

        command += [hosts_str] + command_executor + [":"] + command_monitor
        for n in self.num_cpus:
            command += [":"] + get_worker_command(n)
        return command
