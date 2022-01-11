# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Command line interface for unidist."""

import os
import sys
import argparse

try:
    import unidist  # noqa: F401
except ImportError:
    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    )

from unidist.cli.base.utils import Defaults
from unidist.core.base.common import BackendName


def main():
    """Run an application with unidist."""
    usage_examples = [
        "\n\tIn case 'unidist' is installed as a python package, run binary:",
        "\n\tunidist script.py  # Ray backend is used by default",
        f"\n\tunidist script.py --backend {BackendName.MPI}  # MPI backend is used",
        f"\n\tunidist -m pytest script.py -b {BackendName.DASK}  # Dask backend is used, running the script using 'pytest'",
        f"\n\tunidist script.py -b {BackendName.MP} --num_cpus 16 # MultiProcessing backend is used and uses 16 CPUs",
        "\n\n\tTo run from sources run 'unidist/cli':",
        f"\n\tpython unidist/cli script.py -b {BackendName.MPI} --num_cpus 16 -hosts localhost  # MPI backend uses 16 workers on 'localhost' node",
        f"\n\tpython unidist/cli script.py -b {BackendName.MPI} -num_cpus 2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on 'localhost' and 4 on 'x.x.x.x'",
    ]
    parser = argparse.ArgumentParser(
        description="Run python code with 'unidist'.",
        usage="".join(usage_examples),
    )

    required_args_group = parser.add_argument_group("required arguments")
    ray_specific_args_group = parser.add_argument_group(
        "Ray backend-specific arguments"
    )
    required_args_group.add_argument("script", help="specify a script to be run")
    parser.add_argument(
        "-b",
        "--backend",
        type=str,
        choices=[
            BackendName.RAY,
            BackendName.MPI,
            BackendName.DASK,
            BackendName.MP,
            BackendName.PY,
        ],
        default=Defaults.BACKEND,
        help=f"specify an execution backend. Default value is taken from 'UNIDIST_BACKEND' environment variable. If 'UNIDIST_BACKEND' isn't set, '{BackendName.RAY}' backend is used",
    )
    parser.add_argument(
        "-m",
        "--module",
        type=str,
        help="specify a python module to run your script with",
    )
    parser.add_argument(
        "-num_cpus",
        "--num_cpus",
        type=int,
        default=Defaults.NUM_CPUS,
        nargs="+",
        help=" ".join(
            [
                "specify a number of CPUs per node used by the backend in a cluster.",
                "Can accept multiple values in the case of running in the cluster.",
                "Default value is taken from 'UNIDIST_CPUS' environment variable. If 'UNIDIST_CPUS' isn't set, value is equal to the number of CPUs on a head node.",
            ]
        ),
    )
    parser.add_argument(
        "-hosts",
        "--hosts",
        type=str,
        default=Defaults.HOSTS,
        nargs="+",
        help="specify node(s) IP address(es) to use by the backend. Can accept multiple values in the case of running in a cluster. Default is 'localhost'.",
    )
    ray_specific_args_group.add_argument(
        "-redis_pswd",
        "--redis_password",
        default=Defaults.REDIS_PASSWORD,
        type=str,
        help="specify redis password to connect to existing Ray cluster.",
    )
    args, script_args = parser.parse_known_args()
    kwargs = vars(args)
    kwargs["script_args"] = script_args

    backend = kwargs.pop("backend", Defaults.BACKEND)
    if backend == BackendName.RAY:
        from unidist.cli.backends.ray.runner import RayRunner

        runner = RayRunner(**kwargs)
    elif backend == BackendName.MPI:
        from unidist.cli.backends.mpi.runner import MPIRunner

        runner = MPIRunner(**kwargs)
    elif backend == BackendName.DASK:
        from unidist.cli.backends.dask.runner import DaskRunner

        runner = DaskRunner(**kwargs)
    elif backend == BackendName.MP:
        from unidist.cli.backends.multiprocessing.runner import MultiProcessingRunner

        runner = MultiProcessingRunner(**kwargs)
    else:
        from unidist.cli.backends.python.runner import PythonRunner

        runner = PythonRunner(**kwargs)
    runner.run()


if __name__ == "__main__":
    main()
