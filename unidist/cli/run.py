# Copyright (C) 2021 Modin authors
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

from unidist.cli.backends.utils import BackendName, Defaults
from unidist.cli.backends.ray.runner import RayRunner
from unidist.cli.backends.mpi.runner import MPIRunner
from unidist.cli.backends.dask.runner import DaskRunner
from unidist.cli.backends.python.runner import PythonRunner
from unidist.cli.backends.multiprocessing.runner import MultiProcessingRunner


def main():
    """Run an application with unidist."""
    usage_examples = [
        "\n\tIn case 'unidist' is installed as a python package, use binary:",
        "\n\tunidist script.py  # Ray backend is used by default",
        "\n\tunidist script.py --backend MPI  # MPI backend is used",
        "\n\tunidist script.py --executor pytest -b Dask  # Dask backend is used, running the script using 'pytest'",
        "\n\tunidist script.py -b MultiProcessing --num_cpus 16 # MultiProcessing backend is used and uses 16 CPUs,",
        "\n\n\tTo run from sources use 'unidist/cli/run.py':",
        "\n\tpython unidist/cli/run.py script.py -b MPI --num_cpus 16 -hosts localhost  # MPI backend uses 16 workers on 'localhost' node",
        "\n\tpython unidist/cli/run.py script.py -b MPI -num_cpus 2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on 'localhost' and 4 on 'x.x.x.x'",
    ]
    parser = argparse.ArgumentParser(
        description="Run python code with 'unidist' under the hood.",
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
        help="set an execution backend. Default is 'Ray'",
    )
    parser.add_argument(
        "-e",
        "--executor",
        type=str,
        default=Defaults.EXECUTOR,
        help="set an executable to run. Default is 'python'",
    )
    parser.add_argument(
        "-num_cpus",
        "--num_cpus",
        default=Defaults.NUM_CPUS,
        nargs="+",
        help="set a number of workers per node in a cluster. Can accept multiple values in a case of working on the cluster. Default is equal to the number of CPUs on a head node.",
    )
    parser.add_argument(
        "-hosts",
        "--hosts",
        default=Defaults.HOSTS,
        nargs="+",
        help="set a node ip address to use. Can accept multiple values in a case of working on the cluster. Default is 'localhost'.",
    )
    ray_specific_args_group.add_argument(
        "-redis_pswd",
        "--redis_password",
        default=Defaults.REDIS_PASSWORD,
        type=str,
        help="set redis password to connect to existing Ray cluster.",
    )
    args = parser.parse_args()
    kwargs = vars(args)

    backend = kwargs.get("backend")
    if backend == BackendName.RAY:
        runner = RayRunner(**kwargs)
    elif backend == BackendName.MPI:
        runner = MPIRunner(**kwargs)
    elif backend == BackendName.DASK:
        runner = DaskRunner(**kwargs)
    elif backend == BackendName.MP:
        runner = MultiProcessingRunner(**kwargs)
    else:
        runner = PythonRunner(**kwargs)
    runner.run()


if __name__ == "__main__":
    main()
