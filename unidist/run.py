# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Command line interface for unidist."""

import os
import argparse
import subprocess
import multiprocessing as mp
import ipaddress


def _get_unidist_home_path():
    """
    Get a home path of unidist package.

    Returns
    -------
    str
        Unidist home path.
    """
    unidist_home_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.environ["PYTHONPATH"] = (
        os.environ.get("PYTHONPATH", "") + os.pathsep + unidist_home_path
    )

    return unidist_home_path


def _validate_hosts(hosts: list):
    """
    Validate `hosts` list of ip-addresses on correctness and check duplicates.

    Parameters
    ----------
    hosts : list
        List of strings with ip-addresses.

    Returns
    -------
    list
        List of validated IPs.
    """
    ips = [
        str(ipaddress.ip_address("127.0.0.1" if ip == "localhost" else ip))
        for ip in hosts
    ]
    ips_duplicated = [ip for ip in set(ips) if ips.count(ip) > 1]
    if len(ips_duplicated):
        raise RuntimeError(f"'hosts' list contains duplicates {ips_duplicated}")
    return ips


def _validate_num_workers(num_workers: list):
    """
    Validate `num_workers` on correctness.

    Each value of `num_workers` is checked on possibility
    of converting to int. In case value is ``default`` it will
    be equal to number of CPUs on ``localhost`` node.

    Parameters
    ----------
    num_workers : list
        List of string values. The each value represents
        a number of workers for corresponded host.

    Returns
    -------
    list
        List of validated numbers of workers per hosts.
    """

    def validate(value):
        try:
            value = int(value)
            if value < 1:
                raise RuntimeError(
                    "'num_workers' must be more than 0, got '{num_workers}'"
                )
        except ValueError:
            if value == "default":
                return mp.cpu_count()
            else:
                raise TypeError(
                    f"`num_workers` must be integer, 'default' or sequence of integers, got '{num_workers}'"
                )
        else:
            return value

    return [str(validate(n)) for n in num_workers]


def create_command(
    script,
    executor="python3",
    backend="Ray",
    num_workers=[mp.cpu_count()],
    hosts=["localhost"],
):
    """
    Create a command to be runned in a subprocess.

    Parameters
    ----------
    script : str
        Name of .py script to be run.
    executor : str, default: 'python3'
        Executable to run `script`.
    backend : str, default: 'Ray'
        Unidist backend name to use.
    num_workers : list, default: ['localhost' cpu count]
        List of string values. The each value represents
        a number of workers for corresponded host.
    hosts : list, default: ['localhost']
        List of strings with ip-addresses.

    Returns
    -------
    list
        List of strings represents command for ``subprocess``.
    """
    if backend == "MPI":
        unidist_home_path = _get_unidist_home_path()

        if len(hosts) != len(num_workers):
            # If `num_workers` isn't provided or a single value `default` is provided
            # all workers will use `default` value
            if len(num_workers) == 1 and num_workers[0] == "default":
                num_workers *= len(hosts)
            else:
                raise RuntimeError(
                    "`num_workers` and `hosts` parameters must have the equal number of values."
                )

        hosts = _validate_hosts(hosts)
        num_workers = _validate_num_workers(num_workers)
        workers_dir = "/tmp"
        command = ["mpirun"]
        command_executor = ["-n", "1", "-host", "127.0.0.1", executor, script]
        command_monitor = [
            "-n",
            "1",
            "-host",
            "127.0.0.1",
            "-wdir",
            workers_dir,
            "python3",
            unidist_home_path + "/unidist/core/backends/mpi/core/monitor.py",
        ]

        def get_worker_command(ip, num_workers):
            return [
                "-n",
                num_workers,
                "-host",
                ip,
                "-wdir",
                workers_dir,
                "python3",
                unidist_home_path + "/unidist/core/backends/mpi/core/worker.py",
            ]

        command += command_executor + [":"] + command_monitor

        for host, n in zip(hosts, num_workers):
            command += [":"] + get_worker_command(host, n)
    else:
        command = [executor, script]

    return command


def main():
    """Run an unidist application."""
    usage_examples = [
        "\n\tIn case 'unidist' is installed, use binary:",
        "\n\tunidist script.py  # Ray backend is used",
        "\n\tunidist script.py --backend MPI  # MPI backend is used",
        "\n\tunidist script.py --executor pytest -b Dask  # Dask backend is used, running using 'pytest'",
        "\n\n\tTo run from sources use 'unidist/run.py':",
        "\n\tpython3 unidist/run.py script.py -b MPI --num_workers=16 --hosts localhost  # MPI backend uses 16 workers on 'localhost' node",
        "\n\tunidist script.py -b MPI --num_workers=2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on 'localhost' and 4 on 'x.x.x.x'",
    ]
    parser = argparse.ArgumentParser(
        description="Run python code with 'unidist' under the hood.",
        usage="".join(usage_examples),
    )

    required_args_group = parser.add_argument_group("required arguments")
    mpi_args_group = parser.add_argument_group("MPI-backend specific arguments")
    required_args_group.add_argument("script", help="set script to be run")
    parser.add_argument(
        "-b",
        "--backend",
        type=str,
        choices=["Ray", "MPI", "Dask", "MultiProcessing", "Python"],
        default="Ray",
        help="set an execution backend. Default is 'Ray'",
    )
    parser.add_argument(
        "-e",
        "--executor",
        type=str,
        default="python3",
        help="set an executable to run. Default is 'python3'",
    )
    mpi_args_group.add_argument(
        "-num_workers",
        "--num_workers",
        default=["default"],
        nargs="+",
        help="set a number of workers per node in a cluster. Can accept multiple values in a case of working on the cluster. Default is equal to the number of CPUs on a head node.",
    )
    mpi_args_group.add_argument(
        "-hosts",
        "--hosts",
        default=["localhost"],
        nargs="+",
        help="set a node ip address to use. Can accept multiple values in a case of working on the cluster. Default is 'localhost'.",
    )
    args = parser.parse_args()
    kwargs = vars(args)

    os.environ["UNIDIST_BACKEND"] = kwargs["backend"]

    command = create_command(kwargs.pop("script"), **kwargs)
    subprocess.run(command)


if __name__ == "__main__":
    main()
