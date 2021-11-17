# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Command line interface for unidist."""

import os
import argparse
import subprocess
import multiprocessing as mp
import ipaddress
import socket


def _get_unidist_root():
    """
    Get the project root directory.

    Returns
    -------
    str
        Absolute path to the project root directory.
    """
    unidist_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.environ["PYTHONPATH"] = (
        unidist_root + os.pathsep + os.environ.get("PYTHONPATH", "")
    )

    return unidist_root


def _get_localhost_ip():
    """
    Get a public IP-address of the head node.

    Returns
    -------
    str
        Public IP-address of the head node.
    """
    return socket.gethostbyname(socket.gethostname())


def _validate_hosts(hosts: list):
    """
    Validate `hosts` list of ip-addresses on correctness and duplicates.

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
        str(ipaddress.ip_address(_get_localhost_ip() if ip == "localhost" else ip))
        for ip in hosts
    ]
    duplicate_ips = [ip for ip in set(ips) if ips.count(ip) > 1]
    if len(duplicate_ips):
        raise RuntimeError(f"'hosts' list contains duplicates {duplicate_ips}")
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
        List of validated number of workers per host.
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
                    f"'num_workers' must be integer, 'default' or sequence of integers, got '{num_workers}'"
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
    Create a command to be run in a subprocess.

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
        unidist_root = _get_unidist_root()

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
        command = ["mpiexec", "-hosts"]

        hosts_str = f"{_get_localhost_ip()}:1,{_get_localhost_ip()}:1,"
        for host, n in zip(hosts, num_workers):
            hosts_str += host + ":" + n + ","

        command_executor = ["-n", "1", executor, script]
        command_monitor = [
            "-n",
            "1",
            "-wdir",
            workers_dir,
            "python3",
            unidist_root + "/unidist/core/backends/mpi/core/monitor.py",
        ]

        def get_worker_command(num_workers):
            return [
                "-n",
                num_workers,
                "-wdir",
                workers_dir,
                "python3",
                unidist_root + "/unidist/core/backends/mpi/core/worker.py",
            ]

        command += [hosts_str] + command_executor + [":"] + command_monitor

        for n in num_workers:
            command += [":"] + get_worker_command(n)
    else:
        command = [executor, script]

    return command


def main():
    """Run an application with unidist."""
    usage_examples = [
        "\n\tIn case 'unidist' is installed as a python package, use binary:",
        "\n\tunidist script.py  # Ray backend is used by default",
        "\n\tunidist script.py --backend MPI  # MPI backend is used",
        "\n\tunidist script.py --executor pytest -b Dask  # Dask backend is used, running the script using 'pytest'",
        "\n\n\tTo run from sources use 'unidist/run.py':",
        "\n\tpython unidist/run.py script.py -b MPI --num_workers=16 --hosts localhost  # MPI backend uses 16 workers on 'localhost' node",
        "\n\tpython unidist/run.py script.py -b MPI --num_workers=2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on 'localhost' and 4 on 'x.x.x.x'",
    ]
    parser = argparse.ArgumentParser(
        description="Run python code with 'unidist' under the hood.",
        usage="".join(usage_examples),
    )

    required_args_group = parser.add_argument_group("required arguments")
    mpi_args_group = parser.add_argument_group("MPI backend-specific arguments")
    required_args_group.add_argument("script", help="specify a script to be run")
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
