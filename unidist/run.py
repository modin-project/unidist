import os
import argparse
import subprocess
import multiprocessing as mp
import ipaddress


def get_unidist_home():
    unidist_home_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.environ["PYTHONPATH"] = (
        os.environ.get("PYTHONPATH", "") + os.pathsep + unidist_home_path
    )

    return unidist_home_path


def validate_hosts(ips):
    if not isinstance(ips, list):
        ips = [ips]

    return [
        str(ipaddress.ip_address("127.0.0.1" if ip == "localhost" else ip))
        for ip in ips
    ]


def validate_num_workers(n_workers):
    if not isinstance(n_workers, list):
        n_workers = [n_workers]

    def validate(value):
        try:
            value = int(value)
        except Exception:
            if value == "default":
                return mp.cpu_count()
            else:
                raise TypeError(
                    f"`num_workers` should be integer, 'default' or sequence of integers but got '{n_workers}'"
                )
        else:
            return value

    return [str(validate(n)) for n in n_workers]


def create_command(args):
    if args.backend == "MPI":
        unidist_home_path = get_unidist_home()

        hosts = validate_hosts(args.hosts)
        n_workers = validate_num_workers(args.n_workers)

        assert len(hosts) == len(
            n_workers
        ), "`n_workers` and `hosts` parameters must have the similar number of values."

        command = ["mpirun"]
        command_executor = ["-n", "1", "-host", "127.0.0.1", args.executor, args.script]
        command_monitor = [
            "-n",
            "1",
            "-host",
            "127.0.0.1",
            "-wdir",
            "/tmp",
            args.executor,
            unidist_home_path + "/unidist/core/backends/mpi/core/monitor.py",
        ]

        def get_worker_command(ip, num_workers):
            return [
                "-n",
                num_workers,
                "-host",
                ip,
                "-wdir",
                "/tmp",
                args.executor,
                unidist_home_path + "/unidist/core/backends/mpi/core/worker.py",
            ]

        command += command_executor + [":"] + command_monitor

        for host, n in zip(hosts, n_workers):
            command += [":"] + get_worker_command(host, n)
    else:
        command = [args.executor, args.script]

    return command


def main():
    usage_examples = [
        "\n\tIn case 'unidist' is installed, use binary:",
        "\n\tunidist script.py  # Ray backend is used",
        "\n\tunidist script.py --backend MPI  # MPI backend is used",
        "\n\tunidist script.py --executor pytest -b Dask  # Dask backend is used, running using 'pytest'",
        "\n\n\tTo run from sources use 'unidist/run.py':",
        "\n\tpython3 unidist/run.py script.py -b MPI --num_workers=16 --hosts localhost  # MPI backend uses 16 workers on 'localhost' node",
        "\n\tunidist script.py -b MPI --num_workers=2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on `localhost` and 4 on 'x.x.x.x'",
    ]
    parser = argparse.ArgumentParser(
        description="Run python code with `unidist` under the hood.",
        usage="".join(usage_examples),
    )

    parser.add_argument_group()
    parser.add_argument("script", help="set script to be run")
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
    parser.add_argument(
        "-num_workers",
        "--num_workers",
        default="default",
        nargs="+",
        help="set a number of workers per node in a cluster. Can accept multiple values in a case of working on the cluster. Default is equal to the number of CPUs on a head node.",
    )
    parser.add_argument(
        "-hosts",
        "--hosts",
        default="localhost",
        nargs="+",
        help="set a node ip address to use. Can accept multiple values in a case of working on the cluster. Default is 'localhost'.",
    )
    args = parser.parse_args()

    os.environ["UNIDIST_BACKEND"] = args.backend

    command = create_command(args)
    subprocess.run(command)


if __name__ == "__main__":
    main()
