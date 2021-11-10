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


def validate_n_workers(n_workers):
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
                    f"`n_workers` should be integer, `default` or sequence of integers but got `{n_workers}`"
                )
        else:
            return value

    return [str(validate(n)) for n in n_workers]


def create_command(args):
    if args.backend == "MPI":
        unidist_home_path = get_unidist_home()

        hosts = validate_hosts(args.hosts)
        n_workers = validate_n_workers(args.n_workers)

        assert len(hosts) == len(
            n_workers
        ), "`n_workers` and `hosts` parameters must have the similar number of values."

        command = [
            "mpiexec",
            "-host",
            "localhost",
        ]
        command_executor = ["-n", "1", args.executor, args.script]
        command_monitor = [
            "-n",
            "1",
            "-wdir",
            "/tmp",
            args.executor,
            unidist_home_path + "/unidist/core/backends/mpi/core/monitor.py",
        ]

        def get_worker_command(ip, num_workers):
            return [
                "-host",
                ip,
                "-n",
                num_workers,
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
    parser = argparse.ArgumentParser(
        description="Run python code with unidist distribution under the hood."
    )
    parser.add_argument("script", help="Set script to be run")
    parser.add_argument(
        "-b",
        "--backend",
        type=str,
        choices=["Ray", "MPI", "Dask", "MultiProcessing", "Python"],
        default="Ray",
        help="set an execution backend. Default is `Ray`",
    )
    parser.add_argument(
        "-n",
        "--n_workers",
        default="default",
        nargs="+",
        help="set a number of workers. If `default`, will be equal to the number of CPUs. Can accept multiple values in case of working on several nodes",
    )
    parser.add_argument(
        "-hosts",
        "--hosts",
        default="localhost",
        nargs="+",
        help="set an node ip to use. Can accept multiple values in case of working on several nodes. Default is `localhost`",
    )
    parser.add_argument(
        "-e",
        "--executor",
        type=str,
        default="python3",
        help="set executable to run. Default is `python3`",
    )
    args = parser.parse_args()

    os.environ["UNIDIST_BACKEND"] = args.backend

    command = create_command(args)

    subprocess.run(command)


if __name__ == "__main__":
    main()
