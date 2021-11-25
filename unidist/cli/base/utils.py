# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities for backend runners."""

import os
import ipaddress
import socket
import multiprocessing as mp

from unidist.core.base.common import BackendName


class Defaults:
    """Default values for supported CLI parameters."""

    BACKEND = BackendName.RAY
    NUM_CPUS = "default"
    HOSTS = "localhost"
    REDIS_PASSWORD = ""


def get_unidist_root():
    """
    Get the project root directory.

    Returns
    -------
    str
        Absolute path to the project root directory.
    """
    unidist_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    os.environ["PYTHONPATH"] = (
        unidist_root + os.pathsep + os.environ.get("PYTHONPATH", "")
    )

    return unidist_root


def validate_hosts(hosts):
    """
    Validate `hosts` IP address(es) on correctness and duplicates.

    Parameters
    ----------
    hosts : list or str
        IP addresses to be validated.

    Returns
    -------
    list
        List of validated IP addresses.
    """
    if not isinstance(hosts, list):
        hosts = [hosts]

    ips = [
        str(ipaddress.ip_address(get_localhost_ip() if ip == "localhost" else ip))
        for ip in hosts
    ]
    duplicate_ips = [ip for ip in set(ips) if ips.count(ip) > 1]
    if len(duplicate_ips):
        raise RuntimeError(f"'hosts' list contains duplicates {duplicate_ips}")
    return ips


def validate_num_cpus(num_cpus):
    """
    Validate `num_cpus` on correctness.

    Each value of `num_cpus` is checked on possibility
    of converting to int. In case `num_cpus` is ``default`` it will
    be equal to number of CPUs on ``localhost`` node.

    Parameters
    ----------
    num_cpus : list or "default"
        List of string values. Each value represents
        number of CPUs to be used by corresponding host.

    Returns
    -------
    list
        List of validated number of CPUs per host.
    """
    if not isinstance(num_cpus, list):
        num_cpus = [num_cpus]

    def validate(value):
        try:
            value = int(value)
            if value < 1:
                raise RuntimeError(f"'num_cpus' must be more than 0, got '{num_cpus}'")
        except ValueError:
            if value == "default":
                return mp.cpu_count()
            else:
                raise TypeError(
                    f"'num_cpus' must be integer or sequence of integers, got '{num_cpus}'"
                )
        else:
            return value

    return [str(validate(n)) for n in num_cpus]


def get_localhost_ip():
    """
    Get a public IP-address of the head node.

    Returns
    -------
    str
        Public IP-address of the head node.
    """
    return socket.gethostbyname(socket.gethostname())
