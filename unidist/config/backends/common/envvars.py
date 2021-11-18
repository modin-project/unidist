# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Config entities which can be used for unidist common behavior tuning."""

from packaging import version

from unidist.config.parameter import EnvironmentVariable


class Backend(EnvironmentVariable, type=str):
    """Distribution backend to run queries by."""

    varname = "UNIDIST_BACKEND"
    choices = ("Ray", "Dask", "Mpi", "Multiprocessing", "Python")

    @classmethod
    def _get_default(cls):
        """
        Get default value of the config.

        Returns
        -------
        str
        """
        try:
            import ray

        except ImportError:
            pass
        else:
            if version.parse(ray.__version__) < version.parse("1.4.0"):
                raise ImportError(
                    "Please `pip install unidist[ray]` to install compatible Ray version."
                )
            return "Ray"
        try:
            import dask
            import distributed

        except ImportError:
            pass
        else:
            if version.parse(dask.__version__) < version.parse(
                "2.22.0"
            ) or version.parse(distributed.__version__) < version.parse("2.22.0"):
                raise ImportError(
                    "Please `pip install unidist[dask]` to install compatible Dask version."
                )
            return "Dask"
        try:
            import mpi4py
        except ImportError:
            pass
        else:
            if version.parse(mpi4py.__version__) < version.parse("3.0.3"):
                raise ImportError(
                    "Please `pip install unidist[mpi]` to install compatible MPI version."
                )
            return "Mpi"
        return "Multiprocessing"


class CpuCount(EnvironmentVariable, type=int):
    """How many CPU cores to use during initialization of the unidist backend."""

    varname = "UNIDIST_CPUS"

    @classmethod
    def _get_default(cls):
        """
        Get default value of the config.

        Returns
        -------
        int
        """
        import multiprocessing

        return multiprocessing.cpu_count()
