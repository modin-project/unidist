# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``Backend`` interface using Dask."""

from collections import defaultdict

from distributed.client import get_client, wait
from distributed.utils import get_ip

from unidist.core.backends.dask.actor import DaskActor
from unidist.core.backends.dask.remote_function import DaskRemoteFunction
from unidist.core.base.backend import Backend


class DaskBackend(Backend):
    """The class that implements the interface in ``Backend`` using Dask."""

    @staticmethod
    def make_remote_function(function, num_cpus, num_returns, resources):
        """
        Define a remote function.

        function : callable
            Function to be a remote function.
        num_cpus : int
            The number of CPUs to reserve for the remote function.
        num_returns : int
            The number of ``ObjectRef``-s returned by the remote function invocation.
        resources : dict
            Custom resources to reserve for the remote function.

        Returns
        -------
        DaskRemoteFunction
        """
        return DaskRemoteFunction(function, num_cpus, num_returns, resources)

    @staticmethod
    def make_actor(cls, num_cpus, resources):
        """
        Define an actor class.

        cls : object
            Class to be an actor class.
        num_cpus : int
            The number of CPUs to reserve for the lifetime of the actor.
        resources : dict
            Custom resources to reserve for the lifetime of the actor.

        Returns
        -------
        DaskActor
            The actor class type to create.
        list
            The list of arguments for ``DaskActor`` constructor.
        """
        return DaskActor, [cls, num_cpus, resources]

    @staticmethod
    def get(futures):
        """
        Get a remote object or a list of remote objects
        from distributed memory.

        Parameters
        ----------
        futures : distributed.client.Future or a list of distributed.client.Future-s
            Dask Future or a list of Dask Future objects to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.
        """
        return (
            get_client().gather(futures)
            if isinstance(futures, list)
            else futures.result()
        )

    @staticmethod
    def put(data):
        """
        Put `data` into distributed memory.

        Parameters
        ----------
        data : object
            Data to be put.

        Returns
        -------
        distributed.client.Future
            Dask Future matching to data.
        """
        return get_client().scatter(data, hash=False)

    @staticmethod
    def wait(futures, num_returns=1):
        """
        Wait until `futures` are finished.

        This method returns two lists. The first list consists of
        futures that correspond to objects that completed computations.
        The second list corresponds to the rest of the futures (which may or may not be ready).

        Parameters
        ----------
        futures : distributed.client.Future or list
            Dask Future or list of Dask Futures to be waited.
        num_returns : int, default: 1
            The number of Dask Futures that should be returned as ready.

        Returns
        -------
        tuple
            Two lists of futures that are ready and list of the remaining futures.
        """
        done, not_done = wait(futures, return_when="FIRST_COMPLETED")
        while len(done) < num_returns:
            extra_done, not_done = wait(not_done, return_when="FIRST_COMPLETED")
            done.update(extra_done)

        done, not_done = list(done), list(not_done)
        while len(done) != num_returns:
            last = done.pop()
            not_done.append(last)

        return done, not_done

    @staticmethod
    def get_ip():
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        return get_ip()

    @staticmethod
    def num_cpus():
        """
        Get the number of CPUs used by the execution backend.

        Returns
        -------
        int
        """
        return len(get_client().ncores())

    @staticmethod
    def cluster_resources():
        """
        Get resources of Dask cluster.

        Returns
        -------
        dict
            Dictionary with cluster nodes info in the form
            `{"node_ip0": {"CPU": x0}, "node_ip1": {"CPU": x1}, ...}`.
        """
        client = get_client()
        cluster_resources = defaultdict(lambda: {"CPU": 0})
        for worker_info in client.scheduler_info()["workers"].values():
            cluster_resources[worker_info["host"]]["CPU"] += worker_info["nthreads"]

        localhost = "127.0.0.1"
        if localhost in cluster_resources:
            cluster_resources[get_ip()] = cluster_resources.pop(localhost)
        return dict(cluster_resources)

    @staticmethod
    def shutdown():
        """Shutdown Dask execution backend."""
        client = get_client()
        client.shutdown()
