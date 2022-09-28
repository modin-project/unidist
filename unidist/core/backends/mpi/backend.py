# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``Backend`` interface using MPI."""

import socket

from unidist.config import CpuCount
import unidist.core.backends.mpi.core as mpi
from unidist.core.backends.mpi.actor import MPIActor
from unidist.core.backends.mpi.remote_function import MPIRemoteFunction
from unidist.core.base.backend import Backend

# Static variable for `get_ip` function
host = None


class MPIBackend(Backend):
    """The class that implements the interface in ``Backend`` using MPI."""

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
        MPIRemoteFunction
        """
        return MPIRemoteFunction(function, num_cpus, num_returns, resources)

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
        MPIActor
            The actor class type to create.
        list
            The list of arguments for ``MPIActor`` constructor.
        """
        return MPIActor, [cls, num_cpus, resources]

    @staticmethod
    def get(data_ids):
        """
        Get a remote object or a list of remote objects
        from distributed memory.

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            ``DataID`` or a list of ``DataID`` objects to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.
        """
        return mpi.get(data_ids)

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
        unidist.core.backends.common.data_id.DataID
            ``DataID`` matching to data.
        """
        return mpi.put(data)

    @staticmethod
    def wait(data_ids, num_returns=1):
        """
        Wait until `data_ids` are finished.

        This method returns two lists. The first list consists of
        data IDs that correspond to objects that completed computations.
        The second list corresponds to the rest of the data IDs (which may or may not be ready).

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            ``DataID`` or list of ``DataID``-s to be waited.
        num_returns : int, default: 1
            The number of ``DataID``-s that should be returned as ready.

        Returns
        -------
        tuple
            List of data IDs that are ready and list of the remaining data IDs.
        """
        return mpi.wait(data_ids, num_returns=num_returns)

    @staticmethod
    def get_ip():
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        global host
        if host is None:
            hostname = socket.gethostname()
            host = socket.gethostbyname(hostname)
        return host

    @staticmethod
    def num_cpus():
        """
        Get the number of CPUs used by the execution backend.

        Returns
        -------
        int
        """
        return CpuCount.get()

    @staticmethod
    def cluster_resources():
        """
        Get resources of MPI cluster.

        Returns
        -------
        dict
            Dictionary with cluster nodes info in the form
            `{"node_ip0": {"CPU": x0}, "node_ip1": {"CPU": x1}, ...}`.
        """
        return mpi.cluster_resources()

    @staticmethod
    def shutdown():
        """Shutdown MPI execution backend."""
        mpi.shutdown()

    @staticmethod
    def is_initialized():
        """
        Check if MPI backend has already been initialized.

        Returns
        -------
        bool
            True or False.
        """
        return mpi.is_initialized()
