# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``Backend`` interface using Python Multiprocessing backend."""

import socket

from unidist.config import CpuCount
import unidist.core.backends.pymp.core as mp
from unidist.core.backends.pymp.actor import PyMpActor
from unidist.core.backends.pymp.remote_function import (
    PyMpRemoteFunction,
)
from unidist.core.base.backend import Backend


class PyMpBackend(Backend):
    """The class that implements the interface in ``Backend`` using Python Multiprocessing backend."""

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
        PyMpRemoteFunction
        """
        return PyMpRemoteFunction(function, num_cpus, num_returns, resources)

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
        PyMpActor
            The actor class type to create.
        list
            The list of arguments for ``PyMpActor`` constructor.
        """
        return PyMpActor, [cls, num_cpus, resources]

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
        return mp.get(data_ids)

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
        return mp.put(data)

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
        return mp.wait(data_ids, num_returns=num_returns)

    @staticmethod
    def get_ip():
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)

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
        Get resources of Multiprocessing cluster.

        Returns
        -------
        dict
            Dictionary with node info in the form `{"node_ip": {"CPU": x}}`.
        """
        return {PyMpBackend.get_ip(): {"CPU": PyMpBackend.num_cpus()}}

    @staticmethod
    def shutdown():
        """
        Shutdown Python Multiprocessing execution backend.

        Note
        ----
        Not supported yet.
        """
        raise NotImplementedError(
            "'shutdown' is not supported yet by Python Multiprocessing backend."
        )

    @staticmethod
    def is_initialized():
        """
        Check if Python Multiprocessing backend has already been initialized.

        Returns
        -------
        bool
            True or False.
        """
        return mp.is_initialized()
