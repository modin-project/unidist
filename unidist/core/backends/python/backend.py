# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``Backend`` interface using Python."""

import socket

import unidist.core.backends.python.core as py
from unidist.core.backends.python.actor import PythonActor
from unidist.core.backends.python.remote_function import (
    PythonRemoteFunction,
)
from unidist.core.base.backend import Backend


class PythonBackend(Backend):
    """The class that implements the interface in ``Backend`` using Python."""

    @staticmethod
    def make_remote_function(function, num_cpus, num_returns, resources):
        """
        Define ``PythonRemoteFunction``.

        function : callable
            Function to be ``PythonRemoteFunction``.
        num_cpus : int
            The number of CPUs to reserve for ``PythonRemoteFunction``.
        num_returns : int
            The number of ``ObjectRef``-s returned by the function invocation.
        resources : dict
            Custom resources to reserve for the function.

        Returns
        -------
        PythonRemoteFunction
        """
        return PythonRemoteFunction(function, num_cpus, num_returns, resources)

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
        PythonActor
            The actor class type to create.
        list
            The list of arguments for ``PythonActor`` constructor.
        """
        return PythonActor, [cls, num_cpus, resources]

    @staticmethod
    def get(data_ids):
        """
        Get an object or a list of objects from ``DataID``-(s).

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            ``DataID`` or a list of ``DataID`` objects to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.
        """
        return py.unwrap(data_ids)

    @staticmethod
    def put(data):
        """
        Put `data` into ``DataID``.

        Parameters
        ----------
        data : object
            Data to be put.

        Returns
        -------
        unidist.core.backends.common.data_id.DataID
            ``DataID`` matching to data.
        """
        return py.wrap(data)

    @staticmethod
    def wait(data_ids, num_returns=1):
        """
        Wait until `data_ids` are finished.

        This method returns two lists. The first list consists of
        data IDs that correspond to objects that completed computations.
        The second list corresponds to the rest of the data IDs.

        Parameters
        ----------
        object_refs : unidist.core.backends.common.data_id.DataID or list
            ``DataID`` or list of ``DataID``-s to be waited.
        num_returns : int, default: 1
            The number of ``DataID``-s that should be returned as ready.

        Returns
        -------
        tuple
            List of data IDs that are ready and list of the remaining data IDs.

        Notes
        -----
        Method serves to maintain behavior compatibility between backends. All objects
        completed computation before putting into an object storage for Python backend.
        """
        return data_ids[:num_returns], data_ids[num_returns:]

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
        from multiprocessing import cpu_count

        return cpu_count()
