# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``Backend`` interface using Ray."""

import ray
from ray.util import get_node_ip_address

from unidist.core.backends.ray.actor import RayActor
from unidist.core.backends.ray.remote_function import RayRemoteFunction
from unidist.core.base.backend import Backend


class RayBackend(Backend):
    """The class that implements the interface in ``Backend`` using Ray."""

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
        RayRemoteFunction
        """
        return RayRemoteFunction(function, num_cpus, num_returns, resources)

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
        RayActor
            The actor class type to create.
        list
            The list of arguments for ``RayActor`` constructor.
        """
        return RayActor, [cls, num_cpus, resources]

    @staticmethod
    def get(object_refs):
        """
        Get a remote object or a list of remote objects
        from distributed memory.

        Parameters
        ----------
        object_refs : ray.ObjectRef or list of ray.ObjectRef-s
            Ray ObjectRef or a list of Ray ObjectRef objects to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.
        """
        return ray.get(object_refs)

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
        ray.ObjectRef
            Ray ObjectRef matching to data.
        """
        return ray.put(data)

    @staticmethod
    def wait(object_refs, num_returns=1):
        """
        Wait until `object_refs` are finished.

        This method returns two lists. The first list consists of
        object refs that correspond to objects that completed computations.
        The second list corresponds to the rest of the object refs (which may or may not be ready).

        Parameters
        ----------
        object_refs : list of ray.ObjectRef-s
            List of Ray ObjectRefs to be waited.
        num_returns : int, default: 1
            The number of Ray ObjectRefs that should be returned as ready.

        Returns
        -------
        tuple
            List of object refs that are ready and list of the remaining object refs.
        """
        return ray.wait(object_refs, num_returns=num_returns)

    @staticmethod
    def get_ip():
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        return get_node_ip_address()

    @staticmethod
    def num_cpus():
        """
        Get the number of CPUs used by the execution backend.

        Returns
        -------
        int
        """
        return int(ray.cluster_resources()["CPU"])

    @staticmethod
    def shutdown():
        """Shutdown Ray execution backend."""
        ray.shutdown()
