# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Core base backend specific functionality."""

from abc import ABC

from .actor import ActorClass
from .object_ref import ObjectRef
from .remote_function import RemoteFunction


class Backend(ABC):
    """An interface that represents the parent class for any backend class."""

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
        RemoteFunction

        Notes
        -----
        The method of the child class for the concrete backend should return
        remote function object specific for the backend.
        """
        pass

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
        ActorClass

        Notes
        -----
        The method of the child class for the concrete backend should return
        actor class specific for the backend and arguments to be passed in the actor.
        """
        pass

    @staticmethod
    def get(object_refs):
        """
        Get a remote object or a list of remote objects
        from distributed memory.

        Parameters
        ----------
        object_refs : ObjectRef or list
            Object ref or a list of object refs to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.

        Notes
        -----
        The method of the child class for the concrete backend should pass
        `object_refs` specific for the backend.
        """
        pass

    @staticmethod
    def put(data):
        """
        Put data into distributed memory.

        Parameters
        ----------
        data : object
            Data to be put.

        Returns
        -------
        ObjectRef
            ObjectRef matching to data.

        Notes
        -----
        The method of the child class for the concrete backend should return
        object ref specific for the backend.
        """
        pass

    @staticmethod
    def wait(object_refs, num_returns=1):
        """
        Wait until `object_refs` are finished.

        This method returns two lists. The first list consists of
        object ref-s that correspond to objects that completed computations.
        The second list corresponds to the rest of the object ref-s (which may or may not be ready).

        Parameters
        ----------
        object_refs : ObjectRef or list
            Object ref or list of object refs to be waited.
        num_returns : int, default: 1
            The number of object refs that should be returned as ready.

        Returns
        -------
        two lists
            List of object refs that are ready and list of the remaining object refs.

        Notes
        -----
        The method of the child class for the concrete backend should pass and return
        object ref-s specific for the backend.
        """
        pass

    @staticmethod
    def get_ip():
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        pass

    @staticmethod
    def num_cpus():
        """
        Get the number of CPUs used by the execution backend.

        Returns
        -------
        int
        """
        pass

    @staticmethod
    def shutdown():
        """Shutdown an execution backend."""
        pass

    @staticmethod
    def cluster_resources():
        """
        Get resources of the cluster.

        Returns
        -------
        dict
            Dictionary with cluster nodes info in the style '{node_ip: {CPU: xx, ...}, ..}'.
        """
        pass


class BackendProxy(Backend):
    """
    A class which instance is a proxy object to dispatch operations to the concrete backend.

    Parameters
    ----------
    backend_cls : Backend
        Instance of the concrete backend class.
    """

    __instance = None

    def __init__(self, backend_cls):
        if self.__instance is None:
            self._backend_cls = backend_cls

    @classmethod
    def get_instance(cls, backend_cls=None):
        """
        Get instance of this class.

        Parameters
        ----------
        backend_cls : Backend, optional
            Instance of the concrete backend class.
        """
        if cls.__instance is None and backend_cls is not None:
            cls.__instance = BackendProxy(backend_cls)
        return cls.__instance

    def make_remote_function(self, function, num_cpus, num_returns, resources):
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
        RemoteFunction
        """
        remote_function_cls = self._backend_cls.make_remote_function(
            function, num_cpus, num_returns, resources
        )
        return RemoteFunction(remote_function_cls, function)

    def make_actor(self, cls, num_cpus, resources):
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
        ActorClass
        """
        actor_cls, actor_cls_args = self._backend_cls.make_actor(
            cls, num_cpus, resources
        )
        return ActorClass(actor_cls, *actor_cls_args)

    def get(self, object_refs):
        """
        Get a remote object or a list of remote objects
        from distributed memory.

        Parameters
        ----------
        object_refs : ObjectRef or list
            ``ObjectRef`` or a list of ``ObjectRef``-s to get data from.

        Returns
        -------
        object
            A Python object or a list of Python objects.
        """
        if isinstance(object_refs, ObjectRef):
            object_refs = object_refs._ref
        else:
            object_refs = [
                obj_ref._ref if isinstance(obj_ref, ObjectRef) else obj_ref
                for obj_ref in object_refs
            ]
        return self._backend_cls.get(object_refs)

    def put(self, data):
        """
        Put data into distributed memory.

        Parameters
        ----------
        data : object
            Data to be put.

        Returns
        -------
        ObjectRef
            ObjectRef matching to data.
        """
        ref = self._backend_cls.put(data)
        return ObjectRef(ref)

    def wait(self, object_refs, num_returns=1):
        """
        Wait until `object_refs` are finished.

        This method returns two lists. The first list consists of
        ``ObjectRef``-s that correspond to objects that completed computations.
        The second list corresponds to the rest of the ``ObjectRef``-s (which may or may not be ready).

        Parameters
        ----------
        object_refs : ObjectRef or list
            ``ObjectRef`` or list of ``ObjectRef``-s to be waited.
        num_returns : int, default: 1
            The number of ``ObjectRef``-s that should be returned as ready.

        Returns
        -------
        two lists
            List of ``ObjectRef``-s that are ready and list of the remaining ``ObjectRef``-s.
        """
        if isinstance(object_refs, ObjectRef):
            object_refs = [object_refs._ref]
        else:
            object_refs = [
                obj_ref._ref if isinstance(obj_ref, ObjectRef) else obj_ref
                for obj_ref in object_refs
            ]
        ready, not_ready = self._backend_cls.wait(object_refs, num_returns=num_returns)
        ready = [ObjectRef(r) for r in ready]
        not_ready = [ObjectRef(nr) for nr in not_ready]
        return ready, not_ready

    @staticmethod
    def is_object_ref(obj):
        """
        Whether an object is ``ObjectRef`` or not.

        Parameters
        ----------
        obj : object
            An object to be checked.

        Returns
        -------
        bool
            `True` if an object is ``ObjectRef``, `False` otherwise.
        """
        return isinstance(obj, ObjectRef)

    def get_ip(self):
        """
        Get node IP address.

        Returns
        -------
        str
            Node IP address.
        """
        return self._backend_cls.get_ip()

    def num_cpus(self):
        """
        Get the number of CPUs used by the execution backend.

        Returns
        -------
        int
        """
        return self._backend_cls.num_cpus()

    def shutdown(self):
        """Shutdown an execution backend."""
        self._backend_cls.shutdown()

    def cluster_resources(self):
        """
        Get resources of the cluster.

        Returns
        -------
        dict
            Dictionary with cluster nodes info in the style '{node_ip0: {CPU: x0},
            node_ip1: {CPU: x1}, ..}'.
        """
        return self._backend_cls.cluster_resources()
