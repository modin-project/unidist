# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Actor specific functionality using Ray backend."""

import ray

from unidist.core.base.actor import Actor, ActorMethod
from unidist.core.base.object_ref import ObjectRef


class RayActorMethod(ActorMethod):
    """
    The class implements the interface in ``ActorMethod`` using Ray backend.

    Parameters
    ----------
    cls : ray.actor.ActorHandle
        An actor class from which method `method_name` will be called.
    method_name : str
        The name of the method to be called.
    """

    def __init__(self, cls, method_name):
        self._cls = cls
        self._method_name = method_name
        self._num_returns = 1

    def _remote(self, *args, num_returns=None, **kwargs):
        """
        Execute `self._method_name` in a worker process.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the method.
        num_returns : int, optional
            Number of results to be returned. If it isn't
            provided, `self._num_returns` will be used.
        **kwargs : dict
            Keyword arguments to be passed in the method.

        Returns
        -------
        ObjectRef, list or None
            Type of returns depends on `num_returns` value:

            * if `num_returns == 1`, ``ObjectRef`` will be returned.
            * if `num_returns > 1`, list of ``ObjectRef`` will be returned.
            * if `num_returns == 0`, ``None`` will be returned.
        """
        if num_returns is None:
            num_returns = self._num_returns

        class_method = getattr(self._cls, self._method_name)
        obj_ref = class_method.options(num_returns=num_returns).remote(*args, **kwargs)

        if num_returns == 1:
            return ObjectRef(obj_ref)
        elif num_returns > 1:
            return [ObjectRef(ref) for ref in obj_ref]
        elif num_returns == 0:
            return None


class RayActor(Actor):
    """
    The class implements the interface in ``Actor`` using Ray backend.

    Parameters
    ----------
    cls : object
        Class to be an actor class.
    num_cpus : int
        The number of CPUs to reserve for the lifetime of the actor.
    resources : dict
        Custom resources to reserve for the lifetime of the actor.
    actor_handle : None or ray.actor.ActorHandle
        Used for proper serialization/deserialization via `__reduce__`.
    """

    def __init__(self, cls, num_cpus, resources, actor_handle=None):
        self._cls = cls
        self._num_cpus = num_cpus
        self._resources = resources
        self._actor_handle = actor_handle

    def _serialization_helper(self):
        """
        Helper to save the state of the object.

        This is defined to make pickling work via `__reduce__`.

        Returns
        -------
        dict
            A dictionary of the information needed to reconstruct the object.
        """
        state = {
            "cls": self._cls,
            "num_cpus": self._num_cpus,
            "resources": self._resources,
            "actor_handle": self._actor_handle,
        }
        return state

    @classmethod
    def _deserialization_helper(cls, state):
        """
        Helper to restore the state of the object.

        This is defined to make pickling work via `__reduce__`.

        Parameters
        ----------
        state : dict
            The serialized state of the object.

        Returns
        -------
        RayActor
        """
        return cls(
            state["cls"],
            state["num_cpus"],
            state["resources"],
            state["actor_handle"],
        )

    def __reduce__(self):
        """
        This is defined intentionally to make pickling work correctly.

        Returns
        -------
        tuple
            Callable and arguments to be passed in to it.
        """
        state = self._serialization_helper()
        return self._deserialization_helper, (state,)

    def __getattr__(self, name):
        """
        Get the attribute `name` of the `self._cls` class.

        This methods creates the ``RayActorMethod`` object that is responsible
        for calling method `name` of the `self._cls` class remotely.

        Parameters
        ----------
        name : str
            Name of the method to be called remotely.

        Returns
        -------
        RayActorMethod
        """
        return RayActorMethod(self._actor_handle, name)

    def _remote(self, *args, num_cpus=None, resources=None, **kwargs):
        """
        Create actor class, specific for Ray backend, from `self._cls`.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in `self._cls` class constructor.
        num_cpus : int, optional
            The number of CPUs to reserve for the lifetime of the actor.
        resources : dict, optional
            Custom resources to reserve for the lifetime of the actor.
        **kwargs : dict
            Keyword arguments to be passed in `self._cls` class constructor.

        Returns
        -------
        RayActor
        """
        if num_cpus is None:
            num_cpus = self._num_cpus
        if resources is None:
            resources = self._resources
        self._actor_handle = (
            ray.remote(self._cls)
            .options(num_cpus=num_cpus, resources=resources)
            .remote(*args, **kwargs)
        )
        return self
