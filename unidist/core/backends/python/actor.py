# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Actor specific functionality using Python backend."""

import unidist.core.backends.python.core as py
from unidist.core.base.actor import Actor, ActorMethod
from unidist.core.base.object_ref import ObjectRef


class PythonActorMethod(ActorMethod):
    """
    The class implements the interface in ``ActorMethod`` using Python backend.

    Parameters
    ----------
    cls : object
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
        Execute `self._method_name`.

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
            * if `num_returns > 1`, list of ``ObjectRef``-s will be returned.
            * if `num_returns == 0`, ``None`` will be returned.
        """
        if num_returns is None:
            num_returns = self._num_returns

        class_method = getattr(self._cls, self._method_name)
        data_ids = py.submit(class_method, *args, num_returns=num_returns, **kwargs)

        if num_returns == 1:
            return ObjectRef(data_ids)
        elif num_returns > 1:
            return [ObjectRef(data_id) for data_id in data_ids]
        elif num_returns == 0:
            return None


class PythonActor(Actor):
    """
    The class implements the interface in ``Actor`` using Python backend.

    Parameters
    ----------
    cls : object
        Class to be an actor class.
    num_cpus : int
        The number of CPUs to reserve for the lifetime of the actor.
    resources : dict
        Custom resources to reserve for the lifetime of the actor.
    """

    def __init__(self, cls, num_cpus, resources):
        self._cls = cls
        self._num_cpus = num_cpus
        self._resources = resources
        self._actor_handle = None

    def __getattr__(self, name):
        """
        Get the attribute `name` of the `self._cls` class.

        This method creates the ``PythonActorMethod`` object that is responsible
        for calling method `name` of the `self._cls` class.

        Parameters
        ----------
        name : str
            Name of the method to be called.

        Returns
        -------
        PythonActorMethod
        """
        return PythonActorMethod(self._actor_handle, name)

    def _remote(self, *args, num_cpus=None, resources=None, **kwargs):
        """
        Create actor class, specific for Python backend, from `self._cls`.

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
        PythonActor
        """
        if num_cpus is not None or self._num_cpus is not None:
            raise NotImplementedError("'num_cpus' is not supported by Python backend.")
        if resources is not None or self._resources is not None:
            raise NotImplementedError("'resources' is not supported by Python backend.")

        self._actor_handle = self._cls(*args, **kwargs)
        return self
