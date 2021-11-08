# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Core base actor specific functionality."""

from .common import filter_arguments


class ActorMethod:
    """
    A class that is base for any actor method class specific for the backend.

    This wraps an instance of the child class, which in turn wraps
    an actor method that is meant to be remote, specific for the backend.

    Parameters
    ----------
    actor_method_cls : ActorMethod
        An instance of the ``ActorMethod`` child class.
    """

    def __init__(self, actor_method_cls):
        self._actor_method_cls = actor_method_cls

    def options(self, *args, num_returns=None, **kwargs):
        """
        Override the actor method invocation parameters.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be overrided.
        num_returns : int, optional
            The number of object refs returned by the remote function invocation.
        **kwargs : dict
            Keyword arguments to be overrided.

        Returns
        -------
        FuncWrapper
            An instance of wrapped method that a non-underscore .remote() can be called on.
        """
        actor_method_cls = self._actor_method_cls

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                args, kwargs = filter_arguments(*args, **kwargs)

                return actor_method_cls._remote(
                    *args, num_returns=num_returns, **kwargs
                )

        return FuncWrapper()

    def remote(self, *args, **kwargs):
        """
        Call an actor method in a worker process.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the actor method.
        **kwargs : dict
            Keyword arguments to be passed in the actor method.

        Returns
        -------
        ObjectRef, list or None
        """
        args, kwargs = filter_arguments(*args, **kwargs)

        return self._actor_method_cls._remote(*args, **kwargs)


class Actor:
    """
    A class that is base for any actor class specific for the backend.

    This wraps an instance of the child class, which in turn wraps
    an actor that is meant to be remote, specific for the backend.

    Parameters
    ----------
    actor_cls : Actor
        An instance of the ``Actor`` child class.
    """

    def __init__(self, actor_cls):
        self._actor_cls = actor_cls

    def __getattr__(self, name):
        """
        Get the attribute `name` of the actor class.

        This methods creates the ``ActorMethod`` object whose child
        is responsible for calling method `name` of the actor class remotely.

        Parameters
        ----------
        name : str
            Name of the method to be called remotely.

        Returns
        -------
        ActorMethod
        """
        actor_method_cls = getattr(self._actor_cls, name)
        return ActorMethod(actor_method_cls)


class ActorClass:
    """
    A class that serves as a actor class decorator.

    This wraps an instance of the actor class, which in turn wraps
    an actor that is meant to be remote, specific for the backend.

    Parameters
    ----------
    actor_cls : Actor
        An instance of the ``Actor`` child class.
    """

    def __init__(self, actor_cls, *actor_cls_args):
        self._actor_cls = actor_cls
        self._actor_cls_args = actor_cls_args

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Actors cannot be instantiated directly. "
            f"Instead of '{self._actor_cls_args[0].__name__}()', "
            f"use '{self._actor_cls_args[0].__name__}.remote()'."
        )

    def options(self, *args, num_cpus=None, resources=None, **kwargs):
        """
        Override the actor instantiation parameters.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be overrided.
        num_cpus : int, optional
            The number of CPUs to reserve for the lifetime of the actor.
        resources : dict, optional
            Custom resources to reserve for the lifetime of the actor.
        **kwargs : dict
            Keyword arguments to be overrided.

        Returns
        -------
        ActorWrapper
            An instance of wrapped class that a non-underscore .remote() can be called on.
        """
        actor_cls = self._actor_cls
        actor_cls_args = self._actor_cls_args

        class ActorWrapper:
            def remote(self, *args, **kwargs):
                return Actor(
                    actor_cls(*actor_cls_args)._remote(
                        *args,
                        num_cpus=num_cpus,
                        resources=resources,
                        **kwargs,
                    )
                )

        return ActorWrapper()

    def remote(self, *args, **kwargs):
        """
        Instantiate an actor class.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the class constructor
            that is meant to be actor.
        **kwargs : dict
            Keyword arguments to be passed in the class constructor
            that is meant to be actor.

        Returns
        -------
        Actor
        """
        actor_cls = self._actor_cls(*self._actor_cls_args)._remote(*args, **kwargs)
        return Actor(actor_cls)
