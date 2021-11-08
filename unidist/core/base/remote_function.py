# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Core base remote function specific functionality."""

from functools import wraps

from .common import filter_arguments


class RemoteFunction:
    """
    A class that is base for any remote function class specific for the backend.

    This wraps an instance of the child class, which in turn wraps
    a remote function that is meant to be remote, specific for the backend.

    Parameters
    ----------
    remote_function_cls : RemoteFunction
        An instance of the child class.
    function : callable
        A function that is meant to be remote.
    """

    def __init__(self, remote_function_cls, function):
        self._remote_function_cls = remote_function_cls
        self._original_function = function

        # Override task.remote's signature and docstring
        @wraps(self._original_function)
        def _remote_proxy(*args, **kwargs):
            args, kwargs = filter_arguments(*args, **kwargs)

            return self._remote_function_cls._remote(*args, **kwargs)

        self.remote = _remote_proxy

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote functions cannot be called directly. Instead "
            f"of running '{self._original_function.__name__}()', "
            f"try '{self._original_function.__name__}.remote()'."
        )

    def remote(self, *args, **kwargs):
        """
        Call a remote function in a worker process.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the remote function.
        **kwargs : dict
            Keyword arguments to be passed in the remote function.

        Returns
        -------
        ObjectRef, list or None
        """
        return self._remote_function_cls._remote(*args, **kwargs)

    def options(self, *args, num_cpus=None, num_returns=None, resources=None, **kwargs):
        """
        Override the remote function invocation parameters.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be overrided.
        num_cpus : int, optional
            Positional arguments to be overrided.
        num_returns : int, optional
            Positional arguments to be overrided.
        resources : int, optional
            Positional arguments to be overrided.
        **kwargs : dict
            Keyword arguments to be overrided.

        Returns
        -------
        FuncWrapper
            An instance of wrapped function that a non-underscore .remote() can be called on.
        """
        remote_function_cls = self._remote_function_cls

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                args, kwargs = filter_arguments(*args, **kwargs)

                return remote_function_cls._remote(
                    *args,
                    num_cpus=num_cpus,
                    num_returns=num_returns,
                    resources=resources,
                    **kwargs,
                )

        return FuncWrapper()
