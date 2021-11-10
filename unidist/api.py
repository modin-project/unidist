# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""High-level API and utilities to implement it."""

import inspect


from unidist.core.base.utils import init_backend, get_backend_proxy

a = 0

# --------------------------------------------------------------------------------------
# unidist decorator internal layer
# --------------------------------------------------------------------------------------
def make_decorator(num_cpus=None, num_returns=None, resources=None):
    """
    Make decorator for a user-provided function or a class.

    Parameters
    ----------
    num_cpus : int, optional
        The number of CPUs to reserve for the remote function or for the lifetime of the actor.
    num_returns : int, optional
        The number of ``ObjectRef``-s returned by the remote function invocation.
    resources : dict, optional
        Custom resources to reserve for the remote function or for the lifetime of the actor.

    Returns
    -------
    callable
        Function-decorator.
    """
    execution_backend = get_backend_proxy()

    def decorator(function_or_class):
        """
        Decorate a user-provided function or a class.

        Parameters
        ----------
        function_or_class : object
            Function or class to be decorated.

        Returns
        -------
        RemoteFunction or ActorClass
            Decorated function or decorated class.
        """
        if inspect.isfunction(function_or_class):
            if num_returns is not None and (
                not isinstance(num_returns, int) or num_returns < 0
            ):
                raise ValueError(
                    "The keyword 'num_returns' only accepts 0 or a positive integer"
                )

            return execution_backend.make_remote_function(
                function_or_class,
                num_cpus=num_cpus,
                num_returns=num_returns,
                resources=resources,
            )
        elif inspect.isclass(function_or_class):
            if num_returns is not None:
                raise TypeError("The keyword 'num_returns' is not allowed for actors.")

            return execution_backend.make_actor(
                function_or_class, num_cpus=num_cpus, resources=resources
            )
        else:
            raise TypeError(
                "The @unidist.remote decorator must be applied to either a function or to a class."
            )

    return decorator


# --------------------------------------------------------------------------------------
# unidist execution layer public API
# --------------------------------------------------------------------------------------
def init():
    """
    Initialize an execution backend.

    Notes
    -----
    The concrete execution backend is chosen in depend on
    `UNIDIST_BACKEND` environment variable.
    If the variable is not set, Ray backend is used.
    """
    init_backend()


def shutdown():
    """Shutdown an execution backend."""
    execution_backend = get_backend_proxy()
    execution_backend.shutdown()


def remote(*args, **kwargs):
    """
    Define a remote function or an actor class.

    Parameters
    ----------
    *args : iterable
        Positional arguments to be passed in a remote function or an actor class.
    **kwargs : dict
        Keyword arguments to be passed in a remote function or an actor class.

    Returns
    -------
    RemoteFunction or ActorClass
    """
    # This is the case where the decorator is just @unidist.remote.
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return make_decorator()(args[0])

    # Handle keyword arguments
    assert len(args) == 0 and len(kwargs) > 0, "Error"

    num_cpus = kwargs.get("num_cpus")
    num_returns = kwargs.get("num_returns")
    resources = kwargs.get("resources")
    return make_decorator(
        num_cpus=num_cpus, num_returns=num_returns, resources=resources
    )


def get(object_refs):
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
    execution_backend = get_backend_proxy()
    return execution_backend.get(object_refs)


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
        ``ObjectRef`` matching to data.
    """
    execution_backend = get_backend_proxy()
    return execution_backend.put(data)


def wait(object_refs, num_returns=1):
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
    execution_backend = get_backend_proxy()
    return execution_backend.wait(object_refs, num_returns=num_returns)


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
    execution_backend = get_backend_proxy()
    return execution_backend.is_object_ref(obj)


def get_ip():
    """
    Get node IP address.

    Returns
    -------
    str
        Node IP address.
    """
    execution_backend = get_backend_proxy()
    return execution_backend.get_ip()


def num_cpus():
    """
    Get the number of CPUs used by the execution backend.

    Returns
    -------
    int
    """
    execution_backend = get_backend_proxy()
    return execution_backend.num_cpus()
