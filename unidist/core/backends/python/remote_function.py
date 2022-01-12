# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``RemoteFunction`` interface using Python."""

import unidist.core.backends.python.core as py
from unidist.core.base.object_ref import ObjectRef
from unidist.core.base.remote_function import RemoteFunction


class PythonRemoteFunction(RemoteFunction):
    """
    The class that implements the interface in ``RemoteFunction`` using Python.

    Parameters
    ----------
    function : callable
        A function to be called.
    num_cpus : int
        The number of CPUs to reserve for the function.
    num_returns : int
        The number of ``ObjectRef``-s returned by the function invocation.
    resources : dict
        Custom resources to reserve for the function.
    """

    def __init__(self, function, num_cpus, num_returns, resources):
        self._remote_function = function
        self._num_cpus = num_cpus
        self._num_returns = 1 if num_returns is None else num_returns
        self._resources = resources

    def _remote(self, *args, num_cpus=None, num_returns=None, resources=None, **kwargs):
        """
        Execute `self._remote_function`.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the `self._remote_function`.
        num_cpus : int, optional
            The number of CPUs to reserve for the function.
        num_returns : int, optional
            The number of ``ObjectRef``-s returned by the function invocation.
        resources : dict, optional
            Custom resources to reserve for the function.
        **kwargs : dict
            Keyword arguments to be passed in the `self._remote_function`.

        Returns
        -------
        ObjectRef, list or None
            Type of returns depends on `num_returns` value:

            * if `num_returns == 1`, ``ObjectRef`` will be returned.
            * if `num_returns > 1`, list of ``ObjectRef`` will be returned.
            * if `num_returns == 0`, ``None`` will be returned.
        """
        if num_cpus is not None or self._num_cpus is not None:
            raise NotImplementedError("'num_cpus' is not supported by Python backend.")
        if resources is not None or self._resources is not None:
            raise NotImplementedError("'resources' is not supported by Python backend.")

        if num_returns is None:
            num_returns = self._num_returns

        data_ids = py.submit(
            self._remote_function, *args, num_returns=num_returns, **kwargs
        )

        if num_returns == 1:
            return ObjectRef(data_ids)
        elif num_returns > 1:
            return [ObjectRef(data_id) for data_id in data_ids]
        elif num_returns == 0:
            return None
