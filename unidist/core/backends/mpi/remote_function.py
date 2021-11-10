# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""An implementation of ``RemoteFunction`` interface using MPI."""

import unidist.core.backends.mpi.core as mpi
from unidist.core.backends.common.utils import unwrap_object_refs
from unidist.core.base.object_ref import ObjectRef
from unidist.core.base.remote_function import RemoteFunction


class MPIRemoteFunction(RemoteFunction):
    """
    The class that implements the interface in ``RemoteFunction`` using MPI.

    Parameters
    ----------
    function : callable
        A function to be called remotely.
    num_cpus : int
        The number of CPUs to reserve for the remote function.
    num_returns : int
        The number of ``ObjectRef``-s returned by the remote function invocation.
    resources : dict
        Custom resources to reserve for the remote function.
    """

    def __init__(self, function, num_cpus, num_returns, resources):
        self._remote_function = function
        self._num_cpus = num_cpus
        self._num_returns = 1 if num_returns is None else num_returns
        self._resources = resources

    def _remote(self, *args, num_cpus=None, num_returns=None, resources=None, **kwargs):
        """
        Execute `self._remote_function` in a worker process.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the `self._remote_function`.
        num_cpus : int, optional
            The number of CPUs to reserve for the remote function.
        num_returns : int, optional
            The number of ``ObjectRef``-s returned by the remote function invocation.
        resources : dict, optional
            Custom resources to reserve for the remote function.
        **kwargs : dict
            Keyword arguments to be passed in the `self._remote_function`.

        Returns
        -------
        ObjectRef, list or None
            Type of returns depends on `num_returns` value:

            * if `num_returns == 1`, ``ObjectRef`` will be returned.
            * if `num_returns > 1`, list of ``ObjectRef``-s will be returned.
            * if `num_returns == 0`, ``None`` will be returned.
        """
        if num_cpus is not None or self._num_cpus is not None:
            raise NotImplementedError("'num_cpus' is not supported yet by MPI backend.")
        if resources is not None or self._resources is not None:
            raise NotImplementedError(
                "'resources' is not supported yet by MPI backend."
            )

        if num_returns is None:
            num_returns = self._num_returns

        unwrapped_args = [unwrap_object_refs(arg) for arg in args]
        unwrapped_kwargs = {k: unwrap_object_refs(v) for k, v in kwargs.items()}

        data_ids = mpi.remote(
            self._remote_function,
            *unwrapped_args,
            num_returns=num_returns,
            **unwrapped_kwargs
        )

        if num_returns == 1:
            return ObjectRef(data_ids)
        elif num_returns > 1:
            return [ObjectRef(data_id) for data_id in data_ids]
        elif num_returns == 0:
            return None
