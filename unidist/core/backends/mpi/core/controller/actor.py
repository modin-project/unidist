# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Actor specific functionality using MPI backend."""

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.controller.object_store import object_store
from unidist.core.backends.mpi.core.controller.garbage_collector import (
    garbage_collector,
)
from unidist.core.backends.mpi.core.controller.common import push_data, RoundRobin


class ActorMethod:
    """
    Class responsible to execute method of an actor.

    Execute `method_name` of `actor` in a separate worker process, where ``Actor`` object is created.

    Parameters
    ----------
    actor : unidist.core.backends.mpi.core.executor.Actor
        Actor object.
    method_name : str
        The name of the method to be called.
    """

    def __init__(self, actor, method_name):
        self._actor = actor
        self._method_name = method_name

    def __call__(self, *args, num_returns=1, **kwargs):
        output_id = object_store.generate_output_data_id(
            self._actor._owner_rank, garbage_collector, num_returns
        )

        unwrapped_args = [common.unwrap_data_ids(arg) for arg in args]
        unwrapped_kwargs = {k: common.unwrap_data_ids(v) for k, v in kwargs.items()}

        push_data(self._actor._owner_rank, unwrapped_args)
        push_data(self._actor._owner_rank, unwrapped_kwargs)

        operation_type = common.Operation.ACTOR_EXECUTE
        operation_data = {
            "task": self._method_name,
            "args": unwrapped_args,
            "kwargs": unwrapped_kwargs,
            "output": common.master_data_ids_to_base(output_id),
            "handler": self._actor._handler_id.base_data_id(),
        }
        communication.send_complex_operation(
            communication.MPIState.get_instance().comm,
            operation_type,
            operation_data,
            self._actor._owner_rank,
        )

        return output_id


class Actor:
    """
    Class to execute methods of a wrapped class in a separate worker process.

    Parameters
    ----------
    cls : object
        Class to be an actor class.
    *args : iterable
        Positional arguments to be passed in `cls` constructor.
    **kwargs : dict
        Keyword arguments to be passed in `cls` constructor.

    Notes
    -----
    Instance of the `cls` will be created on the worker.
    """

    def __init__(self, cls, *args, **kwargs):
        self._owner_rank = RoundRobin.get_instance().schedule_rank()
        self._handler_id = object_store.generate_data_id(garbage_collector)

        operation_type = common.Operation.ACTOR_CREATE
        operation_data = {
            "class": cls,
            "args": args,
            "kwargs": kwargs,
            "handler": self._handler_id.base_data_id(),
        }
        communication.send_complex_operation(
            communication.MPIState.get_instance().comm,
            operation_type,
            operation_data,
            self._owner_rank,
        )

    def __getattr__(self, name):
        return ActorMethod(self, name)
