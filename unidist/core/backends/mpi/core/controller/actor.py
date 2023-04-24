# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Actor specific functionality using MPI backend."""

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.controller.object_store import object_store
from unidist.core.backends.mpi.core.controller.garbage_collector import (
    garbage_collector,
)
from unidist.core.backends.mpi.core.controller.common import push_data, Scheduler


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
        async_operations = AsyncOperations.get_instance()
        h_list, _ = communication.isend_complex_operation(
            communication.MPIState.get_instance().comm,
            operation_type,
            operation_data,
            self._actor._owner_rank,
        )
        async_operations.extend(h_list)
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
    owner_rank : None or int
        MPI rank which an actor class should be created on.
        Used for proper serialization/deserialization via `__reduce__`.
    handler_id : None or unidist.core.backends.mpi.core.common.MasterDataID
        An ID to the actor class.
        Used for proper serialization/deserialization via `__reduce__`.
    **kwargs : dict
        Keyword arguments to be passed in `cls` constructor.

    Notes
    -----
    Instance of the `cls` will be created on the worker.
    """

    def __init__(self, cls, *args, owner_rank=None, handler_id=None, **kwargs):
        self._cls = cls
        self._args = args
        self._kwargs = kwargs
        self._owner_rank = (
            Scheduler.get_instance().schedule_rank()
            if owner_rank is None
            else owner_rank
        )
        self._handler_id = (
            object_store.generate_data_id(garbage_collector)
            if handler_id is None
            else handler_id
        )
        object_store.put_data_owner(self._handler_id, self._owner_rank)

        # reserve a rank for actor execution only
        Scheduler.get_instance().reserve_rank(self._owner_rank)

        # submit `ACTOR_CREATE` task to a worker only once
        if owner_rank is None and handler_id is None:
            operation_type = common.Operation.ACTOR_CREATE
            operation_data = {
                "class": cls,
                "args": args,
                "kwargs": kwargs,
                "handler": self._handler_id.base_data_id(),
            }
            async_operations = AsyncOperations.get_instance()
            h_list, _ = communication.isend_complex_operation(
                communication.MPIState.get_instance().comm,
                operation_type,
                operation_data,
                self._owner_rank,
            )
            async_operations.extend(h_list)

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
            "args": self._args,
            "owner_rank": self._owner_rank,
            "handler_id": self._handler_id,
            "kwargs": self._kwargs,
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
        Actor
        """
        return cls(
            state["cls"],
            *state["args"],
            owner_rank=state["owner_rank"],
            handler_id=state["handler_id"],
            **state["kwargs"],
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
        return ActorMethod(self, name)

    def __del__(self):
        """
        This is defined to release the rank reserved for the actor when it gets out of scope.
        """
        Scheduler.get_instance().release_rank(self._owner_rank)
