# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Actor specific functionality implemented using Python multiprocessing."""

import cloudpickle as pkl
from multiprocessing.managers import BaseManager

from unidist.core.backends.multiprocessing.core.object_store import ObjectStore, Delayed
from unidist.core.backends.multiprocessing.core.process_manager import (
    ProcessManager,
    Task,
)


class ActorMethod:
    """
    Class is responsible to execute `method_name` of
    `cls_obj` in the separate worker-process of `actor` object.

    Parameters
    ----------
    cls_obj : multiprocessing.managers.BaseManager
        Shared manager-class.
    actor : Actor
        Actor object.
    method_name : str
        The name of the method to be called.
    obj_store : unidist.core.backends.multiprocessing.core.object_store.ObjectStore
        Object storage to share data between workers.
    """

    def __init__(self, cls_obj, actor, method_name, obj_store):
        self._cls_obj = cls_obj
        self._method_name = method_name
        self._actor = actor
        self._obj_store = obj_store

    def submit(self, *args, num_returns=1, **kwargs):
        """
        Execute `self._method_name` asynchronously in the worker of `self._actor`.

        Parameters
        ----------
        *args : iterable
            Positional arguments to be passed in the `self._method_name` method.
        num_returns : int, default: 1
            Number of results to be returned from `self._method_name`.
        **kwargs : dict
            Keyword arguments to be passed in the `self._method_name` method.

        Returns
        -------
        unidist.core.backends.common.data_id.DataID, list or None
            Type of returns depends on `num_returns` value:

            * if `num_returns == 1`, ``DataID`` will be returned.
            * if `num_returns > 1`, list of ``DataID``-s will be returned.
            * if `num_returns == 0`, ``None`` will be returned.
        """
        if num_returns == 0:
            data_ids = None
        elif num_returns > 1:
            data_ids = [self._obj_store.put(Delayed()) for _ in range(num_returns)]
        else:
            data_ids = self._obj_store.put(Delayed())

        cls_method = getattr(self._cls_obj, self._method_name)

        task = Task(cls_method, data_ids, self._obj_store, *args, **kwargs)
        self._actor.submit(task)

        return data_ids


class Actor:
    """
    Actor-class to execute methods of wrapped class in a separate worker.

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
    Python multiprocessing manager-class will be created to wrap `cls`.
    This makes `cls` class object shared between different workers. Manager-class
    starts additional process to share class state between processes.

    Methods of `cls` class object are executed in the worker, grabbed from a workers pool.
    """

    def __init__(self, cls, *args, **kwargs):
        self._worker = None
        self._worker_id = None
        self._obj_store = ObjectStore.get_instance()

        # FIXME : Change "WrappedClass" -> cls.__name__ + "Manager", for example.
        BaseManager.register("WrappedClass", cls)
        manager = BaseManager()
        manager.start()

        self._cls_obj = manager.WrappedClass(*args, **kwargs)
        self._worker, self._worker_id = ProcessManager.get_instance().grab_worker()

    def __getattr__(self, name):
        """
        Get the attribute `name` of the `self._cls_obj` class.

        This methods creates the ``ActorMethod`` object that is responsible
        for calling method `name` of the `self._cls_obj` class remotely.

        Parameters
        ----------
        name : str
            Name of the method to be called remotely.

        Returns
        -------
        ActorMethod
        """
        return ActorMethod(self._cls_obj, self, name, self._obj_store)

    def submit(self, task):
        """
        Execute `task` asynchronously in the worker grabbed by this actor.

        Parameters
        ----------
        task : unidist.core.backends.multiprocessing.core.process_manager.Task
            Task object holding callable function.
        """
        self._worker.add_task(pkl.dumps(task))

    def __del__(self):
        """
        Destructor of the actor.

        Free worker, grabbed from the workers pool.
        """
        if self._worker_id is not None:
            ProcessManager.get_instance().free_worker(self._worker_id)
