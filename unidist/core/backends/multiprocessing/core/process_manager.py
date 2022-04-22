# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Workers related functionality."""

import cloudpickle as pkl
from multiprocessing import (
    Process,
    JoinableQueue,
)

from unidist.config import CpuCount
from unidist.core.backends.common.data_id import DataID
from unidist.core.backends.multiprocessing.core.object_store import ObjectStore


class Operation:
    EXECUTE = 0
    EXECUTE_ACTOR_METHOD = 1
    CREATE_ACTOR = 2
    REMOVE_ACTOR = 3
    CANCEL = 4


class Worker(Process):
    """
    Class-process that executes tasks from `self.task_queue`.

    Parameters
    ----------
    task_queue : multiprocessing.JoinableQueue
        A queue of task to execute.
    obj_store : unidist.core.backends.multiprocessing.core.object_store.ObjectStore
        Shared object storage to read/write data.
    """

    def __init__(self, task_queue, obj_store):
        Process.__init__(self)
        self.task_queue = task_queue
        self._obj_store = obj_store
        self._actor_handle = None
        self.is_actor = False

    def run(self):
        """Run main infinite loop of process to execute tasks from `self.task_queue`."""
        while 1:
            task = self.task_queue.get()
            task = pkl.loads(task)

            operation = task.operation_type
            func = task.func
            if operation in (Operation.EXECUTE, Operation.EXECUTE_ACTOR_METHOD):
                data_ids = task.data_ids
                args, kwargs = task.get_parameters()
                try:
                    if operation == Operation.EXECUTE:
                        value = func(*args, **kwargs)
                    else:
                        value = getattr(self._actor_handle, func)(*args, **kwargs)
                except Exception as e:
                    if isinstance(data_ids, list) and len(data_ids) > 1:
                        for data_id in data_ids:
                            self._obj_store.store_delayed[data_id] = e
                    else:
                        self._obj_store.store_delayed[data_ids] = e
                else:
                    if data_ids is not None:
                        if isinstance(data_ids, list) and len(data_ids) > 1:
                            for data_id, val in zip(data_ids, value):
                                self._obj_store.store_delayed[data_id] = val
                        else:
                            self._obj_store.store_delayed[data_ids] = value
            elif operation == Operation.CREATE_ACTOR:
                args, kwargs = task.get_parameters()
                self._actor_handle = func(*args, **kwargs)
                self.is_actor = True
            elif operation == Operation.REMOVE_ACTOR:
                self._actor_handle = None
                self.is_actor = False
            elif operation == Operation.CANCEL:
                self.task_queue.task_done()
                break
            else:
                self.task_queue.task_done()
                raise ValueError("Unsupported operation")

            self.task_queue.task_done()

        return

    def add_task(self, task):
        """
        Add `task` to `self.task_queue`.

        Parameters
        ----------
        task : unidist.core.backends.multiprocessing.core.process_manager.Task
            Task to be added in the queue.
        """
        self.task_queue.put(task)


class ProcessManager:
    """
    Class that controls worker pool and assings task to workers.

    Parameters
    ----------
    num_workers : int, optional
        Number of worker-processes to start. If isn't provided,
        will be equal to number of CPUs.

    Notes
    -----
    Constructor starts `num_workers` MultiProcessing Workers.
    """

    __instance = None

    def __init__(self, num_workers=None):
        if ProcessManager.__instance is None:
            if num_workers is None:
                num_workers = CpuCount.get()
            self.workers = [None] * num_workers
            self.is_actors = [False] * num_workers
            self.__class__._worker_id = 0

            obj_store = ObjectStore.get_instance()
            for idx in range(num_workers):
                self.workers[idx] = Worker(JoinableQueue(), obj_store)
                self.workers[idx].start()

            self._is_alive = True

    @classmethod
    def get_instance(cls, num_workers=None):
        """
        Get instance of ``ProcessManager``.

        Returns
        -------
        unidist.core.backends.multiprocessing.core.process_manager.ProcessManager
        """
        if cls.__instance is None:
            cls.__instance = ProcessManager(num_workers=num_workers)
        return cls.__instance

    def _next(self):
        """
        Get current worker index and move to another with incrementing by one.

        Returns
        -------
        int
        """
        idx = self.__class__._worker_id
        self.__class__._worker_id += 1
        if self.__class__._worker_id == len(self.workers):
            self.__class__._worker_id = 0
        return idx

    def create_actor(self, task):
        """
        Grab a worker from worker pool.

        Grabbed worker is marked as `blocked` and doesn't participate
        in the tasks submission.

        Returns
        -------
        unidist.core.backends.multiprocessing.core.process_manager.Worker
            Grabbed worker.
        int
            Index of grabbed worker.
        """
        worker_id = None
        for idx, is_actor in enumerate(self.is_actors):
            if not is_actor:
                worker_id = idx
                self.is_actors[idx] = True
                break
        if worker_id is None:
            raise RuntimeError("Actor can`t be run, no available workers.")

        self.workers[worker_id].add_task(pkl.dumps(task))
        print(f"create actor on: {worker_id}")
        return worker_id

    def remove_actor(self, idx):
        """
        Free worker by index `idx`.

        Parameters
        ----------
        idx : int
            Index of worker to be freed.
        """
        task = Task(None, Operation.REMOVE_ACTOR, None)
        if self._is_alive:
            self.workers[idx].add_task(pkl.dumps(task))
        self.is_actors[idx] = False

    def submit(self, task, target_worker_id=None):
        """
        Add `task` to task queue of one of workers using round-robin.

        Parameters
        ----------
        task : unidist.core.backends.multiprocessing.core.process_manager.Task
            Task to be added in task queue.
        """
        if target_worker_id is None:
            num_skipped = 0

            while num_skipped < len(self.workers):
                idx = self._next()
                if not self.is_actors[idx]:
                    self.workers[idx].add_task(task)
                    return
                else:
                    num_skipped += 1

            raise RuntimeError("Task can`t be run, no available workers.")
        else:
            self.workers[target_worker_id].add_task(task)

    def shutdown(self):
        self._is_alive = False
        task = Task(None, Operation.CANCEL, None)
        [worker.add_task(pkl.dumps(task)) for worker in self.workers]


class Task:
    """
    Class poses as unified callable object to execute in MultiProcessing Worker.

    Parameters
    ----------
    func : callable
        A function to be called in object invocation.
    data_ids : unidist.core.backends.common.data_id.DataID or list
        ``DataID``-(s) associated with result(s) of `func` invocation.
    obj_store : unidist.core.backends.multiprocessing.core.object_store.ObjectStore
        Object storage to share data between workers.
    *args : iterable
        Positional arguments to be passed in the `func`.
    **kwargs : dict
        Keyword arguments to be passed in the `func`.
    """

    def __init__(self, func, operation_type, obj_store, *args, data_ids=None, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._obj_store = obj_store
        self.func = func
        self.operation_type = operation_type
        self.data_ids = data_ids

    def get_parameters(self):
        materialized_args = [
            self._obj_store.get(arg) if isinstance(arg, DataID) else arg
            for arg in self._args
        ]
        materialized_kwargs = {
            key: self._obj_store.get(value) if isinstance(value, DataID) else value
            for key, value in self._kwargs.items()
        }
        return materialized_args, materialized_kwargs
