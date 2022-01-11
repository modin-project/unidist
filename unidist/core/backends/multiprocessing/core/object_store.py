# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Shared object storage related functionality."""

import cloudpickle as pkl
from multiprocessing import Manager

from unidist.core.backends.common.data_id import DataID


class Delayed:
    """Class-type that used for replacement objects during computation of those."""

    pass


class ObjectStore:
    """
    Class that stores objects and provides access to these from different processes.

    Notes
    -----
    Shared storage is organized using ``multiprocessing.Manager.dict``. This is separate
    process which starts work in the class constructor.
    """

    __instance = None

    def __init__(self):
        if ObjectStore.__instance is None:
            self.store_delayed = Manager().dict()

    def __repr__(self):
        return f"Object store: {self.store_delayed}"

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``ObjectStore``.

        Returns
        -------
        unidist.core.backends.multiprocessing.core.object_store.ObjectStore
        """
        if cls.__instance is None:
            cls.__instance = ObjectStore()
        return cls.__instance

    def put(self, data, data_id=None):
        """
        Put `data` to internal shared dictionary.

        Parameters
        ----------
        data : object
            Data to be put.
        data_id : unidist.core.backends.common.data_id.DataID, optional
            An ID to data. If it isn't provided, will be created automatically.

        Returns
        -------
        unidist.core.backends.common.data_id.DataID
            An ID of object in internal shared dictionary.
        """
        data_id = DataID() if data_id is None else data_id

        self.store_delayed[data_id] = pkl.dumps(data) if callable(data) else data
        return data_id

    def get(self, data_ids):
        """
        Get a object(s) associated with `data_ids` from the shared internal dictionary.

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            An ID(s) of object(s) to get data from.

        Returns
        -------
        object
            A Python object.
        """
        is_list = isinstance(data_ids, list)
        if not is_list:
            data_ids = [data_ids]
        if not all(isinstance(ref, DataID) for ref in data_ids):
            raise ValueError(
                "`data_ids` must either be a data ID or a list of data IDs."
            )

        values = [None] * len(data_ids)

        for idx, data_id in enumerate(data_ids):
            while isinstance(self.store_delayed[data_id], Delayed):
                pass

            value = self.store_delayed[data_id]
            if isinstance(value, Exception):
                raise value

            values[idx] = pkl.loads(value) if isinstance(value, bytes) else value

        return values if is_list else values[0]

    def wait(self, data_ids, num_returns=1):
        """
        Wait until `data_ids` are finished.

        This method returns two lists. The first list consists of
        ``DataID``-s that correspond to objects that completed computations.
        The second list corresponds to the rest of the ``DataID``-s (which may or may not be ready).

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            ``DataID`` or list of ``DataID``-s to be waited.
        num_returns : int, default: 1
            The number of ``DataID``-s that should be returned as ready.

        Returns
        -------
        tuple
            List of data IDs that are ready and list of the remaining data IDs.
        """
        if not isinstance(data_ids, list):
            data_ids = [data_ids]

        ready = list()
        not_ready = list()

        for idx, data_id in enumerate(data_ids[:]):
            if not isinstance(self.store_delayed[data_id], Delayed):
                ready.append(data_ids.pop(idx))

            if len(ready) == num_returns:
                break

        not_ready = data_ids

        while len(ready) != num_returns:
            self.get(not_ready[0])
            ready.append(not_ready.pop(0))

        return ready, not_ready
