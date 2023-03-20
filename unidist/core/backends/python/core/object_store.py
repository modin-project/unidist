# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Object storage related functionality."""

from unidist.core.backends.common.data_id import DataID


class ObjectStore:
    """Class that stores objects and provides access to these."""

    __instance = None

    def __init__(self):
        if ObjectStore.__instance is None:
            self.store = dict()

    def __repr__(self):
        return f"Object store: {self.store}"

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``ObjectStore``.

        Returns
        -------
        unidist.core.backends.python.core.object_store.ObjectStore
        """
        if cls.__instance is None:
            cls.__instance = ObjectStore()
        return cls.__instance

    def put(self, data, data_id=None):
        """
        Put `data` to internal dictionary.

        Parameters
        ----------
        data : object
            Data to be put.
        data_id : unidist.core.backends.common.data_id.DataID, optional
            An ID of data. If it isn't provided, will be created automatically.

        Returns
        -------
        unidist.core.backends.common.data_id.DataID
            An ID of object in internal dictionary.
        """
        data_id = DataID() if data_id is None else data_id

        self.store[data_id] = data
        return data_id

    def get(self, data_ids):
        """
        Get object(s) associated with `data_ids` from the internal dictionary.

        Parameters
        ----------
        data_ids : unidist.core.backends.common.data_id.DataID or list
            ID(s) of object(s) to get data from.

        Returns
        -------
        object
            A Python object.
        """
        is_list = isinstance(data_ids, list)
        if not is_list:
            data_ids = [data_ids]
        if not all(isinstance(data_id, DataID) for data_id in data_ids):
            raise ValueError(
                "`data_ids` must either be a data ID or a list of data IDs."
            )

        def check_exception(value):
            if isinstance(value, Exception):
                raise value
            return value

        values = [check_exception(self.store[data_id]) for data_id in data_ids]

        return values if is_list else values[0]
