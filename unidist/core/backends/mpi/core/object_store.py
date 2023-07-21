# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`ObjectStore` functionality."""

import weakref
from collections import defaultdict

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication


class ObjectStore:
    """
    Class that stores local objects and provides access to them.

    Notes
    -----
    Currently, the storage is local to the current worker process only.
    """

    __instance = None

    def __init__(self):
        # Add local data {DataId : Data}
        self._data_map = weakref.WeakKeyDictionary()
        # "strong" references to data IDs {DataId : DataId}
        # we are using dict here to improve performance when getting an element from it,
        # whereas other containers would require O(n) complexity
        self._data_id_map = {}
        # Data owner {DataId : Rank}
        self._data_owner_map = weakref.WeakKeyDictionary()
        # Data was already sent to this ranks {DataID : [ranks]}
        self._sent_data_map = defaultdict(set)
        # Data id generator
        self._data_id_counter = 0
        # Data serialized cache
        self._serialization_cache = weakref.WeakKeyDictionary()

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``ObjectStore``.

        Returns
        -------
        ObjectStore
        """
        if cls.__instance is None:
            cls.__instance = ObjectStore()
        return cls.__instance

    def put(self, data_id, data):
        """
        Put `data` to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        data : object
            Data to be put.
        """
        self._data_map[data_id] = data

    def put_data_owner(self, data_id, rank):
        """
        Put data location (owner rank) to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        rank : int
            Rank number where the data resides.
        """
        self._data_owner_map[data_id] = rank

    def get(self, data_id):
        """
        Get the data from a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        object
            Return local data associated with `data_id`.
        """
        return self._data_map[data_id]

    def get_data_owner(self, data_id):
        """
        Get the data owner rank.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        int
            Rank number where the data resides.
        """
        return self._data_owner_map[data_id]

    def contains(self, data_id):
        """
        Check if the data associated with `data_id` exists in a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            Return the status if an object exist in local dictionary.
        """
        return data_id in self._data_map

    def contains_data_owner(self, data_id):
        """
        Check if the data location info associated with `data_id` exists in a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            Return the ``True`` status if an object location is known.
        """
        return data_id in self._data_owner_map

    def get_unique_data_id(self, data_id):
        """
        Get the "strong" reference to the data ID if it is already stored locally.

        If the passed data ID is not stored locally yet, save and return it.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        unidist.core.backends.common.data_id.DataID
            The unique ID to data.

        Notes
        -----
        We need to use a unique data ID reference for the garbage colleactor to work correctly.
        """
        if communication.MPIState.get_instance().is_root_process():
            # Root process must not have a strong references and user always have actual data_id
            return data_id
        if data_id not in self._data_id_map:
            self._data_id_map[data_id] = data_id
        return self._data_id_map[data_id]

    def clear(self, cleanup_list):
        """
        Clear "strong" references to data IDs from `cleanup_list`.

        Parameters
        ----------
        cleanup_list : list
            List of data IDs.

        Notes
        -----
        The actual data will be collected later when there is no weak or
        strong reference to data in the current worker.
        """
        for data_id in cleanup_list:
            self._data_id_map.pop(data_id, None)
            self._sent_data_map.pop(data_id, None)

    def generate_data_id(self, gc):
        """
        Generate unique ``MasterDataID`` instance.

        Parameters
        ----------
        gc : unidist.core.backends.mpi.core.executor.GarbageCollector
            Local garbage collector reference.

        Returns
        -------
        unidist.core.backends.mpi.core.common.MasterDataID
            Unique data ID instance.
        """
        data_id = f"rank_{communication.MPIState.get_instance().rank}_id_{self._data_id_counter}"
        self._data_id_counter += 1
        return common.MasterDataID(data_id, gc)

    def generate_output_data_id(self, dest_rank, gc, num_returns=1):
        """
        Generate unique list of ``unidist.core.backends.mpi.core.common.MasterDataID`` instance.

        Parameters
        ----------
        dest_rank : int
            Ranks number where generated list will be located.
        gc : unidist.core.backends.mpi.core.executor.GarbageCollector
            Local garbage collector reference.
        num_returns : int, default: 1
            Generated list size.

        Returns
        -------
        list
            A list of unique ``MasterDataID`` instances.
        """
        if num_returns == 1:
            output_ids = self.generate_data_id(gc)
            self.put_data_owner(output_ids, dest_rank)
        elif num_returns == 0:
            output_ids = None
        else:
            output_ids = [None] * num_returns
            for i in range(num_returns):
                output_id = self.generate_data_id(gc)
                output_ids[i] = output_id
                self.put_data_owner(output_id, dest_rank)
        return output_ids

    def cache_send_info(self, data_id, rank):
        """
        Save communication event for this `data_id` and rank.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ``ID`` to data.
        rank : int
            Rank number where the data was sent.
        """
        self._sent_data_map[data_id].add(rank)

    def is_already_sent(self, data_id, rank):
        """
        Check if communication event on this `data_id` and rank happened.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data
        rank : int
            Rank number to check.

        Returns
        -------
        bool
            ``True`` if communication event already happened.
        """
        return (data_id in self._sent_data_map) and (
            rank in self._sent_data_map[data_id]
        )

    def cache_serialized_data(self, data_id, data):
        """
        Save serialized object for this `data_id`.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        data : object
            Serialized data to cache.
        """
        self._serialization_cache[data_id] = data

    def is_already_serialized(self, data_id):
        """
        Check if the data on this `data_id` is already serialized.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        bool
            ``True`` if the data is already serialized.
        """
        return data_id in self._serialization_cache

    def get_serialized_data(self, data_id):
        """
        Get serialized data on this `data_id`.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.

        Returns
        -------
        object
            Cached serialized data associated with `data_id`.
        """
        return self._serialization_cache[data_id]
