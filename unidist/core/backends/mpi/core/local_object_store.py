# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`LocalObjectStore` functionality."""

import weakref

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication


class LocalObjectStore:
    """
    Class that stores local objects and provides access to them.

    Notes
    -----
    The storage is local to the current worker process only.
    """

    __instance = None

    def __init__(self):
        # Add local data {DataID : Data}
        self._data_map = weakref.WeakKeyDictionary()
        # "strong" references to data IDs {DataID : DataID}
        # we are using dict here to improve performance when getting an element from it,
        # whereas other containers would require O(n) complexity
        self._data_id_map = {}
        # Data owner {DataID : Rank}
        self._data_owner_map = weakref.WeakKeyDictionary()
        # Data was already sent to this ranks {DataID : [ranks]}
        self._sent_data_map = weakref.WeakKeyDictionary()
        # Data id generator
        self._data_id_counter = 0
        # Data serialized cache
        self._serialization_cache = weakref.WeakKeyDictionary()
        # keep finalizers to clean deleted MasterId in _data_id_map
        self.master_id_finalizers = []

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``LocalObjectStore``.

        Returns
        -------
        LocalObjectStore
        """
        if cls.__instance is None:
            cls.__instance = LocalObjectStore()
        return cls.__instance

    def maybe_update_data_id_map(self, data_id):
        """
        Add a strong reference to the `data_id` if necessary.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.

        Notes
        -----
        The worker must have a strong reference to the external `data_id` until the owner process
        send the `unidist.core.backends.common.Operation.CLEANUP` operation with this `data_id`.
        """
        if (
            data_id.owner_rank != communication.MPIState.get_instance().global_rank
            and data_id not in self._data_id_map
        ):
            self._data_id_map[data_id] = data_id

    def put(self, data_id, data):
        """
        Put `data` to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        data : object
            Data to be put.
        """
        self._data_map[data_id] = data
        self.maybe_update_data_id_map(data_id)

    def put_data_owner(self, data_id, rank):
        """
        Put data location (owner rank) to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        rank : int
            Rank number where the data resides.
        """
        self._data_owner_map[data_id] = rank
        self.maybe_update_data_id_map(data_id)

    def get(self, data_id):
        """
        Get the data from a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.

        Returns
        -------
        bool
            Return the ``True`` status if an object location is known.
        """
        return data_id in self._data_owner_map

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

    def generate_data_id(self, gc):
        """
        Generate unique ``MpiDataID`` instance.

        Parameters
        ----------
        gc : unidist.core.backends.mpi.core.executor.GarbageCollector
            Local garbage collector reference.

        Returns
        -------
        unidist.core.backends.mpi.core.common.MpiDataID
            Unique data ID instance.
        """
        data_id = common.MpiDataID(
            communication.MPIState.get_instance().global_rank, self._data_id_counter, gc
        )
        self._data_id_counter += 1
        return data_id

    def generate_output_data_id(self, dest_rank, gc, num_returns=1):
        """
        Generate unique list of ``unidist.core.backends.mpi.core.common.MpiDataID`` instance.

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
            A list of unique ``MpiDataID`` instances.
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ``ID`` to data.
        rank : int
            Rank number where the data was sent.
        """
        if data_id in self._sent_data_map:
            self._sent_data_map[data_id].add(rank)
        else:
            self._sent_data_map[data_id] = set([rank])

    def is_already_sent(self, data_id, rank):
        """
        Check if communication event on this `data_id` and rank happened.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        data : object
            Serialized data to cache.
        """
        self._serialization_cache[data_id] = data
        self.maybe_update_data_id_map(data_id)

    def is_already_serialized(self, data_id):
        """
        Check if the data on this `data_id` is already serialized.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.

        Returns
        -------
        object
            Cached serialized data associated with `data_id`.
        """
        return self._serialization_cache[data_id]
