# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`ObjectStore` functionality."""

from array import array
import weakref
from collections import defaultdict

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.serialization import ComplexDataSerializer


class ObjectStore:
    """
    Class that stores local objects and provides access to them.

    Notes
    -----
    Currently, the storage is local to the current worker process only.
    """

    __instance = None

    def __init__(self):
        self._shared_buffer = None
        self._helper_win = None

        # Add local data {DataId : Data}
        self._data_map = weakref.WeakKeyDictionary()
        # Shared memory range {DataID: (firstIndex, lastIndex)}
        self._shared_info = weakref.WeakKeyDictionary()
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

    def init_shared_memory(self, comm, size):
        (
            self._shared_buffer,
            itemsize,
            self._helper_win,
        ) = communication.init_shared_memory(comm, size)
        self._helper_buffer = array("L", [0])

    def reserve_shared_memory(self, data):
        serializer = ComplexDataSerializer()
        # Main job
        s_data = serializer.serialize(data)
        # Retrive the metadata
        raw_buffers = serializer.buffers
        buffer_count = serializer.buffer_count
        size = len(s_data) + sum([len(buf) for buf in raw_buffers])

        self._helper_win.Lock(communication.MPIRank.MONITOR)
        self._helper_win.Get(self._helper_buffer, communication.MPIRank.MONITOR)
        firstIndex = self._helper_buffer[0]
        lastIndex = firstIndex + size
        self._helper_buffer[0] = lastIndex
        self._helper_win.Put(array("L", [lastIndex]), communication.MPIRank.MONITOR)
        self._helper_win.Unlock(communication.MPIRank.MONITOR)
        return {"firstIndex": firstIndex, "lastIndex": lastIndex}, {
            "s_data": s_data,
            "raw_buffers": raw_buffers,
            "buffer_count": buffer_count,
        }

    def put(self, data_id, data):
        """
        Put `data` to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data.
        data : object
            Data to be put.
        """
        self._data_map[data_id] = data

    def put_shared_memory(self, data_id, reservation_data, serialized_data):
        if self._shared_buffer is None:
            raise RuntimeError("Shared memory was not initialized")

        first_index = reservation_data["firstIndex"]
        last_index = reservation_data["lastIndex"]

        s_data = serialized_data["s_data"]
        raw_buffers = serialized_data["raw_buffers"]
        buffer_count = serialized_data["buffer_count"]
        s_data_len = len(s_data)

        s_data_first_index = first_index
        s_data_last_index = s_data_first_index + s_data_len

        if s_data_last_index > last_index:
            raise ValueError("Not enough shared space for data")
        self._shared_buffer[s_data_first_index:s_data_last_index] = s_data

        buffer_lens = []
        last_prev_index = s_data_last_index
        for i, raw_buffer in enumerate(raw_buffers):
            raw_buffer_first_index = last_prev_index
            raw_buffer_len = len(raw_buffer)
            raw_buffer_last_index = raw_buffer_first_index + len(raw_buffer)
            if s_data_last_index > last_index:
                raise ValueError(f"Not enough shared space for {i} raw_buffer")

            self._shared_buffer[
                raw_buffer_first_index:raw_buffer_last_index
            ] = raw_buffer

            buffer_lens.append(raw_buffer_len)
            last_prev_index = raw_buffer_last_index

        # save shared memory range for data_id
        return s_data_len, buffer_lens, buffer_count, first_index

    def put_shared_info(self, data_id, shared_info):
        self._shared_info[data_id] = shared_info

    def put_data_owner(self, data_id, rank):
        """
        Put data location (owner rank) to internal dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data.

        Returns
        -------
        object
            Return local data associated with `data_id`.
        """
        return self._data_map[data_id]

    def get_data_shared_info(self, data_id):
        return self._shared_info[data_id]

    def get_shared_data(self, data_id):
        if self._shared_buffer is None:
            raise RuntimeError("Shared memory was not initialized")

        info_package = self._shared_info[data_id]
        first_index = info_package["first_shared_index"]
        buffer_lens = info_package["raw_buffers_lens"]
        buffer_count = info_package["buffer_count"]
        s_data_len = info_package["s_data_len"]

        s_data_last_index = first_index + s_data_len
        s_data = self._shared_buffer[first_index:s_data_last_index].toreadonly()
        prev_last_index = s_data_last_index
        raw_buffers = []
        for raw_buffer_len in buffer_lens:
            raw_last_index = prev_last_index + raw_buffer_len
            raw_buffers.append(
                self._shared_buffer[prev_last_index:raw_last_index].toreadonly()
            )
            prev_last_index = raw_last_index

        # Set the necessary metadata for unpacking
        deserializer = ComplexDataSerializer(raw_buffers, buffer_count)

        # Start unpacking
        return deserializer.deserialize(s_data)

    def get_data_owner(self, data_id):
        """
        Get the data owner rank.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data.

        Returns
        -------
        bool
            Return the status if an object exist in local dictionary.
        """
        return data_id in self._data_map

    def contains_shared_memory(self, data_id):
        return data_id in self._shared_info

    def contains_data_owner(self, data_id):
        """
        Check if the data location info associated with `data_id` exists in a local dictionary.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
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
