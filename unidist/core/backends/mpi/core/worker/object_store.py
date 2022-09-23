# Logger configuration
# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication


# When building documentation we do not have MPI initialized so
# we use the condition to set "worker_0.log" in order to build it succesfully.
log_file = "worker_{}.log".format(
    communication.MPIState.get_instance().rank
    if communication.MPIState.get_instance() is not None
    else 0
)
logger = common.get_logger("worker", log_file)


class ObjectStore:
    """
    Class that stores local objects and provides access to them.

    Notes
    -----
    For now, the storage is local to the current worker process only.
    """

    __instance = None

    def __init__(self):
        # Add local data {DataId : Data}
        self._data_map = {}
        # Data owner {DataId : Rank}
        self._data_owner_map = {}
        # Data serialized cache
        self._serialization_cache = {}

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

    def clear(self, cleanup_list):
        """
        Clear all local dictionary data ID instances from `cleanup_list`.

        Parameters
        ----------
        cleanup_list : list
            List of data IDs.
        """
        for data_id in cleanup_list:
            if data_id in self._data_map:
                logger.debug("CLEANUP DataMap id {}".format(data_id._id))
                del self._data_map[data_id]
            if data_id in self._data_owner_map:
                logger.debug("CLEANUP DataOwnerMap id {}".format(data_id._id))
                del self._data_owner_map[data_id]
            if data_id in self._serialization_cache:
                del self._serialization_cache[data_id]

    def cache_serialized_data(self, data_id, data):
        """
        Save serialized object for this `data_id` and rank.

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
