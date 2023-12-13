from unidist.core.backends.mpi.core.local_object_store import LocalObjectStore
from unidist.core.backends.mpi.core.serialization import deserialize_complex_data
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore


class ObjectStore:
    """
    Class that combines checking and reciving data from all stores in a current process.

    Notes
    -----
    The store checks for both deserialized and serialized data.
    """

    __instance = None

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

    def contains_data(self, data_id):
        """
        Check if the data associated with `data_id` exists in the current process.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.

        Returns
        -------
        bool
            Return the status if an object exist in the current process.
        """
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()
        return (
            local_store.contains(data_id)
            or local_store.is_already_serialized(data_id)
            or shared_store.contains(data_id)
        )

    def get_data(self, data_id):
        """
        Get data from any location in the current process.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.

        Returns
        -------
        object
            Return data associated with `data_id`.
        """
        local_store = LocalObjectStore.get_instance()
        shared_store = SharedObjectStore.get_instance()

        if local_store.contains(data_id):
            return local_store.get(data_id)

        if local_store.is_already_serialized(data_id):
            serialized_data = local_store.get_serialized_data(data_id)
            value = deserialize_complex_data(
                serialized_data["s_data"],
                serialized_data["raw_buffers"],
                serialized_data["buffer_count"],
            )
        elif shared_store.contains(data_id):
            value = shared_store.get(data_id)
        else:
            raise ValueError("The current data ID is not contained in the procces.")
        local_store.put(data_id, value)
        return value
