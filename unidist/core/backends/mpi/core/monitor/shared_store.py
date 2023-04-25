class SharedStore:
    _instance = None

    def __init__(self):
        self._memory_range = {}
        self._max_memory = 0
        self._index_of_free_memory = 0

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``SharedStore``.

        Returns
        -------
        SharedStore
        """
        if cls.__instance is None:
            cls.__instance = SharedStore()
        return cls.__instance

    def get(self, data_id):
        return self._memory_range[data_id]

    def put(self, data_id, size):
        if size <= 0:
            raise ValueError("Size must have positive value")
        if self._index_of_free_memory + size > self._max_memory:
            raise MemoryError("Shared storaged is overflowed")

        start_index = self._index_of_free_memory
        self._index_of_free_memory += size
        self._memory_range[data_id] = (start_index, self._index_of_free_memory)
        return self._memory_range[data_id]

    def clear(self, cleanup_list):
        pass
