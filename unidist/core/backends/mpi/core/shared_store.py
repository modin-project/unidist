import os
import sys
import psutil
import weakref
import numpy as np
from array import array
from mpi4py import MPI

from unidist.core.backends.mpi.core import common, communication
from unidist.core.backends.mpi.core.communication import MPIState
from unidist.core.backends.mpi.core.serialization import ComplexDataSerializer

class SharedStore:
    # [byte]            shared buffer with serialized data
    # [int]             service buffer                                                                      ?? How to delete data ??
    # ?? [int int]      used ranges of shared buffer (use for reserve memory in shared buffer)
    #
    # [int]             DataId.WorkerId
    # [int]             DataId.Number
    # [int]             first index in shared buffer
    # [array of int]    S_data len + raw_buffers_lens -> [int]   first index of service buffer
    #                                                 -> [int]   last index of service buffer
    # [array of int]    buffer_count                  -> [int]   first index of service buffer
    #                                                 -> [int]   last index of service buffer
    __instance = None
    INFO_COUNT = 3
    WORKER_ID_INDEX = 0
    DATA_NUMBER_INDEX = 1
    FIRST_DATA_INDEX = 2

    SERVICE_COUNT = 10000

    def __init__(self):
        (
            self.shared_buffer,
            self._helper_win,
            self.service_buffer,
        ) = self.__init_shared_memory()

        # Shared memory range {DataID: (firstIndex, lastIndex)}
        self._shared_info = weakref.WeakKeyDictionary()
        self._helper_buffer = array("L", [0])
        mpi_state = communication.MPIState.get_instance()
        log_name = f'shared_store{mpi_state.rank}'
        self.logger = common.get_logger(log_name, f'{log_name}.log')

    def __init_shared_memory(self):
        mpi_state = communication.MPIState.get_instance()

        virtual_memory = psutil.virtual_memory().total
        if mpi_state.host_rank == communication.MPIRank.MONITOR:
            if sys.platform.startswith("linux"):
                shm_fd = os.open("/dev/shm", os.O_RDONLY)
                try:
                    shm_stats = os.fstatvfs(shm_fd)
                    system_memory = shm_stats.f_bsize * shm_stats.f_bavail
                    if system_memory / (virtual_memory / 2) < 0.99:
                        print(
                            f"The size of /dev/shm is too small ({system_memory} bytes). The required size "
                            + f"at least half of RAM ({virtual_memory // 2} bytes). Please, delete files in /dev/shm or "
                            + "increase size of /dev/shm with --shm-size in Docker."
                        )
                finally:
                    os.close(shm_fd)
            else:
                system_memory = virtual_memory

            # use only 95% because other memory need for local worker storages
            shared_memory_size = int(system_memory * 0.95)
        else:
            shared_memory_size = 0
        # experimentary for 07 server
        # 4800374938 - 73728 * mpi_state.world_size

        info = MPI.Info.Create()
        info.Set("alloc_shared_noncontig", "true")
        win = MPI.Win.Allocate_shared(
            shared_memory_size, MPI.BYTE.size, comm=mpi_state.host_comm, info=info
        )
        win_helper = MPI.Win.Allocate_shared(
            1 if shared_memory_size > 0 else 0,
            MPI.INT.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        rank = MPIState.get_instance().rank
        shared_buffer, _ = win.Shared_query(communication.MPIRank.MONITOR)

        service_size = self.SERVICE_COUNT if mpi_state.host_rank == communication.MPIRank.MONITOR else 0
        win_service = MPI.Win.Allocate_shared(
            service_size, MPI.INT.size, comm=mpi_state.host_comm, info=info
        )
        service_buffer, itemsize = win_service.Shared_query(communication.MPIRank.MONITOR)
        ary = np.ndarray(
            buffer=service_buffer,
            dtype="i",
            shape=(int(len(service_buffer) / itemsize),),
        )
        if service_size:
            ary[True] = -1

        return shared_buffer, win_helper, ary

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``SharedMemoryManager``.

        Returns
        -------
        SharedMemoryManager
        """
        if cls.__instance is None:
            cls.__instance = SharedStore()
        return cls.__instance

    def service_iterator(self):
        current = 0
        while current < len(self.service_buffer) - self.INFO_COUNT:
            yield self.service_buffer[
                current + self.WORKER_ID_INDEX
            ], self.service_buffer[
                current + self.DATA_NUMBER_INDEX
            ], self.service_buffer[
                current + self.FIRST_DATA_INDEX
            ]
            current += self.INFO_COUNT

    def get_first_index(self, data_id):
        worker_id, data_number = self.parse_data_id(data_id)
        for w_id, d_id, f_index in self.service_iterator():
            if w_id == worker_id and d_id == data_number:
                return f_index
        return None

    def contains_shared_info(self, data_id):
        return data_id in self._shared_info

    def contains(self, data_id):
        index = self.get_first_index(data_id)
        if index is None:
            return False
        else:
            return True

    def put_shared_info(self, data_id, shared_info):
        self._shared_info[data_id] = shared_info

    def get_data_shared_info(self, data_id):
        return self._shared_info[data_id]

    def parse_data_id(self, data_id):
        splited_id = str(data_id).replace(")", "").split("_")
        return int(splited_id[1]), int(splited_id[3])

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

    def get(self, data_id):
        # index = self.get_index(data_id)
        # buffer_lens = self.service_buffer[
        #     self.buffer_len_firsts[index] : self.buffer_count_firsts[index]
        # ]

        info_package = SharedStore.get_instance().get_data_shared_info(data_id)
        first_index = info_package["first_shared_index"]
        buffer_lens = info_package["raw_buffers_lens"]
        buffer_count = info_package["buffer_count"]
        s_data_len = info_package["s_data_len"]

        s_data_last_index = first_index + s_data_len
        self.logger.debug(self.shared_buffer[first_index:first_index+100].tobytes())
        s_data = self.shared_buffer[first_index:s_data_last_index].toreadonly()
        prev_last_index = s_data_last_index
        raw_buffers = []
        for raw_buffer_len in buffer_lens:
            raw_last_index = prev_last_index + raw_buffer_len
            raw_buffers.append(
                self.shared_buffer[prev_last_index:raw_last_index].toreadonly()
            )
            prev_last_index = raw_last_index

        # Set the necessary metadata for unpacking
        deserializer = ComplexDataSerializer(raw_buffers, buffer_count)

        # Start unpacking
        try:
            return deserializer.deserialize(s_data)
        except Exception as ex:
            self.logger.debug(data_id)
            self.logger.exception(ex)
            raise

        

    def get_shared_buffer(self, data_id):
        info_package = self.get_data_shared_info(data_id)
        first_index = info_package["first_shared_index"]
        buffer_lens = info_package["raw_buffers_lens"]
        s_data_len = info_package["s_data_len"]

        last_index = first_index + s_data_len
        for raw_buffer_len in buffer_lens:
            last_index += raw_buffer_len

        return self.shared_buffer[first_index:last_index]

    def put_service_info(self, data_id):
        mpi_state = MPIState.get_instance()
        rank = mpi_state.rank
        logger_name = f"shm_{mpi_state.host}"
        logger = common.get_logger(logger_name, f"{logger_name}.log")
        info_package = self.get_data_shared_info(data_id)
        first_index = info_package["first_shared_index"]
        worker_id, data_number = self.parse_data_id(data_id)
        index_to_write = 0
        for w_id, d_num, f_i in self.service_iterator():
            if w_id == -1 or d_num == -1:
                break
            else:
                index_to_write += self.INFO_COUNT
        if index_to_write >= self.SERVICE_COUNT:
            logger.debug(f"Service buffer overflow on {rank} rank")
            raise BufferError("Service buffer overflow")
        self.service_buffer[index_to_write + self.WORKER_ID_INDEX] = worker_id
        self.service_buffer[index_to_write + self.DATA_NUMBER_INDEX] = data_number
        self.service_buffer[index_to_write + self.FIRST_DATA_INDEX] = first_index
        logger.debug(f"Write service from {rank} rank:")
        logger.debug(f"{index_to_write + self.WORKER_ID_INDEX}: {worker_id}")
        logger.debug(f"{index_to_write + self.DATA_NUMBER_INDEX}: {data_number}")
        logger.debug(f"{index_to_write + self.FIRST_DATA_INDEX}: {first_index}")

    def put(self, data_id, reservation_data, serialized_data):
        first_index = reservation_data["firstIndex"]
        last_index = reservation_data["lastIndex"]

        s_data = serialized_data["s_data"]
        raw_buffers = serialized_data["raw_buffers"]
        buffer_count = serialized_data["buffer_count"]
        s_data_len = len(s_data)

        buffer_lens = []
        s_data_first_index = first_index
        s_data_last_index = s_data_first_index + s_data_len
        # buffer_lens.append(s_data_len)

        if s_data_last_index > last_index:
            raise ValueError("Not enough shared space for data")
        self.shared_buffer[s_data_first_index:s_data_last_index] = s_data

        last_prev_index = s_data_last_index
        for i, raw_buffer in enumerate(raw_buffers):
            raw_buffer_first_index = last_prev_index
            raw_buffer_len = len(raw_buffer)
            raw_buffer_last_index = raw_buffer_first_index + len(raw_buffer)
            if s_data_last_index > last_index:
                raise ValueError(f"Not enough shared space for {i} raw_buffer")

            self.shared_buffer[
                raw_buffer_first_index:raw_buffer_last_index
            ] = raw_buffer

            buffer_lens.append(raw_buffer_len)
            last_prev_index = raw_buffer_last_index

        sharing_info = communication.get_shared_info(
            data_id, s_data_len, buffer_lens, buffer_count, first_index
        )
        self.put_shared_info(data_id, sharing_info)
        self.put_service_info(data_id)
