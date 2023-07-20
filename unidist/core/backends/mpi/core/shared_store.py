# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`SharedStore` functionality."""

import os
import sys
import copy
import psutil
import weakref
import numpy as np
from array import array
from mpi4py import MPI
from unidist.config.backends.mpi.envvars import MpiSharedMemoryThreshold

from unidist.core.backends.mpi.core import common, communication
from unidist.core.backends.mpi.core.serialization import ComplexDataSerializer


class SharedSignaler:
    def __init__(self, win):
        self.win = win

    def __enter__(self):
        self.win.Lock(communication.MPIRank.MONITOR)

    def __exit__(self, *args):
        self.win.Unlock(communication.MPIRank.MONITOR)


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
    INFO_COUNT = 4
    WORKER_ID_INDEX = 0
    DATA_NUMBER_INDEX = 1
    FIRST_DATA_INDEX = 2
    REFERENCES_NUMBER = 3

    SERVICE_COUNT = 200000

    def __init__(self):
        (
            self.shared_buffer,
            self._helper_win,
            self.service_buffer,
        ) = self.__init_shared_memory()

        # Shared memory range {DataID: reservation_info}
        self._shared_info = weakref.WeakKeyDictionary()
        self._helper_buffer = array("L", [0])
        self.logger = None
        self.finalizers = []

    def finalize(self):
        for f in self.finalizers:
            f.detach()

    def __init_shared_memory(self):
        mpi_state = communication.MPIState.get_instance()

        virtual_memory = psutil.virtual_memory().total
        if mpi_state.is_monitor_process():
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

            # use only 95% of available memory because the rest is needed for local storages of workers
            self.shared_memory_size = int(system_memory * 0.95)
        else:
            self.shared_memory_size = 0

        info = MPI.Info.Create()
        info.Set("alloc_shared_noncontig", "true")
        self.win = MPI.Win.Allocate_shared(
            self.shared_memory_size, MPI.BYTE.size, comm=mpi_state.host_comm, info=info
        )
        win_helper = MPI.Win.Allocate_shared(
            1 if self.shared_memory_size > 0 else 0,
            MPI.INT.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        shared_buffer, _ = self.win.Shared_query(communication.MPIRank.MONITOR)

        service_size = self.SERVICE_COUNT if mpi_state.is_monitor_process() else 0
        self.win_service = MPI.Win.Allocate_shared(
            service_size, MPI.LONG.size, comm=mpi_state.host_comm, info=info
        )
        service_buffer, itemsize = self.win_service.Shared_query(
            communication.MPIRank.MONITOR
        )
        ary = np.ndarray(
            buffer=service_buffer,
            dtype="l",
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
        if cls.__instance.logger is None:
            logger_name = f"shm_{communication.MPIState.get_instance().host}"
            cls.__instance.logger = common.get_logger(
                logger_name, f"{logger_name}.log", True
            )
        return cls.__instance

    def is_should_be_shared(self, data):
        # The original data size of numpy.ndarray is greater than the deserialized one using the pickle protocol 5
        # https://discuss.python.org/t/pickle-original-data-size-is-greater-than-deserialized-one-using-pickle-5-protocol/23327
        if str(type(data)) == "<class 'numpy.ndarray'>":
            size = data.size * data.dtype.itemsize
        else:
            size = sys.getsizeof(data)

        return size > MpiSharedMemoryThreshold.get()

    def service_iterator(self):
        current = 0
        while current < len(self.service_buffer) - self.INFO_COUNT:
            yield current, self.service_buffer[
                current + self.WORKER_ID_INDEX
            ], self.service_buffer[
                current + self.DATA_NUMBER_INDEX
            ], self.service_buffer[
                current + self.FIRST_DATA_INDEX
            ], self.service_buffer[
                current + self.REFERENCES_NUMBER
            ],
            current += self.INFO_COUNT

    def check_serice_index(self, data_id, service_index):
        worker_id, data_number = self.parse_data_id(data_id)
        w_id = self.service_buffer[service_index + self.WORKER_ID_INDEX]
        d_id = self.service_buffer[service_index + self.DATA_NUMBER_INDEX]
        result = w_id == worker_id and d_id == data_number
        return result

    def get_service_index(self, data_id):
        if data_id in self._shared_info:
            return self._shared_info[data_id]["service_index"]
        return None

    def get_first_index(self, data_id):
        service_index = self.get_service_index(data_id)
        if service_index is not None:
            return self.service_buffer[service_index + self.FIRST_DATA_INDEX]
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
        if data_id not in self._shared_info:
            self._shared_info[data_id] = copy.deepcopy(shared_info)
            service_index = shared_info["service_index"]
            self.increment_ref_number(data_id, service_index)

    def get_data_shared_info(self, data_id):
        return copy.deepcopy(self._shared_info[data_id])

    def clear_shared_info(self, cleanup_list):
        for data_id in cleanup_list:
            self._shared_info.pop(data_id, None)

    def parse_data_id(self, data_id):
        splited_id = str(data_id).replace(")", "").split("_")
        return int(splited_id[1]), int(splited_id[3])

    def increment_ref_number(self, data_id, service_index):
        if MPI.Is_finalized():
            return
        if service_index is None:
            raise KeyError(
                "it is not possible to increment the reference number for this data_id because it is not part of the shared data"
            )
        with SharedSignaler(self.win_service):
            prev_ref_number = self.service_buffer[
                service_index + self.REFERENCES_NUMBER
            ]
            self.service_buffer[service_index + self.REFERENCES_NUMBER] = (
                prev_ref_number + 1
            )
            self.logger.debug(
                f"Rank {communication.MPIState.get_instance().rank}: Increment references number for {data_id} from {prev_ref_number} to {prev_ref_number + 1}"
            )
        self.finalizers.append(
            weakref.finalize(
                data_id, self.decrement_ref_number, str(data_id), service_index
            )
        )

    def decrement_ref_number(self, data_id, service_index):
        # we must to set service_index in args because it will be deleted before than this function is called
        if MPI.Is_finalized():
            return
        if self.check_serice_index(data_id, service_index):
            with SharedSignaler(self.win_service):
                prev_ref_number = self.service_buffer[
                    service_index + self.REFERENCES_NUMBER
                ]
                self.service_buffer[service_index + self.REFERENCES_NUMBER] = (
                    prev_ref_number - 1
                )
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().rank}: Decrement references number for {data_id} from {prev_ref_number} to {prev_ref_number - 1}"
                )

    def get_ref_number(self, service_index):
        return self.service_buffer[service_index + self.REFERENCES_NUMBER]

    def get(self, data_id):
        info_package = self.get_data_shared_info(data_id)
        buffer_lens = info_package["raw_buffers_lens"]
        buffer_count = info_package["buffer_count"]
        s_data_len = info_package["s_data_len"]
        service_index = info_package["service_index"]

        first_index = self.service_buffer[service_index + self.FIRST_DATA_INDEX]

        s_data_last_index = first_index + s_data_len
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
        data = deserializer.deserialize(s_data)
        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().rank}: Get {data_id} from {first_index} to {prev_last_index}. Service index: {service_index}"
        )
        return data

    def get_shared_buffer(self, first_index, last_index):
        return self.shared_buffer[first_index:last_index]

    def put_service_info(self, service_index, data_id, first_index):
        worker_id, data_number = self.parse_data_id(data_id)

        with SharedSignaler(self.win_service):
            self.service_buffer[service_index + self.FIRST_DATA_INDEX] = first_index
            self.service_buffer[service_index + self.REFERENCES_NUMBER] = 0
            self.service_buffer[service_index + self.DATA_NUMBER_INDEX] = data_number
            self.service_buffer[service_index + self.WORKER_ID_INDEX] = worker_id

    def delete_service_info(self, data_id, service_index):
        with SharedSignaler(self.win_service):
            # Read actual value
            old_worker_id = self.service_buffer[service_index + self.WORKER_ID_INDEX]
            old_data_id = self.service_buffer[service_index + self.DATA_NUMBER_INDEX]
            old_first_index = self.service_buffer[service_index + self.FIRST_DATA_INDEX]
            old_references_number = self.service_buffer[
                service_index + self.REFERENCES_NUMBER
            ]

            # check if data_id is correct
            if self.parse_data_id(data_id) == (old_worker_id, old_data_id):
                self.service_buffer[service_index + self.WORKER_ID_INDEX] = -1
                self.service_buffer[service_index + self.DATA_NUMBER_INDEX] = -1
                self.service_buffer[service_index + self.FIRST_DATA_INDEX] = -1
                self.service_buffer[service_index + self.REFERENCES_NUMBER] = -1
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().rank}: Clear {old_data_id}. Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
            else:
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().rank}: Did not clear {old_data_id}, because there are was written another data_id: Data_ID(rank_{old_worker_id}_id_{old_data_id})"
                )
                self.logger.debug(
                    f"Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
                raise RuntimeError("Unexpected data_id for cleanup shared memory")

    def put(self, data_id, reservation_data, serialized_data):
        first_index = reservation_data["first_index"]
        last_index = reservation_data["last_index"]
        service_index = reservation_data["service_index"]

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
            s_data_len, buffer_lens, buffer_count, service_index
        )
        self.put_service_info(service_index, data_id, first_index)
        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().rank}: PUT {data_id} from {first_index} to {last_prev_index}. Service index: {service_index}"
        )
        self.put_shared_info(data_id, sharing_info)

    def sync_shared_memory_from_another_host(
        self, comm, data_id, owner_rank, first_index, last_index, service_index
    ):
        sh_buf = self.get_shared_buffer(first_index, last_index)
        # recv serialized data to shared memory
        owner_monitor = (
            communication.MPIState.get_instance().get_monitor_by_worker_rank(owner_rank)
        )
        communication.send_simple_operation(
            comm,
            operation_type=common.Operation.REQUEST_SHARED_DATA,
            operation_data={"id": data_id},
            dest_rank=owner_monitor,
        )
        communication.mpi_recv_shared_buffer(comm, sh_buf, owner_monitor)
        self.put_service_info(service_index, data_id, first_index)
        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().rank}: Sync_copy {data_id} from {owner_rank} rank. Put data from {first_index} to {last_index}. Service index: {service_index}"
        )
