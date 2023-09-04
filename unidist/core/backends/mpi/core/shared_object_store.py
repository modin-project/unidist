# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`SharedObjectStore` functionality."""

import os
import sys
import time
import psutil
import weakref
from array import array

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.config.backends.mpi.envvars import MpiSharedMemoryThreshold, MpiBackoff
from unidist.core.backends.mpi.core import common, communication
from unidist.core.backends.mpi.core.serialization import ComplexDataSerializer

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


class SharedSignaler:
    """
    Class that help to synchronize write to shared memory.

    Parameters
    ----------
    win : MPI.win
        The MPI window that was used to create the shared memory.

    Notes
    -----
    This class should be used with the `with` statement.
    """

    def __init__(self, win):
        self.win = win

    def __enter__(self):
        """Lock the current MPI.Window for other processes."""
        self.win.Lock(communication.MPIRank.MONITOR)

    def __exit__(self, *args):
        """Unlock the current MPI.Window for other processes."""
        self.win.Unlock(communication.MPIRank.MONITOR)


class SharedObjectStore:
    """
    Class that provides access to data in shared memory

    Notes
    -----
    This class initializes and manage shared memory.
    """

    __instance = None

    # Service constants defining the structure of the service buffer
    SERVICE_COUNT = 20000
    INFO_COUNT = 4
    WORKER_ID_INDEX = 0
    DATA_NUMBER_INDEX = 1
    FIRST_DATA_INDEX = 2
    REFERENCES_NUMBER = 3

    def __init__(self):
        # The `MPI.Win` object to manage shared memory for data
        self.win = None
        # The `MPI.Win` object to manage shared memory for service purposes
        self.win_service = None
        # `MPI.memory` object for reading/writing data from/to shared memory
        self.shared_buffer = None
        # `memoryview` object for reading/writing data from/to service shared memory.
        # Service buffer includes service information about written shared data.
        # The service info is set by the worker who sends the data to shared memory
        # and is removed by the monitor if the data is cleared.
        # The service info indicates that the current data is written to shared memory
        # and shows the actual location and number of references.
        self.service_buffer = None
        # Length of shared memory buffer in bytes
        self.shared_memory_size = None
        # Length of service shared memory buffer in bytes
        self.service_info_max_count = None

        # Initialize all properties above
        if common.is_shared_memory_supported():
            self._init_shared_memory()

        # Logger will be initialized after `communicator.MPIState`
        self.logger = None
        # Shared memory range {DataID: dict}
        # Shared information is the necessary information to properly deserialize data from shared memory.
        self._shared_info = weakref.WeakKeyDictionary()
        # The list of `weakref.finalize` which should be canceled before closing the shared memory.
        self.finalizers = []

    def _get_allowed_memory_size(self):
        """
        Get allowed memory size for allocate shared memory.

        Returns
        -------
        int
            The number of bytes available to allocate shared memory.
        """
        virtual_memory = psutil.virtual_memory().total
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
        return system_memory

    def _init_shared_memory(self):
        """
        Shared memory initializing
        """
        mpi_state = communication.MPIState.get_instance()

        # Use only 95% of available shared memory because
        # the rest is needed for intermediate shared buffers
        # handled by MPI itself for communication of small messages.
        # Shared memory is allocated only once by the monitor process.
        self.shared_memory_size = (
            int(self._get_allowed_memory_size() * 0.95)
            if mpi_state.is_monitor_process()
            else 0
        )

        info = MPI.Info.Create()
        info.Set("alloc_shared_noncontig", "true")
        self.win = MPI.Win.Allocate_shared(
            self.shared_memory_size * MPI.BYTE.size,
            MPI.BYTE.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        self.shared_buffer, _ = self.win.Shared_query(communication.MPIRank.MONITOR)

        # Service shared memory is allocated only once by the monitor process
        self.service_info_max_count = self.SERVICE_COUNT
        self.win_service = MPI.Win.Allocate_shared(
            self.service_info_max_count * MPI.LONG.size
            if mpi_state.is_monitor_process()
            else 0,
            MPI.LONG.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        service_buffer, _ = self.win_service.Shared_query(communication.MPIRank.MONITOR)
        self.service_buffer = memoryview(service_buffer).cast("l")
        # Set -1 to the service buffer because 0 is a valid value and may be recognized by mistake.
        if mpi_state.is_monitor_process():
            self.service_buffer[:] = array("l", [-1] * len(self.service_buffer))

    def _parse_data_id(self, data_id):
        """
        Parse `DataID` object to pair of int.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            The data identifier to be converted to a numerical form

        Returns
        -------
        tuple
            Pair of int that define the DataID
        """
        splited_id = str(data_id).replace(")", "").split("_")
        return int(splited_id[1]), int(splited_id[3])

    def _increment_ref_number(self, data_id, service_index):
        """
        Increment the number of references to indicate to the monitor that this data is being used.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        service_index : int
            The service buffer index.

        Notes
        -----
        This function create `weakref.finalizer' with decrement function which will be called after data_id collecting.
        """
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
                f"Rank {communication.MPIState.get_instance().global_rank}: Increment references number for {data_id} from {prev_ref_number} to {prev_ref_number + 1}"
            )
        self.finalizers.append(
            weakref.finalize(
                data_id, self._decrement_ref_number, str(data_id), service_index
            )
        )

    def _decrement_ref_number(self, data_id, service_index):
        """
        Decrement the number of references to indicate to the monitor that this data is no longer used.
        When references count is 0, it can be cleared.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        service_index : int
            The service buffer index.

        Notes
        -----
        This function is called in `weakref.finalizer' after data_id collecting.
        """
        # we must to set service_index in args because the shared_info will be deleted before than this function is called
        if MPI.Is_finalized():
            return
        if self._check_service_info(data_id, service_index):
            with SharedSignaler(self.win_service):
                prev_ref_number = self.service_buffer[
                    service_index + self.REFERENCES_NUMBER
                ]
                self.service_buffer[service_index + self.REFERENCES_NUMBER] = (
                    prev_ref_number - 1
                )
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().global_rank}: Decrement references number for {data_id} from {prev_ref_number} to {prev_ref_number - 1}"
                )

    def _put_service_info(self, service_index, data_id, first_index):
        """
        Set service information about written shared data.

        Parameters
        ----------
        service_index : int
        data_id : unidist.core.backends.common.data_id.DataID
        first_index : int

        Notes
        -----
        This information must be set after writing data to shared memory.
        """
        worker_id, data_number = self._parse_data_id(data_id)

        with SharedSignaler(self.win_service):
            self.service_buffer[service_index + self.FIRST_DATA_INDEX] = first_index
            self.service_buffer[service_index + self.REFERENCES_NUMBER] = 1
            self.service_buffer[service_index + self.DATA_NUMBER_INDEX] = data_number
            self.service_buffer[service_index + self.WORKER_ID_INDEX] = worker_id

        self.finalizers.append(
            weakref.finalize(
                data_id, self._decrement_ref_number, str(data_id), service_index
            )
        )

    def _check_service_info(self, data_id, service_index):
        """
        Check if the `data_id` is in the shared memory on the current host.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
            An ID to data.
        service_index : int
            The service buffer index.

        Returns
        -------
        bool
            Return the ``True`` status if `data_id` is in the shared memory.

        Notes
        -----
        This check ensures that the data is physically located in shared memory.
        """
        worker_id, data_number = self._parse_data_id(data_id)
        w_id = self.service_buffer[service_index + self.WORKER_ID_INDEX]
        d_id = self.service_buffer[service_index + self.DATA_NUMBER_INDEX]
        return w_id == worker_id and d_id == data_number

    def _put_shared_info(self, shared_info):
        """
        Put required information to deserialize `data_id`.

        Parameters
        ----------
        shared_info : dict
            Information required for data deserialization

        Notes
        -----
        The store keeps a deep copy, because this information must not be changed.
        """
        data_id = shared_info["id"]
        if data_id not in self._shared_info:
            self._shared_info[data_id] = {
                "s_data_len": shared_info["s_data_len"],
                "raw_buffers_len": shared_info["raw_buffers_len"].copy(),
                "buffer_count": shared_info["buffer_count"].copy(),
                "service_index": shared_info["service_index"],
            }

    def _read_from_shared_buffer(self, data_id, shared_info):
        """
        Read and deserialize data from the shared buffer.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        shared_info : dict
            Information for correct deserialization.

        Returns
        -------
        object
            Data for the current Id.
        """
        buffer_lens = shared_info["raw_buffers_len"]
        buffer_count = shared_info["buffer_count"]
        s_data_len = shared_info["s_data_len"]
        service_index = shared_info["service_index"]

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
            f"Rank {communication.MPIState.get_instance().global_rank}: Get {data_id} from {first_index} to {prev_last_index}. Service index: {service_index}"
        )
        return data

    def _write_to_shared_buffer(self, data_id, reservation_data, serialized_data):
        """
        Write serialized data to the shared buffer.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        reservation_data : dict
            Information about the reserved space in shared memory for the current DataId.
        serialized_data : dict
            Serialized data.

        Returns
        -------
        dict
            Information required for correct data deserialization
        """
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

        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().global_rank}: PUT {data_id} from {first_index} to {last_prev_index}. Service index: {service_index}"
        )

        return common.MetadataPackage.get_shared_info(
            data_id, s_data_len, buffer_lens, buffer_count, service_index
        )

    def _sync_shared_memory_from_another_host(
        self, comm, data_id, owner_rank, first_index, last_index, service_index
    ):
        """
        Receive shared data from another host including the owner's rank

        Parameters
        ----------
        comm : object
            MPI communicator object.
        data_id : unidist.core.backends.common.data_id.DataID
            Data identifier
        owner_rank : int
            Rank of the owner process.
        first_index : int
            First index in shared memory.
        last_index : int
            Last index in shared memory.
        service_index : int
            Service buffer index.

        Notes
        -----
        After writting data, service information should be set.
        """
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
        communication.mpi_recv_buffer(comm, owner_monitor, sh_buf)
        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().global_rank}: Sync_copy {data_id} from {owner_rank} rank. Put data from {first_index} to {last_index}. Service index: {service_index}"
        )

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``SharedObjectStore``.

        Returns
        -------
        SharedObjectStore
        """
        if cls.__instance is None:
            cls.__instance = SharedObjectStore()
        if cls.__instance.logger is None:
            logger_name = f"shared_store_{communication.MPIState.get_instance().host}"
            cls.__instance.logger = common.get_logger(
                logger_name, f"{logger_name}.log", True
            )
        return cls.__instance

    def should_be_shared(self, data):
        """
        Check if data should be sent using shared memory.

        Parameters
        ----------
        data : object
            Any data needed to be sent to another process.

        Returns
        -------
        bool
            Return the ``True`` status if data should be sent using shared memory.
        """
        if self.shared_buffer is None:
            return False

        size = sys.getsizeof(data)

        # sys.getsizeof may return incorrect data size as
        # it doesn't fully assume the whole structure of an object
        # so we manually compute the data size for np.array here.
        try:
            import numpy as np

            if isinstance(data, np.ndarray):
                size = data.size * data.dtype.itemsize
        except ImportError:
            pass

        return size > MpiSharedMemoryThreshold.get()

    def contains(self, data_id):
        """
        Check if the store contains the `data_id` information required to deserialize the data.

        Returns
        -------
        bool
            Return the ``True`` status if shared store contains required information.

        Notes
        -----
        This check does not ensure that the data is physically located in shared memory.
        """
        return data_id in self._shared_info

    def get_shared_info(self, data_id):
        """
        Get required information to correct deserialize `data_id` from shared memory.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID

        Returns
        -------
        dict
            Information required for data deserialization

        Notes
        -----
        The store return a deep copy, because this information must not be changed.
        """
        shared_info = self._shared_info[data_id]
        return common.MetadataPackage.get_shared_info(
            data_id,
            shared_info["s_data_len"],
            shared_info["raw_buffers_len"],
            shared_info["buffer_count"],
            shared_info["service_index"],
        )

    def get_ref_number(self, data_id, service_index):
        """
        Get current references count of data_id by service index.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        service_index : int
            The service buffer index.

        Returns
        -------
        int
            The number of references to this data_id
        """
        # we must to set service_index in args because this function is called from monitor which can not known the shared_info
        if not self._check_service_info(data_id, service_index):
            return 0
        return self.service_buffer[service_index + self.REFERENCES_NUMBER]

    def get_shared_buffer(self, first_index, last_index):
        """
        Get the requested range of shared memory

        Parameters
        ----------
        first_index : int
            Start of the requested range.
        last_index : int
            End of the requested range. (excluding)

        Notes
        -----
        This function is used to synchronize shared memory between different hosts.
        """
        return self.shared_buffer[first_index:last_index]

    def delete_service_info(self, data_id, service_index):
        """
        Delete service information for the current data Id.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        service_index : int
            The service buffer index.

        Notes
        -----
        This function should be called by the monitor during the cleanup of shared data.
        """
        with SharedSignaler(self.win_service):
            # Read actual value
            old_worker_id = self.service_buffer[service_index + self.WORKER_ID_INDEX]
            old_data_id = self.service_buffer[service_index + self.DATA_NUMBER_INDEX]
            old_first_index = self.service_buffer[service_index + self.FIRST_DATA_INDEX]
            old_references_number = self.service_buffer[
                service_index + self.REFERENCES_NUMBER
            ]

            # check if data_id is correct
            if self._parse_data_id(data_id) == (old_worker_id, old_data_id):
                self.service_buffer[service_index + self.WORKER_ID_INDEX] = -1
                self.service_buffer[service_index + self.DATA_NUMBER_INDEX] = -1
                self.service_buffer[service_index + self.FIRST_DATA_INDEX] = -1
                self.service_buffer[service_index + self.REFERENCES_NUMBER] = -1
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().global_rank}: Clear {old_data_id}. Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
            else:
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().global_rank}: Did not clear {old_data_id}, because there are was written another data_id: Data_ID(rank_{old_worker_id}_id_{old_data_id})"
                )
                self.logger.debug(
                    f"Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
                raise RuntimeError("Unexpected data_id for cleanup shared memory")

    def put(self, data_id, data):
        """
        Put data into shared memory.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        data : object
            The current data.
        """
        mpi_state = communication.MPIState.get_instance()

        # serialize data
        serializer = ComplexDataSerializer()
        s_data = serializer.serialize(data)
        raw_buffers = serializer.buffers
        buffer_count = serializer.buffer_count
        data_size = len(s_data) + sum([len(buf) for buf in raw_buffers])
        serialized_data = {
            "s_data": s_data,
            "raw_buffers": raw_buffers,
            "buffer_count": buffer_count,
        }

        # reserve shared memory
        reservation_data = communication.send_reserve_operation(
            mpi_state.comm, data_id, data_size
        )
        service_index = reservation_data["service_index"]
        first_index = reservation_data["first_index"]

        # write into shared buffer
        shared_info = self._write_to_shared_buffer(
            data_id, reservation_data, serialized_data
        )

        # put service info
        self._put_service_info(service_index, data_id, first_index)

        # put shared info
        self._put_shared_info(shared_info)

    def get(self, data_id, owner_rank, shared_info):
        """
        Get data from another worker using shared memory.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        owner_rank : int
            The rank that sent the data.
        shared_info : dict
            The necessary information to properly deserialize data from shared memory.
        """
        mpi_state = communication.MPIState.get_instance()
        s_data_len = shared_info["s_data_len"]
        raw_buffers_len = shared_info["raw_buffers_len"]
        service_index = shared_info["service_index"]
        buffer_count = shared_info["buffer_count"]

        # check data in shared memory
        if not self._check_service_info(data_id, service_index):
            # reserve shared memory
            shared_data_len = s_data_len + sum([buf for buf in raw_buffers_len])
            reservation_info = communication.send_reserve_operation(
                mpi_state.comm, data_id, shared_data_len
            )

            service_index = reservation_info["service_index"]
            # check if worker should sync shared buffer or it is doing by another worker
            if reservation_info["is_first_request"]:
                # syncronize shared buffer
                self._sync_shared_memory_from_another_host(
                    mpi_state.comm,
                    data_id,
                    owner_rank,
                    reservation_info["first_index"],
                    reservation_info["last_index"],
                    service_index,
                )
                # put service info
                self._put_service_info(
                    service_index, data_id, reservation_info["first_index"]
                )
            else:
                # wait while another worker syncronize shared buffer
                while not self._check_service_info(data_id, service_index):
                    time.sleep(MpiBackoff.get())

        # put shared info with updated data_id and service_index
        shared_info = common.MetadataPackage.get_shared_info(
            data_id, s_data_len, raw_buffers_len, buffer_count, service_index
        )
        self._put_shared_info(shared_info)

        # increment ref
        self._increment_ref_number(data_id, shared_info["service_index"])

        # read from shared buffer and deserialized
        return self._read_from_shared_buffer(data_id, shared_info)

    def finalize(self):
        """
        Release used resources.

        Notes
        -----
        Shared store should be finalized before MPI.Finalize().
        """
        if self.win is not None:
            self.win.Free()
            self.win = None
        if self.win_service is not None:
            self.win_service.Free()
            self.win_service = None
        for f in self.finalizers:
            f.detach()
