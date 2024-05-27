# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`SharedObjectStore` functionality."""

import os
import sys
import time
import warnings
import psutil
import weakref

from unidist.core.backends.mpi.core._memory import parallel_memcopy, fill
from unidist.core.backends.mpi.utils import ImmutableDict

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.config.backends.mpi.envvars import (
    MpiSharedObjectStoreMemory,
    MpiSharedServiceMemory,
    MpiSharedObjectStoreThreshold,
    MpiBackoff,
)
from unidist.core.backends.mpi.core import common, communication
from unidist.core.backends.mpi.core.serialization import (
    deserialize_complex_data,
)

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


class WinLock:
    """
    Class that helps to synchronize the write to shared memory.

    Parameters
    ----------
    win : MPI.win
        The MPI window that was used to allocate the shared memory.

    Notes
    -----
    This class should be used as a context manager.
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
    Class that provides access to data in shared memory.

    Notes
    -----
    This class initializes and manages shared memory.
    """

    __instance = None

    # Service constants defining the structure of the service buffer
    # The amount of service information for one data object in shared memory.
    INFO_SIZE = 4
    # Index of service information for the first part of the DataID.
    WORKER_ID_INDEX = 0
    # Index of service information for the second part of the DataID.
    DATA_NUMBER_INDEX = 1
    # Index of service information for the first shared memory index where the data is located.
    FIRST_DATA_INDEX = 2
    # Index of service information to count the number of data references,
    # which shows how many processes are using this data.
    REFERENCES_NUMBER = 3

    def __init__(self):
        # The `MPI.Win` object to manage shared memory for data
        self.win = None
        # The `MPI.Win` object to manage shared memory for service purposes
        self.service_win = None
        # `MPI.memory` object for reading/writing data from/to shared memory
        self.shared_buffer = None
        # `memoryview` object for reading/writing data from/to service shared memory.
        # Service shared buffer includes service information about written shared data.
        # The service info is set by the worker who sends the data to shared memory
        # and is removed by the monitor if the data is cleared.
        # The service info indicates that the current data is written to shared memory
        # and shows the actual location and number of references.
        self.service_shared_buffer = None
        # Length of shared memory buffer in bytes
        self.shared_memory_size = None
        # Length of service shared memory buffer in items
        self.service_info_max_count = None

        mpi_state = communication.MPIState.get_instance()
        # Initialize all properties above
        if common.is_shared_memory_supported(raise_warning=mpi_state.is_root_process()):
            self._allocate_shared_memory()

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
                    warnings.warn(
                        f"The size of /dev/shm is too small ({system_memory} bytes). The required size "
                        + f"at least half of RAM ({virtual_memory // 2} bytes). Please, delete files in /dev/shm or "
                        + "increase size of /dev/shm with --shm-size in Docker."
                    )
            finally:
                os.close(shm_fd)
        else:
            system_memory = virtual_memory
        return system_memory

    def _allocate_shared_memory(self):
        """
        Allocate shared memory.
        """
        mpi_state = communication.MPIState.get_instance()

        shared_object_store_memory = MpiSharedObjectStoreMemory.get()
        shared_service_memory = MpiSharedServiceMemory.get()
        # Use only 95% of available shared memory because
        # the rest is needed for intermediate shared buffers
        # handled by MPI itself for communication of small messages.
        allowed_memory_size = int(self._get_allowed_memory_size() * 0.95)

        if shared_object_store_memory is not None:
            if shared_service_memory is not None:
                self.shared_memory_size = shared_object_store_memory
                self.service_memory_size = shared_service_memory
            else:
                self.shared_memory_size = shared_object_store_memory
                # To avoid division by 0
                if MpiSharedObjectStoreThreshold.get() > 0:
                    self.service_memory_size = min(
                        # allowed memory size for service buffer
                        allowed_memory_size - self.shared_memory_size,
                        # maximum amount of memory required for the service buffer
                        (self.shared_memory_size // MpiSharedObjectStoreThreshold.get())
                        * (self.INFO_SIZE * MPI.LONG.size),
                    )
                else:
                    self.service_memory_size = (
                        allowed_memory_size - self.shared_memory_size
                    )
        else:
            if shared_service_memory is not None:
                self.service_memory_size = shared_service_memory
                self.shared_memory_size = allowed_memory_size - self.service_memory_size
            else:
                A = allowed_memory_size
                B = MpiSharedObjectStoreThreshold.get()
                C = self.INFO_SIZE * MPI.LONG.size
                # "x" is shared_memory_size
                # "y" is service_memory_size

                # requirements:
                # x + y = A
                # y = min[ (x/B) * C, 0.01 * A ]

                # calculation results:
                # if B > 99 * C:
                # x = (A * B) / (B + C)
                # y = (A * C) / (B + C)
                # else:
                # x = 0.99 * A
                # y = 0.01 * A

                if B > 99 * C:
                    self.shared_memory_size = (A * B) // (B + C)
                    self.service_memory_size = (A * C) // (B + C)
                else:
                    self.shared_memory_size = int(0.99 * A)
                    self.service_memory_size = int(0.01 * A)

        if self.shared_memory_size > allowed_memory_size:
            raise ValueError(
                "Memory for shared object storage cannot be allocated "
                + "because the value set to `MpiSharedObjectStoreMemory` exceeds the available memory."
            )

        if self.service_memory_size > allowed_memory_size:
            raise ValueError(
                "Memory for shared service storage cannot be allocated "
                + "because the value set to `MpiSharedServiceMemory` exceeds the available memory."
            )

        if self.shared_memory_size + self.service_memory_size > allowed_memory_size:
            raise ValueError(
                "The sum of the `MpiSharedObjectStoreMemory` and `MpiSharedServiceMemory` values is greater "
                + "than the available amount of memory."
            )

        # Shared memory is allocated only once by the monitor process.
        info = MPI.Info.Create()
        info.Set("alloc_shared_noncontig", "true")
        self.win = MPI.Win.Allocate_shared(
            (
                self.shared_memory_size * MPI.BYTE.size
                if mpi_state.is_monitor_process()
                else 0
            ),
            MPI.BYTE.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        self.shared_buffer, _ = self.win.Shared_query(communication.MPIRank.MONITOR)

        self.service_info_max_count = (
            self.service_memory_size
            // (self.INFO_SIZE * MPI.LONG.size)
            * self.INFO_SIZE
        )
        self.service_win = MPI.Win.Allocate_shared(
            (
                self.service_info_max_count * MPI.LONG.size
                if mpi_state.is_monitor_process()
                else 0
            ),
            MPI.LONG.size,
            comm=mpi_state.host_comm,
            info=info,
        )
        service_buffer, _ = self.service_win.Shared_query(communication.MPIRank.MONITOR)
        self.service_shared_buffer = memoryview(service_buffer).cast("l")
        # Set -1 to the service buffer because 0 is a valid value and may be recognized by mistake.
        if mpi_state.is_monitor_process():
            fill(self.service_shared_buffer, -1)

    def _parse_data_id(self, data_id):
        """
        Parse `DataID` object to pair of int.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        with WinLock(self.service_win):
            prev_ref_number = self.service_shared_buffer[
                service_index + self.REFERENCES_NUMBER
            ]
            self.service_shared_buffer[service_index + self.REFERENCES_NUMBER] = (
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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        service_index : int
            The service buffer index.

        Notes
        -----
        This function is called in `weakref.finalizer' after data_id collecting.
        """
        # we must set service_index in args because the shared_info will be deleted before than this function is called
        if MPI.Is_finalized():
            return
        if self._check_service_info(data_id, service_index):
            with WinLock(self.service_win):
                prev_ref_number = self.service_shared_buffer[
                    service_index + self.REFERENCES_NUMBER
                ]
                self.service_shared_buffer[service_index + self.REFERENCES_NUMBER] = (
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
            The service buffer index.
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        first_index : int
            The first index of data in the shared buffer.

        Notes
        -----
        This information must be set after writing data to shared memory.
        """
        worker_id, data_number = self._parse_data_id(data_id)

        with WinLock(self.service_win):
            self.service_shared_buffer[service_index + self.FIRST_DATA_INDEX] = (
                first_index
            )
            self.service_shared_buffer[service_index + self.REFERENCES_NUMBER] = 1
            self.service_shared_buffer[service_index + self.DATA_NUMBER_INDEX] = (
                data_number
            )
            self.service_shared_buffer[service_index + self.WORKER_ID_INDEX] = worker_id

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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        w_id = self.service_shared_buffer[service_index + self.WORKER_ID_INDEX]
        d_id = self.service_shared_buffer[service_index + self.DATA_NUMBER_INDEX]
        return w_id == worker_id and d_id == data_number

    def _put_shared_info(self, data_id, shared_info):
        """
        Put required information to deserialize `data_id`.

        Parameters
        ----------
        data_id : uunidist.core.backends.mpi.core.common.MpiDataID
        shared_info : unidist.core.backends.mpi.utils.ImmutableDict
            Information required for data deserialization
        """
        if not isinstance(shared_info, ImmutableDict):
            raise ValueError("Shared info should be immutable.")
        if data_id not in self._shared_info:
            self._shared_info[data_id] = shared_info

    def _read_from_shared_buffer(self, data_id, shared_info):
        """
        Read and deserialize data from the shared buffer.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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

        first_index = self.service_shared_buffer[service_index + self.FIRST_DATA_INDEX]

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

        data = deserialize_complex_data(s_data, raw_buffers, buffer_count)
        self.logger.debug(
            f"Rank {communication.MPIState.get_instance().global_rank}: Get {data_id} from {first_index} to {prev_last_index}. Service index: {service_index}"
        )
        return data

    def _write_to_shared_buffer(self, data_id, reservation_data, serialized_data):
        """
        Write serialized data to the shared buffer.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
        reservation_data : dict
            Information about the reserved space in shared memory for the current DataID.
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

        if s_data_last_index > last_index:
            raise ValueError("Not enough shared space for data")
        self.shared_buffer[s_data_first_index:s_data_last_index] = s_data

        last_prev_index = s_data_last_index
        for i, raw_buffer in enumerate(raw_buffers):
            raw_buffer_first_index = last_prev_index
            raw_buffer_len = len(raw_buffer)
            last_prev_index = raw_buffer_first_index + len(raw_buffer)
            if last_prev_index > last_index:
                raise ValueError(f"Not enough shared space for {i} raw_buffer")

            parallel_memcopy(
                raw_buffer,
                self.shared_buffer[raw_buffer_first_index:last_prev_index],
                6,
            )

            buffer_lens.append(raw_buffer_len)

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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
            cls.__instance.logger = common.get_logger(logger_name, f"{logger_name}.log")
        return cls.__instance

    def is_allocated(self):
        """
        Check if the shared memory is allocated and ready to put data.x

        Returns
        -------
        bool
            True ot False.
        """
        return self.shared_buffer is not None

    def should_be_shared(self, data):
        """
        Check if data should be sent using shared memory.

        Parameters
        ----------
        data : dict
            Serialized data to check its size.

        Returns
        -------
        bool
            Return the ``True`` status if data should be sent using shared memory.
        """
        data_size = len(data["s_data"]) + sum([len(buf) for buf in data["raw_buffers"]])
        return data_size > MpiSharedObjectStoreThreshold.get()

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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID

        Returns
        -------
        dict
            Information required for data deserialization
        """
        return self._shared_info[data_id]

    def get_ref_number(self, data_id, service_index):
        """
        Get current references count of data_id by service index.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
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
        return self.service_shared_buffer[service_index + self.REFERENCES_NUMBER]

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
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
        service_index : int
            The service buffer index.

        Notes
        -----
        This function should be called by the monitor during the cleanup of shared data.
        """
        with WinLock(self.service_win):
            # Read actual value
            old_worker_id = self.service_shared_buffer[
                service_index + self.WORKER_ID_INDEX
            ]
            old_data_id = self.service_shared_buffer[
                service_index + self.DATA_NUMBER_INDEX
            ]
            old_first_index = self.service_shared_buffer[
                service_index + self.FIRST_DATA_INDEX
            ]
            old_references_number = self.service_shared_buffer[
                service_index + self.REFERENCES_NUMBER
            ]

            # check if data_id is correct
            if self._parse_data_id(data_id) == (old_worker_id, old_data_id):
                self.service_shared_buffer[service_index + self.WORKER_ID_INDEX] = -1
                self.service_shared_buffer[service_index + self.DATA_NUMBER_INDEX] = -1
                self.service_shared_buffer[service_index + self.FIRST_DATA_INDEX] = -1
                self.service_shared_buffer[service_index + self.REFERENCES_NUMBER] = -1
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().global_rank}: Clear {data_id}. Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
            else:
                self.logger.debug(
                    f"Rank {communication.MPIState.get_instance().global_rank}: Did not clear {data_id}, because there are was written another data_id: Data_ID(rank_{old_worker_id}_id_{old_data_id})"
                )
                self.logger.debug(
                    f"Service index: {service_index} First index: {old_first_index} References number: {old_references_number}"
                )
                raise RuntimeError("Unexpected data_id for cleanup shared memory")

    def put(self, data_id, serialized_data):
        """
        Put data into shared memory.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
        serialized_data : dict
            Serialized data to put into the storage.
        """
        mpi_state = communication.MPIState.get_instance()

        data_size = len(serialized_data["s_data"]) + sum(
            [len(buf) for buf in serialized_data["raw_buffers"]]
        )
        # reserve shared memory
        reservation_data = communication.send_reserve_operation(
            mpi_state.global_comm, data_id, data_size
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
        self._put_shared_info(data_id, shared_info)

    def get(self, data_id, owner_rank=None, shared_info=None):
        """
        Get data from another worker using shared memory.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MpiDataID
            An ID to data.
        owner_rank : int, default: None
            The rank that sent the data.
            This value is used to synchronize data in shared memory between different hosts
            if the value is not ``None``.
        shared_info : dict, default: None
            The necessary information to properly deserialize data from shared memory.
            If `shared_info` is ``None``, the data already exists in shared memory in the current process.
        """
        if shared_info is None:
            shared_info = self.get_shared_info(data_id)
        else:
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
                    mpi_state.global_comm, data_id, shared_data_len
                )

                service_index = reservation_info["service_index"]
                # check if worker should sync shared buffer or it is doing by another worker
                if reservation_info["is_first_request"]:
                    # syncronize shared buffer
                    if owner_rank is None:
                        raise ValueError(
                            "The data is not in the host's shared memory and the data must be synchronized, "
                            + "but the owner rank is not defined."
                        )

                    self._sync_shared_memory_from_another_host(
                        mpi_state.global_comm,
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
            self._put_shared_info(data_id, shared_info)

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
        if self.service_win is not None:
            self.service_win.Free()
            self.service_win = None
        for f in self.finalizers:
            f.detach()
