# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`SharedMemoryManager` functionality."""

from array import array

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.core.backends.mpi.core import communication, common
from unidist.core.backends.mpi.core.shared_object_store import SharedObjectStore

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


class FreeMemoryRange:
    """
    Class that helps keep track of free space in memory.

    Parameters
    ----------
    range_len : int
        Memory length.
    """

    def __init__(self, range_len):
        self.range = [[0, range_len]]

    def occupy(self, count=1):
        """
        Take the place of a certain length in memory.

        Parameters
        ----------
        count : int
            Required number of elements in memory.

        Returns
        -------
        int
            First index in memory.
        int
            Last index in memory.
        """
        first_index = None
        last_index = None
        for i in range(len(self.range)):
            if count <= self.range[i][1] - self.range[i][0]:
                first_index = self.range[i][0]
                last_index = first_index + count
                if self.range[i][1] == last_index:
                    self.range = self.range[:i] + self.range[i + 1 :]
                else:
                    self.range[i][0] = last_index
                break

        return first_index, last_index

    def release(self, first_index, last_index):
        """
        Free up memory space.

        Parameters
        ----------
        first_index : int
            First index in memory.
        last_index : int
            Last index in memory. (not inclusive)
        """
        if len(self.range) == 0:
            self.range.append([first_index, last_index])
        elif self.range[-1][1] < first_index:
            self.range.append([first_index, last_index])
        else:
            for i in range(len(self.range)):
                if self.range[i][0] == last_index:
                    if self.range[i - 1][1] == first_index:
                        self.range[i - 1][1] = self.range[i][1]
                        self.range = self.range[:i] + self.range[i + 1 :]
                    else:
                        self.range[i][0] = first_index
                    break
                if self.range[i][1] == first_index:
                    if len(self.range) > i + 1 and self.range[i + 1][0] == last_index:
                        self.range[i + 1][0] = self.range[i][0]
                        self.range = self.range[:i] + self.range[i + 1 :]
                    else:
                        self.range[i][1] = last_index
                    break
                if self.range[i][0] > last_index:
                    self.range = (
                        self.range[:i] + [[first_index, last_index]] + self.range[i:]
                    )
                    break


class SharedMemoryManager:
    """
    Class that helps manage shared memory.
    """

    def __init__(self):
        self.shared_store = SharedObjectStore.get_instance()
        self._reservation_info = {}
        self.free_memory = FreeMemoryRange(self.shared_store.shared_memory_size)
        self.free_service_indexes = FreeMemoryRange(
            self.shared_store.service_info_max_count
        )
        self.deleted_ids = []
        self.pending_cleanup = []

        self.monitor_comm = None
        if common.is_shared_memory_supported():
            mpi_state = communication.MPIState.get_instance()

            monitor_group = mpi_state.comm.Get_group().Incl(mpi_state.monitor_processes)
            self.monitor_comm = mpi_state.comm.Create_group(monitor_group)

    def get(self, data_id):
        """
        Get the reservation information for the `data_id`.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID

        Returns
        -------
        dict or None
            Reservation information.

        Notes
        -----
        The `dict` is returned if a reservation has been specified, otherwise `False` is returned.
        """
        if data_id not in self._reservation_info:
            return None
        return self._reservation_info[data_id].copy()

    def put(self, data_id, memory_len):
        """
        Reserve memory for the `data_id`.

        Parameters
        ----------
        data_id : unidist.core.backends.common.data_id.DataID
        memory_len : int
            Required memory length.

        Returns
        -------
        dict
            Reservation information.
        """
        first_index, last_index = self.free_memory.occupy(memory_len)
        service_index, _ = self.free_service_indexes.occupy(
            SharedObjectStore.INFO_COUNT
        )
        if first_index is None:
            raise MemoryError("Overflow memory")
        if service_index is None:
            raise MemoryError("Overflow service memory")

        reservation_info = {
            "first_index": first_index,
            "last_index": last_index,
            "service_index": service_index,
        }

        self._reservation_info[data_id] = reservation_info
        return reservation_info.copy()

    def clear(self, data_id_list):
        """
        Clear shared memory for the list of `DataID`.

        Parameters
        ----------
        data_id_list : list
            List of `DataID`.
        """
        cleanup_list = self.pending_cleanup + data_id_list
        self.pending_cleanup = []

        has_refs = array(
            "B",
            [
                1
                if data_id in self._reservation_info
                and self.shared_store.get_ref_number(
                    data_id, self._reservation_info[data_id]["service_index"]
                )
                > 0
                else 0
                for data_id in cleanup_list
            ],
        )

        if self.monitor_comm is not None:
            all_refs = array("B", [0] * len(has_refs))
            self.monitor_comm.Allreduce(has_refs, all_refs, MPI.MAX)
        else:
            all_refs = has_refs

        for data_id, referers in zip(cleanup_list, all_refs):
            if referers == 0:
                if data_id in self._reservation_info:
                    reservation_info = self._reservation_info[data_id]
                    self.deleted_ids.append(data_id)
                    self.shared_store.delete_service_info(
                        data_id, reservation_info["service_index"]
                    )
                    self.free_service_indexes.release(
                        reservation_info["service_index"],
                        reservation_info["service_index"]
                        + SharedObjectStore.INFO_COUNT,
                    )
                    self.free_memory.release(
                        reservation_info["first_index"], reservation_info["last_index"]
                    )
                    del self._reservation_info[data_id]
            else:
                self.pending_cleanup.append(data_id)
