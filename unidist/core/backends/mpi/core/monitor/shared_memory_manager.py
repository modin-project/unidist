
from array import array
from mpi4py import MPI
from unidist.core.backends.mpi.core import communication
from unidist.core.backends.mpi.core.shared_store import SharedStore


class FreeMemoryRange:
    def __init__(self, range_len):
        self.range = [[0, range_len]]

    def pop(self, count=1):
        first_index = None
        last_index = None
        for i in range(len(self.range)):
            if self.range[i][1] - self.range[i][0] >= count:
                first_index = self.range[i][0]
                last_index = first_index + count
                if self.range[i][1] == last_index:
                    self.range = self.range[:i] + self.range[i+1:]
                else:
                    self.range[i][0] = last_index
                break
        
        return first_index, last_index

    def push(self, first_index, last_index=None):
        if last_index is None:
            last_index = first_index + 1

        if len(self.range) == 0:
            self.range.append([first_index, last_index])
        elif self.range[-1][1] < first_index:
            self.range.append([first_index, last_index])
        else:
            for i in range(len(self.range)):
                if self.range[i][0] == last_index:
                    if self.range[i-1][1] == first_index:
                        self.range[i-1][1] = self.range[i][1]
                        self.range = self.range[:i] + self.range[i+1:]
                    else:
                        self.range[i][0] = first_index
                    break
                if self.range[i][1] == first_index:
                    if len(self.range) > i+1 and self.range[i+1][0] == last_index:
                        self.range[i+1][0] = self.range[i][0]
                        self.range = self.range[:i] + self.range[i+1:]
                    else:
                        self.range[i][1] = last_index
                    break
                if self.range[i][0] > last_index:
                    self.range =  self.range[:i] + [[first_index, last_index]] + self.range[i:]
                    break   



class SharedMemoryMahager:
    def __init__(self, shared_memory_len):
        self.reservation_info = {}
        self.free_memory = FreeMemoryRange(shared_memory_len)
        self.free_service_indexes = FreeMemoryRange(SharedStore.SERVICE_COUNT)
        self.deleted_ids = []
        self.pending_cleanup = []
        self.shared_store = SharedStore.get_instance()

        self.monitor_comm = None
        if communication.is_internal_host_communication_supported():
            mpi_state = communication.MPIState.get_instance()
            
            monitor_group = mpi_state.comm.Get_group().Incl(mpi_state.monitor_processes)
            self.monitor_comm = mpi_state.comm.Create_group(monitor_group)
        

    def put(self, data_id, memory_len):
        first_index, last_index = self.free_memory.pop(memory_len)
        service_index, _ = self.free_service_indexes.pop(SharedStore.INFO_COUNT)
        if first_index is None:
            raise MemoryError("Overflow memory")
        if first_index is None:
            raise MemoryError("Overflow service memory")

        reservation_info = {
                "first_index": first_index,
                "last_index": last_index,
                "service_index": service_index,
            }
        
        self.reservation_info[data_id] = reservation_info
        return reservation_info
    
    def clear(self, data_id_list):
        cleanup_list = self.pending_cleanup + data_id_list
        self.pending_cleanup = []

        has_refs = array('B', [
            1 
            if data_id in self.reservation_info and self.shared_store.get_ref_number(self.reservation_info[data_id]["service_index"]) > 0 
            else 0
            for data_id in cleanup_list
        ])

        if self.monitor_comm is not None:
            all_refs = array('B', [1]*len(cleanup_list))
            self.monitor_comm.Allreduce(has_refs, all_refs, MPI.MAX)
        else:
            all_refs = has_refs

        for data_id, referers in zip(cleanup_list, all_refs):
            if referers == 0 and data_id in self.reservation_info:
                reservation_info = self.reservation_info[data_id]
                del self.reservation_info[data_id]
                self.deleted_ids.append(data_id)
                self.shared_store.delete_service_info(reservation_info["service_index"])
                self.free_service_indexes.push(reservation_info["service_index"], reservation_info["service_index"]+SharedStore.INFO_COUNT)
                self.free_memory.push(reservation_info["first_index"], reservation_info["last_index"])
            else:
                self.pending_cleanup.append(data_id)
        
