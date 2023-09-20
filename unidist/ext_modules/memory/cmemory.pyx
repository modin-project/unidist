# distutils: language = c++

from libc.stdint cimport uint8_t
cimport memory

def parallel_memcopy(const uint8_t[:] inband, uint8_t[:] data, int memcopy_threads):
    with nogil:
        memory.parallel_memcopy(&data[0],
                                &inband[0],
                                len(inband),
                                64,
                                memcopy_threads)
