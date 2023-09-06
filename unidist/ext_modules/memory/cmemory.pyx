# distutils: language = c++

from libc.stdint cimport uint8_t
cimport memory

import time

def write_to(const uint8_t[:] inband, uint8_t[:] data, int memcopy_threads):
    t0 = time.perf_counter()
    with nogil:
        memory.parallel_memcopy(&data[0],
                                &inband[0],
                                len(inband),
                                64,
                                memcopy_threads)
    t1 = time.perf_counter()
    print(f'write_to: {t1 - t0}')