from libc.stdint cimport uint8_t, uintptr_t, int64_t

cdef extern from "memory.cpp" nogil:
    pass

# Declare the class with cdef
cdef extern from "memory.h" namespace "unidist" nogil:
    void parallel_memcopy(uint8_t* dst, 
                            const uint8_t* src, 
                            int64_t nbytes, 
                            uintptr_t block_size, 
                            int num_threads)