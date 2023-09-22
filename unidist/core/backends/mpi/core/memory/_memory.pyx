# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

from libc.stdint cimport uint8_t
cimport memory

def parallel_memcopy(const uint8_t[:] src, uint8_t[:] dst, int memcopy_threads):
    """
    Multithreaded data copying between buffers.

    Parameters
    ----------
    src : uint8_t[:]
        Copied data.
    dst : uint8_t[:]
        Buffer for writing.
    memcopy_threads : int
        Number of threads to write.
    """
    with nogil:
        memory.parallel_memcopy(&dst[0],
                                &src[0],
                                len(src),
                                64,
                                memcopy_threads)
