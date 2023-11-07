# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

from libc.stdint cimport uint8_t, int64_t

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

def fill(int64_t[:] buff, int64_t value):
    """
    Fill a given buffer with a given value.

    Parameters
    ----------
    buff : int64_t[:]
        Original data.
    value : int64_t
        Value to fill.
    """
    with nogil:
        memory.fill(&buff[0], len(buff), value)
