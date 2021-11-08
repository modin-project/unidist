# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""MPI communication interfaces."""

import mpi4py
mpi4py.rc(recv_mprobe=False)
from mpi4py import MPI

import time

from unidist.core.backends.mpi.core.serialization import MPISerializer

# Sleep time setting inside the busy wait loop
sleep_time = 0.0001

class MPIRank:
    """Class that describes ranks assignment."""
    ROOT = 0
    MONITOR = 1

def get_mpi_state():
    """
    Get the necessary MPI structures.

    Returns
    -------
    object
        An MPI communicator.
    int
        Current rank.
    int
        MPI processes number.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    world_size = comm.Get_size()
    return comm, rank, world_size

# Main communication utilities

def mpi_send_object(comm, data, dest_rank):
    """
    Send Python object to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data to send.
    dest_rank : int
        Target MPI process to transfer data.
    """
    comm.send(data, dest=dest_rank)

def mpi_isend_object(comm, data, dest_rank):
    """
    Send Python object to another MPI rank in a non-blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    object
        A handler to MPI_Isend communication result.
    """
    return comm.isend(data, dest=dest_rank)

def mpi_send_buffer(comm, data, dest_rank):
    """
    Send buffer object to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Buffer object to send.
    dest_rank : int
        Target MPI process to transfer data.
    """
    comm.Send([data, MPI.CHAR], dest=dest_rank)

def mpi_isend_buffer(comm, data, dest_rank):
    """
    Send buffer object to another MPI rank in a non-blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Buffer object to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    object
        A handler to MPI_Isend communication result.
    """
    return comm.Isend([data, MPI.CHAR], dest=dest_rank)

def mpi_busy_wait_recv(comm, source_rank):
    """
    Wait for receive operation result in a custom busy wait loop.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data.
    """
    req_handle = comm.irecv(source=source_rank)
    while True:
        status, data = req_handle.test()
        if status:
            return data
        else:
            time.sleep(sleep_time)

def recv_operation_type(comm):
    """
    Worker receive operation type interface.

    Busy waits to avoid contention. Receives data from any source.

    Parameters
    ----------
    comm : object
        MPI communicator object.

    Returns
    -------
    unidist.core.backends.mpi.core.common.Operation
        Operation type.
    int
        Source rank.
    """
    status = MPI.Status()
    req_handle = comm.irecv(source=MPI.ANY_SOURCE)
    while True:
        is_ready, op_type = req_handle.test(status=status)
        if is_ready:
            return op_type, status.Get_source()
        else:
            time.sleep(sleep_time)

# Communication operation functions
# ---------------------------------

def send_complex_data(comm, data, dest_rank):
    """
    Send the data that consists of different user provided complex types, lambdas and buffers.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    """
    serializer = MPISerializer()
    # Main job
    s_data = serializer.serialize(data)

    # Send message pack bytestring
    comm.send(len(s_data), dest=dest_rank)
    comm.Send([s_data, MPI.CHAR], dest=dest_rank)
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # Send the necessary metadata
    comm.send(len(raw_buffers), dest=dest_rank)
    for i in range(0, len(raw_buffers)):
        comm.send(len(raw_buffers[i].raw()), dest=dest_rank)
        comm.Send([raw_buffers[i], MPI.CHAR], dest=dest_rank)
    comm.send(len_buffers, dest=dest_rank)

def isend_complex_data(comm, data, dest_rank):
    """
    Send the data that consists of different user provided complex types, lambdas and buffers.

    Non-blocking asynchronous interface.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.
    """
    handler_list = []

    serializer = MPISerializer()
    # Main job
    s_data = serializer.serialize(data)

    # Send message pack bytestring
    h1 = mpi_isend_object(comm, len(s_data), dest_rank)
    h2 = mpi_isend_buffer(comm, s_data, dest_rank)
    handler_list.append((h1, None))
    handler_list.append((h2, s_data))
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # Send the necessary metadata
    h3 = mpi_isend_object(comm, len(raw_buffers), dest_rank)
    handler_list.append((h3, None))
    for i in range(0, len(raw_buffers)):
        h4 = mpi_isend_object(comm, len(raw_buffers[i].raw()), dest_rank)
        h5 = mpi_isend_buffer(comm, raw_buffers[i], dest_rank)
        handler_list.append((h4, None))
        handler_list.append((h5, raw_buffers[i]))
    h6 = mpi_isend_object(comm, len_buffers, dest_rank)
    handler_list.append((h6, len_buffers))

    return handler_list

def recv_complex_data(comm, source_rank):
    """
    Receive the data that may consist of different user provided complex types, lambdas and buffers.

    The data is de-serialized from received buffer.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received data object from another MPI process.
    """
    # Recv main message pack buffer
    buf_size = comm.recv(source=source_rank)
    msgpack_buffer = bytearray(buf_size)
    comm.Recv([msgpack_buffer, MPI.CHAR], source=source_rank)

    # Recv pickle buffers array for all complex data frames
    raw_buffers_size = comm.recv(source=source_rank)
    # Pre-allocate pickle buffers list
    raw_buffers = [None] * raw_buffers_size
    for i in range(raw_buffers_size):
        buf_size = comm.recv(source=source_rank)
        recv_buffer = bytearray(buf_size)
        comm.Recv([recv_buffer, MPI.CHAR], source=source_rank)
        raw_buffers[i] = recv_buffer
    # Recv len of buffers for each complex data frames
    len_buffers = comm.recv(source=source_rank)

    # Set the necessary metadata for unpacking
    deserializer = MPISerializer(raw_buffers, len_buffers)

    # Start unpacking
    return deserializer.deserialize(msgpack_buffer)

# Public API
#-----------

def send_complex_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.MPISerializer``.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    operation_type : ``unidist.core.backends.mpi.core.common.Operation``
        Operation message type.
    operation_data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    """
    # Send operation type
    comm.send(operation_type, dest=dest_rank)
    # Send complex dictionary data
    send_complex_data(comm, operation_data, dest_rank)

def isend_complex_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consists of different user provided complex types, lambdas and buffers.

    Non-blocking asynchronous interface. The data is serialized with ``unidist.core.backends.mpi.core.MPISerializer``.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    operation_type : ``unidist.core.backends.mpi.core.common.Operation``
        Operation message type.
    operation_data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.
    """
    handler_list = []
    # Send operation type
    h1 = mpi_isend_object(comm, operation_type, dest_rank)
    handler_list.append((h1, None))
    # Send complex dictionary data
    h2_list = isend_complex_data(comm, operation_data, dest_rank)
    handler_list.extend(h2_list)
    return handler_list

def recv_complex_operation(comm, source_rank):
    """
    Receive the data that may consist of different user provided complex types, lambdas and buffers.

    The data is de-serialized from received buffer.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received data object from another MPI process.
    """
    return recv_complex_data(comm, source_rank)

def send_simple_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and Python standard data types.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    operation_type : unidist.core.backends.mpi.core.common.Operation
        Operation message type.
    operation_data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.

    Notes
    -----
    Serialization is a simple pickle.dump in this case.
    """
    # Send operation type
    mpi_send_object(comm, operation_type, dest_rank)
    # Send request details
    mpi_send_object(comm, operation_data, dest_rank)

def recv_simple_operation(comm, source_rank):
    """
    Receive an object of a standard Python data type.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received data object from another MPI process.

    Notes
    -----
    De-serialization is a simple pickle.load in this case
    """
    return comm.recv(source=source_rank)
