# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""MPI communication interfaces."""

import time

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

import unidist.core.backends.mpi.core.common as common

from unidist.core.backends.mpi.core.serialization import (
    ComplexSerializer,
    SimpleSerializer,
)  # noqa: E402

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False)
from mpi4py import MPI  # noqa: E402


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


def mpi_send_buffer(comm, data_size, data, dest_rank):
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
    comm.send(data_size, dest=dest_rank)
    comm.Send([data, MPI.CHAR], dest=dest_rank)


def mpi_recv_buffer(comm, source_rank):
    """
    Receive buffer data.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Communication event source rank

    Returns
    -------
    object
        Buffer array or serialized object.
    """
    buf_size = comm.recv(source=source_rank)
    s_buffer = bytearray(buf_size)
    comm.Recv([s_buffer, MPI.CHAR], source=source_rank)
    return s_buffer


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


def send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank):
    """
    Send already serialized complex data.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    """
    # Send message pack bytestring
    mpi_send_buffer(comm, len(s_data), s_data, dest_rank)
    # Send the necessary metadata
    mpi_send_object(comm, len(raw_buffers), dest_rank)
    for i in range(0, len(raw_buffers)):
        mpi_send_buffer(comm, len(raw_buffers[i].raw()), raw_buffers[i], dest_rank)
    # TODO: do not send if raw_buffers is zero
    mpi_send_object(comm, len_buffers, dest_rank)


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

    Returns
    -------
    object
        A serialized msgpack data.
    list
        A list of pickle buffers.
    list
        A list of buffers amount for each object.
    """
    serializer = ComplexSerializer()
    # Main job
    s_data = serializer.serialize(data)
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # MPI comminucation
    send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)

    # For caching purpose
    return s_data, raw_buffers, len_buffers


def isend_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank):
    """
    Send serialized complex data.

    Non-blocking asynchronous interface.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    s_data: object
        A serialized msgpack data.
    raw_buffers : list
        A list of pickle buffers.
    len_buffers : list
        A list of buffers amount for each object.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.
    """
    handler_list = []

    # Send message pack bytestring
    h1 = mpi_isend_object(comm, len(s_data), dest_rank)
    h2 = mpi_isend_buffer(comm, s_data, dest_rank)
    handler_list.append((h1, None))
    handler_list.append((h2, s_data))

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
    object
        A serialized msgpack data.
    list
        A list of pickle buffers.
    list
        A list of buffers amount for each object.
    """
    handler_list = []

    serializer = ComplexSerializer()
    # Main job
    s_data = serializer.serialize(data)
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # Send message pack bytestring
    h_list = isend_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)
    handler_list.extend(h_list)

    return handler_list, s_data, raw_buffers, len_buffers


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
    # Recv main message pack buffer.
    # First MPI call uses busy wait loop to remove possible contention
    # in a long running data receive operations.
    buf_size = mpi_busy_wait_recv(comm, source_rank)
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
    deserializer = ComplexSerializer(raw_buffers, len_buffers)

    # Start unpacking
    return deserializer.deserialize(msgpack_buffer)


# Public API
# -----------


def send_complex_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``.

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


def send_operation_data(comm, operation_data, dest_rank, is_serialized=False):
    """
    Send data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``.
    Function works with already serialized data.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    operation_data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    is_serialized : bool
        `operation_data` is already serialized or not.

    Returns
    -------
    dict or None
        Serialization data for caching purpose.
    """
    if is_serialized:
        # Send already serialized data
        s_data = operation_data["s_data"]
        raw_buffers = operation_data["raw_buffers"]
        len_buffers = operation_data["len_buffers"]
        send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)
        return None
    else:
        # Serialize and send the data
        s_data, raw_buffers, len_buffers = send_complex_data(
            comm, operation_data, dest_rank
        )
        return {
            "s_data": s_data,
            "raw_buffers": raw_buffers,
            "len_buffers": len_buffers,
        }


def send_operation(
    comm, operation_type, operation_data, dest_rank, is_serialized=False
):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``.
    Function works with already serialized data.

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
    is_serialized : bool
        `operation_data` is already serialized or not.

    Returns
    -------
    dict or None
        Serialization data for caching purpose.
    """
    # Send operation type
    mpi_send_object(comm, operation_type, dest_rank)
    # Send operation data
    return send_operation_data(comm, operation_data, dest_rank, is_serialized)


def isend_complex_operation(
    comm, operation_type, operation_data, dest_rank, is_serialized=False
):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    Non-blocking asynchronous interface.
    The data is serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``.
    Function works with already serialized data.

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
    is_serialized : bool
        `operation_data` is already serialized or not.

    Returns
    -------
    dict or None
        Serialization data for caching purpose.
    """
    # Send operation type
    handler_list = []
    h1 = mpi_isend_object(comm, operation_type, dest_rank)
    handler_list.append((h1, None))

    # Send operation data
    if is_serialized:
        # Send already serialized data
        s_data = operation_data["s_data"]
        raw_buffers = operation_data["raw_buffers"]
        len_buffers = operation_data["len_buffers"]

        h2_list = isend_complex_data_impl(
            comm, s_data, raw_buffers, len_buffers, dest_rank
        )
        handler_list.extend(h2_list)

        return handler_list, None
    else:
        # Serialize and send the data
        h2_list, s_data, raw_buffers, len_buffers = isend_complex_data(
            comm, operation_data, dest_rank
        )
        handler_list.extend(h2_list)
        return handler_list, {
            "s_data": s_data,
            "raw_buffers": raw_buffers,
            "len_buffers": len_buffers,
        }


def send_remote_task_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``
    or ``unidist.core.backends.mpi.core.SimpleSerializer`` depending on the data size.

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
    mpi_send_object(comm, operation_type, dest_rank)

    # Check the message size and decide on serialization scheme
    if common.get_size(operation_data) < 256 * 1024:
        # Send the data type for serialization (simple or complex)
        mpi_send_object(comm, True, dest_rank)
        # Serialize and send the simple data
        s_operation_data = SimpleSerializer().serialize(operation_data)
        mpi_send_buffer(comm, len(s_operation_data), s_operation_data, dest_rank)
    else:
        mpi_send_object(comm, False, dest_rank)
        # Serialize and send the complex data
        send_complex_data(comm, operation_data, dest_rank)


def recv_remote_task_data(comm, source_rank):
    """
    Receive the data for remote operation.

    The data is de-serialized with ``unidist.core.backends.mpi.core.ComplexSerializer``
    or ``unidist.core.backends.mpi.core.SimpleSerializer`` depending on the data size.

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
    # Decide what deserialization scheme to use
    # TODO: check busy wait
    is_simple = comm.recv(source=source_rank)
    if is_simple:
        s_buffer = mpi_recv_buffer(comm, source_rank)
        return SimpleSerializer().deserialize(s_buffer)
    else:
        return recv_complex_data(comm, source_rank)


def send_serialized_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and serialized simple data.

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
    """
    # Send operation type
    mpi_send_object(comm, operation_type, dest_rank)
    # Send request details
    mpi_send_buffer(comm, len(operation_data), operation_data, dest_rank)


def recv_serialized_data(comm, source_rank):
    """
    Receive serialized data buffer.

    The data is de-serialized with ``unidist.core.backends.mpi.core.SimpleSerializer``.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data from.

    Returns
    -------
    object
        Received de-serialized data object from another MPI process.
    """
    s_buffer = mpi_recv_buffer(comm, source_rank)
    return SimpleSerializer().deserialize_pickle(s_buffer)
