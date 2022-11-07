# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""MPI communication interfaces."""

from collections import defaultdict
import socket
import time

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

from unidist.config import CpuCount
from unidist.core.backends.mpi.core.serialization import (
    ComplexDataSerializer,
    SimpleDataSerializer,
)
import unidist.core.backends.mpi.core.common as common

logger = common.get_logger("communication", "communication.log")
# Logger configuration
logger_worker_count = CpuCount.get() + 2
logger_opp_name_len = 15
logger_tab_len = 4
worker_ids_str = "".join(["{}\t".format(i) for i in range(logger_worker_count)])
logger.debug(f'#{" "*logger_opp_name_len}{worker_ids_str}')

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402


# Sleep time setting inside the busy wait loop
sleep_time = 0.0001


class MPIState:
    """
    The class holding MPI information.

    Parameters
    ----------
    comm : mpi4py.MPI.Comm
        MPI communicator.
    rank : int
        Rank of a process.
    world_sise : int
        Number of processes.
    """

    __instance = None

    def __init__(self, comm, rank, world_sise):
        # attributes get actual values when MPI is initialized in `init` function
        self.comm = comm
        self.rank = rank
        self.world_size = world_sise

    @classmethod
    def get_instance(cls, *args):
        """
        Get instance of this class.

        Parameters
        ----------
        *args : tuple
            Positional arguments to create the instance.
            See the constructor's docstring on the arguments.

        Returns
        -------
        MPIState
        """
        if cls.__instance is None and args:
            cls.__instance = MPIState(*args)
        return cls.__instance


class MPIRank:
    """Class that describes ranks assignment."""

    ROOT = 0
    MONITOR = 1


def get_topology():
    """
    Get topology of MPI cluster.

    Returns
    -------
    dict
        Dictionary, containing workers ranks assignments by IP-addresses in
        the form: `{"node_ip0": [rank_2, rank_3, ...], "node_ip1": [rank_i, ...], ...}`.
    """
    mpi_state = MPIState.get_instance()
    comm = mpi_state.comm
    rank = mpi_state.rank

    hostname = socket.gethostname()
    host = socket.gethostbyname(hostname)
    cluster_info = comm.allgather((host, rank))
    topology = defaultdict(list)

    for host, rank in cluster_info:
        if rank not in [MPIRank.ROOT, MPIRank.MONITOR]:
            topology[host].append(rank)

    return dict(topology)


# ---------------------------- #
# Main communication utilities #
# ---------------------------- #


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


def mpi_send_buffer(comm, buffer_size, buffer, dest_rank):
    """
    Send buffer object to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    buffer_size : int
        Buffer size in bytes.
    buffer : object
        Buffer object to send.
    dest_rank : int
        Target MPI process to transfer buffer.
    """
    comm.send(buffer_size, dest=dest_rank)
    comm.Send([buffer, MPI.CHAR], dest=dest_rank)


def mpi_recv_buffer(comm, source_rank):
    """
    Receive data buffer.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Communication event source rank.

    Returns
    -------
    object
        Array buffer or serialized object.
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
            # Write opperation to log
            source_rank = status.Get_source()
            dest_rank = MPIState.get_instance().rank
            opp_name = common.get_opp_name(op_type)
            space_after_opp_name = " " * (logger_opp_name_len - len(opp_name))
            space_before_arrow = ".   " * (min(source_rank, dest_rank))
            space_after_arrow = "   ." * (
                logger_worker_count - max(source_rank, dest_rank) - 1
            )
            arrow_line_str = abs(dest_rank - source_rank)
            # Right arrow if dest_rank > source_rank else left
            if dest_rank > source_rank:
                arrow = f'{".---"*arrow_line_str}>'
            else:
                arrow = f'<{arrow_line_str*"---."}'
            logger.debug(
                f"{opp_name}:{space_after_opp_name}{space_before_arrow}{arrow}{space_after_arrow}"
            )

            return op_type, status.Get_source()
        else:
            time.sleep(sleep_time)


# --------------------------------- #
# Communication operation functions #
# --------------------------------- #


def _send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank):
    """
    Send already serialized complex data.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    s_data : bytearray
        Serialized data as bytearray.
    raw_buffers : list
        Pickle buffers list, out-of-band data collected with pickle 5 protocol.
    len_buffers : list
        Size of each buffer from `raw_buffers` list.
    dest_rank : int
        Target MPI process to transfer data.
    """
    # Send message pack bytestring
    mpi_send_buffer(comm, len(s_data), s_data, dest_rank)
    # Send the necessary metadata
    mpi_send_object(comm, len(raw_buffers), dest_rank)
    for raw_buffer in raw_buffers:
        mpi_send_buffer(comm, len(raw_buffer.raw()), raw_buffer, dest_rank)
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
    serializer = ComplexDataSerializer()
    # Main job
    s_data = serializer.serialize(data)
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # MPI comminucation
    _send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)

    # For caching purpose
    return s_data, raw_buffers, len_buffers


def _isend_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank):
    """
    Send serialized complex data.

    Non-blocking asynchronous interface.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    s_data : object
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
    handlers = []

    # Send message pack bytestring
    h1 = mpi_isend_object(comm, len(s_data), dest_rank)
    h2 = mpi_isend_buffer(comm, s_data, dest_rank)
    handlers.append((h1, None))
    handlers.append((h2, s_data))

    # Send the necessary metadata
    h3 = mpi_isend_object(comm, len(raw_buffers), dest_rank)
    handlers.append((h3, None))
    for raw_buffer in raw_buffers:
        h4 = mpi_isend_object(comm, len(raw_buffer.raw()), dest_rank)
        h5 = mpi_isend_buffer(comm, raw_buffer, dest_rank)
        handlers.append((h4, None))
        handlers.append((h5, raw_buffer))
    # TODO: do not send if raw_buffers is zero
    h6 = mpi_isend_object(comm, len_buffers, dest_rank)
    handlers.append((h6, len_buffers))

    return handlers


def _isend_complex_data(comm, data, dest_rank):
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
    handlers = []

    serializer = ComplexDataSerializer()
    # Main job
    s_data = serializer.serialize(data)
    # Retrive the metadata
    raw_buffers = serializer.buffers
    len_buffers = serializer.len_buffers

    # Send message pack bytestring
    handlers.extend(
        _isend_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)
    )

    return handlers, s_data, raw_buffers, len_buffers


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
    deserializer = ComplexDataSerializer(raw_buffers, len_buffers)

    # Start unpacking
    return deserializer.deserialize(msgpack_buffer)


# ---------- #
# Public API #
# ---------- #


def send_complex_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexDataSerializer``.

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
    Send an operation and standard Python data types.

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
    Send data that consists of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexDataSerializer``.
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
        Serialization data for caching purpose or nothing.

    Notes
    -----
    Function returns ``None`` if `operation_data` is already serialized,
    otherwise ``dict`` containing data serialized in this function.
    """
    if is_serialized:
        # Send already serialized data
        s_data = operation_data["s_data"]
        raw_buffers = operation_data["raw_buffers"]
        len_buffers = operation_data["len_buffers"]
        _send_complex_data_impl(comm, s_data, raw_buffers, len_buffers, dest_rank)
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
    Send operation and data that consists of different user provided complex types, lambdas and buffers.

    The data is serialized with ``unidist.core.backends.mpi.core.ComplexDataSerializer``.
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

    Notes
    -----
    Function returns ``None`` if `operation_data` is already serialized,
    otherwise ``dict`` containing data serialized in this function.
    """
    # Send operation type
    mpi_send_object(comm, operation_type, dest_rank)
    # Send operation data
    return send_operation_data(comm, operation_data, dest_rank, is_serialized)


def isend_complex_operation(
    comm, operation_type, operation_data, dest_rank, is_serialized=False
):
    """
    Send operation and data that consists of different user provided complex types, lambdas and buffers.

    Non-blocking asynchronous interface.
    The data is serialized with ``unidist.core.backends.mpi.core.ComplexDataSerializer``.
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
    dict and dict or dict and None
        Async handlers and serialization data for caching purpose.

    Notes
    -----
    Function always returns a ``dict`` containing async handlers to the sent MPI operations.
    In addition, ``None`` is returned if `operation_data` is already serialized,
    otherwise ``dict`` containing data serialized in this function.
    """
    # Send operation type
    handlers = []
    h1 = mpi_isend_object(comm, operation_type, dest_rank)
    handlers.append((h1, None))

    # Send operation data
    if is_serialized:
        # Send already serialized data
        s_data = operation_data["s_data"]
        raw_buffers = operation_data["raw_buffers"]
        len_buffers = operation_data["len_buffers"]

        h2_list = _isend_complex_data_impl(
            comm, s_data, raw_buffers, len_buffers, dest_rank
        )
        handlers.extend(h2_list)

        return handlers, None
    else:
        # Serialize and send the data
        h2_list, s_data, raw_buffers, len_buffers = _isend_complex_data(
            comm, operation_data, dest_rank
        )
        handlers.extend(h2_list)
        return handlers, {
            "s_data": s_data,
            "raw_buffers": raw_buffers,
            "len_buffers": len_buffers,
        }


def send_remote_task_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send operation and data that consist of different user provided complex types, lambdas and buffers.

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
    # Serialize and send the complex data
    send_complex_data(comm, operation_data, dest_rank)


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

    The data is de-serialized with ``unidist.core.backends.mpi.core.SimpleDataSerializer``.

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
    return SimpleDataSerializer().deserialize_pickle(s_buffer)
