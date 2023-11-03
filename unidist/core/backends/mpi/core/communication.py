# Copyright (C) 2021-2023 Modin authors
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

from unidist.config import MpiBackoff
from unidist.core.backends.mpi.core.serialization import (
    SimpleDataSerializer,
    serialize_complex_data,
    deserialize_complex_data,
)
import unidist.core.backends.mpi.core.common as common

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402
from mpi4py.util import pkl5  # noqa: E402


# Logger configuration
logger = common.get_logger("communication", "communication.log")
is_logger_header_printed = False


def log_operation(op_type, status):
    """
    Log a communication between worker processes.

    Parameters
    ----------
    op_type : unidist.core.backends.mpi.core.common.Operation
        Operation type.
    status : mpi4py.MPI.Status
        Represents the status of a reception operation.
    """
    global is_logger_header_printed
    logger_op_name_len = 15
    logger_worker_count = MPIState.get_instance().global_size

    # write header on first worker
    if (
        not is_logger_header_printed
        and MPIState.get_instance().global_rank == MPIRank.FIRST_WORKER
    ):
        worker_ids_str = "".join([f"{i}\t" for i in range(logger_worker_count)])
        logger.debug(f'#{" "*logger_op_name_len}{worker_ids_str}')
        is_logger_header_printed = True

    # Write operation to log
    source_rank = status.Get_source()
    dest_rank = MPIState.get_instance().global_rank
    op_name = common.get_op_name(op_type)
    space_after_op_name = " " * (logger_op_name_len - len(op_name))
    space_before_arrow = ".   " * (min(source_rank, dest_rank))
    space_after_arrow = "   ." * (logger_worker_count - max(source_rank, dest_rank) - 1)
    arrow_line_str = abs(dest_rank - source_rank)
    # Right arrow if dest_rank > source_rank else left
    if dest_rank > source_rank:
        arrow = f'{".---"*arrow_line_str}>'
    else:
        arrow = f'<{arrow_line_str*"---."}'
    logger.debug(
        f"{op_name}:{space_after_op_name}{space_before_arrow}{arrow}{space_after_arrow}"
    )


class MPIState:
    """
    The class holding MPI information.

    Parameters
    ----------
    comm : mpi4py.MPI.Comm
        MPI communicator.

    Attributes
    ----------
    global_comm : mpi4py.MPI.Comm
        Global MPI communicator.
    host_comm : mpi4py.MPI.Comm
        MPI subcommunicator for the current host.
    global_rank : int
        Rank of a process.
    global_size : int
        Number of processes in the global communicator.
    host : str
        IP-address of the current host.
    topology : dict
        Dictionary, containing all ranks assignments by IP-addresses in
        the form: `{"node_ip0": {"host_rank": "global_rank", ...}, ...}`.
    host_by_rank : dict
        Dictionary containing IP addresses by rank.
    monitor_processes : list
        List of ranks that are monitor processes.
    workers : list
        List of ranks that are worker processes.
    """

    __instance = None

    def __init__(self, comm):
        # attributes get actual values when MPI is initialized in `init` function
        self.global_comm = comm
        self.global_rank = comm.Get_rank()
        self.global_size = comm.Get_size()
        self.host_comm = None
        if common.is_shared_memory_supported():
            self.host_comm = comm.Split_type(MPI.COMM_TYPE_SHARED)
        host_rank = (
            self.host_comm.Get_rank()
            if self.host_comm is not None
            else self.global_rank
        )
        self.host = socket.gethostbyname(socket.gethostname())
        # Get topology of MPI cluster.
        cluster_info = self.global_comm.allgather(
            (self.host, self.global_rank, host_rank)
        )
        self.topology = defaultdict(dict)
        self.host_by_rank = defaultdict(None)
        for host, global_rank, host_rank in cluster_info:
            self.topology[host][host_rank] = global_rank
            self.host_by_rank[global_rank] = host

        self.monitor_processes = [
            self.topology[host][MPIRank.MONITOR] for host in self.topology
        ]

        self.workers = []
        for host in self.topology:
            self.workers.extend(
                [
                    rank
                    for rank in self.topology[host].values()
                    if not self.is_root_process(rank)
                    and not self.is_monitor_process(rank)
                ]
            )

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

    def is_root_process(self, rank=None):
        """
        Check if the rank is root process.

        Parameters
        ----------
        rank : int, optional
            The rank to be checked.
            If ``None``, the current rank is to be checked.

        Returns
        -------
        bool
            True or False.
        """
        if rank is None:
            rank = self.global_rank
        return rank == MPIRank.ROOT

    def is_monitor_process(self, rank=None):
        """
        Check if the rank is a monitor process.

        Parameters
        ----------
        rank : int, optional
            The rank to be checked.
            If ``None``, the current rank is to be checked.

        Returns
        -------
        bool
            True or False.
        """
        if rank is None:
            rank = self.global_rank
        return rank in self.monitor_processes

    def get_monitor_by_worker_rank(self, rank=None):
        """
        Get the monitor process rank for the host that includes this rank.

        Parameters
        ----------
        rank : int, optional
            The global rank to search for a monitor process.

        Returns
        -------
        int
            Rank of a monitor process.
        """
        if self.host_comm is None:
            return MPIRank.MONITOR

        if rank is None:
            rank = self.global_rank
        host = self.host_by_rank[rank]
        if host is None:
            raise ValueError("Unknown rank of workers")

        return self.topology[host][MPIRank.MONITOR]


class MPIRank:
    """Class that describes rank assignment."""

    ROOT = 0
    MONITOR = 1
    FIRST_WORKER = 2


# ---------------------------- #
# Main communication utilities #
# ---------------------------- #


def mpi_send_operation(comm, op_type, dest_rank):
    """
    Send an operation type to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    op_type : unidist.core.backends.mpi.core.common.Operation
        An operation type to send.
    dest_rank : int
        Target MPI process to transfer data.

    Notes
    -----
    * This blocking send is used when we have to wait for completion of the communication,
    which is necessary for the pipeline to continue, or when the receiver is waiting for a result.
    Otherwise, use non-blocking ``mpi_isend_operation``.
    * The special tag is used for this communication, namely, ``common.MPITag.OPERATION``.
    """
    comm.send(op_type, dest=dest_rank, tag=common.MPITag.OPERATION)


def mpi_send_object(comm, data, dest_rank, tag=common.MPITag.OBJECT):
    """
    Send a Python object to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data to send.
    dest_rank : int
        Target MPI process to transfer data.
    tag : common.MPITag, default: common.MPITag.OBJECT
        Message tag.

    Notes
    -----
    * This blocking send is used when we have to wait for completion of the communication,
    which is necessary for the pipeline to continue, or when the receiver is waiting for a result.
    Otherwise, use non-blocking ``mpi_isend_object``.
    * The special tag is used for this communication, namely, ``common.MPITag.OBJECT``.
    """
    comm.send(data, dest=dest_rank, tag=tag)


def mpi_isend_operation(comm, op_type, dest_rank):
    """
    Send an operation type to another MPI rank in a non-blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    op_type : unidist.core.backends.mpi.core.common.Operation
        An operation type to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    object
        A handler to MPI_Isend communication result.

    Notes
    -----
    The special tag is used for this communication, namely, ``common.MPITag.OPERATION``.
    """
    return comm.isend(op_type, dest=dest_rank, tag=common.MPITag.OPERATION)


def mpi_isend_object(comm, data, dest_rank):
    """
    Send a Python object to another MPI rank in a non-blocking way.

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

    Notes
    -----
    * The special tag is used for this communication, namely, ``common.MPITag.OBJECT``.
    """
    return comm.isend(data, dest=dest_rank, tag=common.MPITag.OBJECT)


def mpi_recv_operation(comm):
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

    Notes
    -----
    The special tag is used for this communication, namely, ``common.MPITag.OPERATION``.
    """
    backoff = MpiBackoff.get()
    status = MPI.Status()
    source = MPI.ANY_SOURCE
    tag = common.MPITag.OPERATION
    while not comm.iprobe(source=source, tag=tag, status=status):
        time.sleep(backoff)
    source = status.source
    tag = status.tag
    op_type = comm.recv(buf=None, source=source, tag=tag, status=status)
    log_operation(op_type, status)
    return op_type, status.Get_source()


def mpi_iprobe_recv_object(comm, tag=common.MPITag.OBJECT):
    """
    Receive an object of a standard Python data type from any source.

    The source rank gets available from `iprobe`.

    Parameters
    ----------
    comm : mpi4py.MPI.Comm
        MPI communicator.
    tag : common.MPITag, default: common.MPITag.OBJECT
        Message tag.

    Returns
    -------
    object
        Received data from the source rank.
    int
        Source rank.
    """
    backoff = MpiBackoff.get()
    status = MPI.Status()
    source = MPI.ANY_SOURCE
    while not comm.iprobe(source=source, tag=tag, status=status):
        time.sleep(backoff)
    source = status.source
    data = comm.recv(source=source, tag=tag, status=status)
    return data, source


def mpi_recv_object(comm, source_rank):
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
    * De-serialization is a simple pickle.load in this case.
    * The special tag is used for this communication, namely, ``common.MPITag.OBJECT``.
    """
    return comm.recv(source=source_rank, tag=common.MPITag.OBJECT)


def mpi_send_buffer(comm, buffer, dest_rank, data_type=MPI.CHAR, buffer_size=None):
    """
    Send buffer object to another MPI rank in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    buffer : object
        Buffer object to send.
    dest_rank : int
        Target MPI process to transfer buffer.
    data_type : MPI.Datatype, default: MPI.CHAR
        MPI data type for sending data.
    buffer_size: int, default: None
        Buffer size in bytes. Send an additional message with a buffer size to prepare another process to receive if `buffer_size` is not None.

    Notes
    -----
    * This blocking send is used when we have to wait for completion of the communication,
    which is necessary for the pipeline to continue, or when the receiver is waiting for a result.
    Otherwise, use non-blocking ``mpi_isend_buffer``.
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    if buffer_size:
        comm.send(buffer_size, dest=dest_rank, tag=common.MPITag.OBJECT)
    comm.Send([buffer, data_type], dest=dest_rank, tag=common.MPITag.BUFFER)


def mpi_isend_buffer(comm, buffer_size, buffer, dest_rank):
    """
    Send buffer object to another MPI rank in a non-blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    buffer_size : int
        Buffer size in bytes.
    buffer : object
        Buffer object to send.
    dest_rank : int
        Target MPI process to transfer data.

    Returns
    -------
    object
        A handler to MPI_Isend communication result.

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    requests = []
    h1 = comm.isend(buffer_size, dest=dest_rank, tag=common.MPITag.OBJECT)
    requests.append((h1, None))
    h2 = comm.Isend([buffer, MPI.CHAR], dest=dest_rank, tag=common.MPITag.BUFFER)
    requests.append((h2, buffer))
    return requests


def mpi_recv_buffer(comm, source_rank, result_buffer=None):
    """
    Receive data buffer.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Communication event source rank.
    result_buffer : object, default: None
        The array to be filled. If `result_buffer` is None, the buffer size will be requested and the necessary buffer created.

    Returns
    -------
    object
        Array buffer or serialized object.

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    if result_buffer is None:
        buf_size = comm.recv(source=source_rank, tag=common.MPITag.OBJECT)
        result_buffer = bytearray(buf_size)
    comm.Recv(result_buffer, source=source_rank, tag=common.MPITag.BUFFER)
    return result_buffer


def mpi_busy_wait_recv(comm, source_rank):
    """
    Wait for receive operation result in a custom busy wait loop.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data.

    Returns
    -------
    object
        Received data.

    Notes
    -----
    The special tag is used for this communication, namely, ``common.MPITag.OBJECT``.
    """
    backoff = MpiBackoff.get()
    req_handle = comm.irecv(source=source_rank, tag=common.MPITag.OBJECT)
    while True:
        status, data = req_handle.test()
        if status:
            return data
        else:
            time.sleep(backoff)


# --------------------------------- #
# Communication operation functions #
# --------------------------------- #


def _send_complex_data_impl(comm, s_data, raw_buffers, dest_rank, info_package):
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
    dest_rank : int
        Target MPI process to transfer data.
    info_package : unidist.core.backends.mpi.core.common.MetadataPackage
        Required information to deserialize data on a receiver side.

    Notes
    -----
    The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    # wrap to dict for sending and correct deserialization of the object by the recipient
    comm.send(dict(info_package), dest=dest_rank, tag=common.MPITag.OBJECT_BLOCKING)
    with pkl5._bigmpi as bigmpi:
        comm.Send(bigmpi(s_data), dest=dest_rank, tag=common.MPITag.BUFFER)
        for sbuf in raw_buffers:
            comm.Send(bigmpi(sbuf), dest=dest_rank, tag=common.MPITag.BUFFER)


def send_complex_data(comm, data, dest_rank, is_serialized=False):
    """
    Send the data that consists of different user provided complex types, lambdas and buffers in a blocking way.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    is_serialized : bool, default: False
        `data` is already serialized or not.

    Returns
    -------
    dict
        Serialized data for caching purpose.

    Notes
    -----
    * This blocking send is used when we have to wait for completion of the communication,
    which is necessary for the pipeline to continue, or when the receiver is waiting for a result.
    Otherwise, use non-blocking ``isend_complex_data``.
    """
    if is_serialized:
        s_data = data["s_data"]
        raw_buffers = data["raw_buffers"]
        buffer_count = data["buffer_count"]
        # pop `data_id` out of the dict because it will be send as part of metadata package
        data_id = data.pop("id")
        serialized_data = data
    else:
        data_id = data["id"]
        serialized_data = serialize_complex_data(data)
        s_data = serialized_data["s_data"]
        raw_buffers = serialized_data["raw_buffers"]
        buffer_count = serialized_data["buffer_count"]

    info_package = common.MetadataPackage.get_local_info(
        data_id, len(s_data), [len(sbuf) for sbuf in raw_buffers], buffer_count
    )
    # MPI communication
    _send_complex_data_impl(comm, s_data, raw_buffers, dest_rank, info_package)

    # For caching purpose
    return serialized_data


def _isend_complex_data_impl(comm, s_data, raw_buffers, dest_rank, info_package):
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
    dest_rank : int
        Target MPI process to transfer data.
    info_package : unidist.core.backends.mpi.core.common.MetadataPackage
        Required information to deserialize data on a receiver side.

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    handlers = []
    # wrap to dict for sending and correct deserialization of the object by the recipient
    h1 = comm.isend(dict(info_package), dest=dest_rank, tag=common.MPITag.OBJECT)
    handlers.append((h1, None))
    with pkl5._bigmpi as bigmpi:
        h2 = comm.Isend(bigmpi(s_data), dest=dest_rank, tag=common.MPITag.BUFFER)
        handlers.append((h2, s_data))
        for sbuf in raw_buffers:
            h_sbuf = comm.Isend(bigmpi(sbuf), dest=dest_rank, tag=common.MPITag.BUFFER)
            handlers.append((h_sbuf, sbuf))

    return handlers


def isend_complex_data(comm, data, dest_rank, is_serialized=False):
    """
    Send the data that consists of different user provided complex types, lambdas and buffers in a non-blocking way.

    Non-blocking asynchronous interface.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    data : object
        Data object to send.
    dest_rank : int
        Target MPI process to transfer data.
    is_serialized : bool, default: False
        `operation_data` is already serialized or not.

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

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    if is_serialized:
        s_data = data["s_data"]
        raw_buffers = data["raw_buffers"]
        buffer_count = data["buffer_count"]
        # pop `data_id` out of the dict because it will be send as part of metadata package
        data_id = data.pop("id")
        info_package = common.MetadataPackage.get_local_info(
            data_id, len(s_data), [len(sbuf) for sbuf in raw_buffers], buffer_count
        )
    else:
        serialized_data = serialize_complex_data(data)
        s_data = serialized_data["s_data"]
        raw_buffers = serialized_data["raw_buffers"]
        buffer_count = serialized_data["buffer_count"]
        info_package = common.MetadataPackage.get_task_info(
            len(s_data), [len(sbuf) for sbuf in raw_buffers], buffer_count
        )

    # MPI communication
    handlers = _isend_complex_data_impl(
        comm, s_data, raw_buffers, dest_rank, info_package
    )

    return handlers, s_data, raw_buffers, buffer_count


def recv_complex_data(comm, source_rank, info_package):
    """
    Receive the data that may consist of different user provided complex types, lambdas and buffers.

    The data is de-serialized from received buffer.

    Parameters
    ----------
    comm : object
        MPI communicator object.
    source_rank : int
        Source MPI process to receive data from.
    info_package : unidist.core.backends.mpi.core.common.MetadataPackage
        Required information to deserialize data.

    Returns
    -------
    object
        Received data object from another MPI process.

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    msgpack_buffer = bytearray(info_package["s_data_len"])
    buffer_count = info_package["buffer_count"]
    raw_buffers = list(map(bytearray, info_package["raw_buffers_len"]))
    with pkl5._bigmpi as bigmpi:
        comm.Recv(bigmpi(msgpack_buffer), source=source_rank, tag=common.MPITag.BUFFER)
        for rbuf in raw_buffers:
            comm.Recv(bigmpi(rbuf), source=source_rank, tag=common.MPITag.BUFFER)

    return deserialize_complex_data(msgpack_buffer, raw_buffers, buffer_count)


# ---------- #
# Public API #
# ---------- #


def send_simple_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send an operation type and standard Python data types in a blocking way.

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
    * This blocking send is used when we have to wait for completion of the communication,
    which is necessary for the pipeline to continue, or when the receiver is waiting for a result.
    Otherwise, use non-blocking ``isend_simple_operation``.
    * Serialization of the data to be sent takes place just using ``pickle.dump`` in this case.
    * The special tags are used for this communication, namely,
    ``common.MPITag.OPERATION`` and ``common.MPITag.OBJECT``.
    """
    # Send operation type
    mpi_send_operation(comm, operation_type, dest_rank)
    # Send the details of a communication request
    mpi_send_object(comm, operation_data, dest_rank)


def isend_simple_operation(comm, operation_type, operation_data, dest_rank):
    """
    Send an operation type and standard Python data types in a non-blocking way.

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

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.

    Notes
    -----
    * Serialization of the data to be sent takes place just using ``pickle.dump`` in this case.
    * The special tags are used for this communication, namely,
    ``common.MPITag.OPERATION`` and ``common.MPITag.OBJECT``.
    """
    # Send operation type
    handlers = []
    h1 = mpi_isend_operation(comm, operation_type, dest_rank)
    handlers.append((h1, operation_type))
    # Send the details of a communication request
    h2 = mpi_isend_object(comm, operation_data, dest_rank)
    handlers.append((h2, operation_data))
    return handlers


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
        - `operation_data` is always serialized for data
        that has already been saved into the object store.
        - `operation_data` is always not serialized
        for sending a task or an actor (actor method).

    Returns
    -------
    list and dict
        Async handlers list and serialization data dict for caching purpose.

    Notes
    -----
    * The special tags are used for this communication, namely,
    ``common.MPITag.OPERATION``, ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    # Send operation type
    handlers = []
    h1 = mpi_isend_operation(comm, operation_type, dest_rank)
    handlers.append((h1, None))

    # Send operation data
    if is_serialized:
        # Send already serialized data
        s_data = operation_data["s_data"]
        raw_buffers = operation_data["raw_buffers"]
        buffer_count = operation_data["buffer_count"]
        # pop `data_id` out of the dict because it will be send as part of metadata package
        data_id = operation_data.pop("id")
        info_package = common.MetadataPackage.get_local_info(
            data_id, len(s_data), [len(sbuf) for sbuf in raw_buffers], buffer_count
        )
        h2_list = _isend_complex_data_impl(
            comm, s_data, raw_buffers, dest_rank, info_package
        )
        handlers.extend(h2_list)
    else:
        # Serialize and send the data
        h2_list, s_data, raw_buffers, buffer_count = isend_complex_data(
            comm, operation_data, dest_rank
        )
        handlers.extend(h2_list)
    return handlers, {
        "s_data": s_data,
        "raw_buffers": raw_buffers,
        "buffer_count": buffer_count,
    }


def isend_serialized_operation(comm, operation_type, operation_data, dest_rank):
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

    Returns
    -------
    list
        A list of pairs, ``MPI_Isend`` handler and associated data to send.

    Notes
    -----
    The special tags are used for this communication, namely,
    ``common.MPITag.OPERATION``, ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    handlers = []
    # Send operation type
    h1 = mpi_isend_operation(comm, operation_type, dest_rank)
    handlers.append((h1, operation_type))
    # Send the details of a communication request
    h2_list = mpi_isend_buffer(comm, len(operation_data), operation_data, dest_rank)
    handlers.extend(h2_list)
    return handlers


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

    Notes
    -----
    The special tags are used for this communication, namely,
    ``common.MPITag.OBJECT`` and ``common.MPITag.BUFFER``.
    """
    s_buffer = mpi_recv_buffer(comm, source_rank)
    return SimpleDataSerializer().deserialize_pickle(s_buffer)


def send_reserve_operation(comm, data_id, data_size):
    """
    Reserve shared memory for `data_id`.

    Parameters
    ----------
    data_id : unidist.core.backends.mpi.core.common.MpiDataID
        An ID to data.
    data_size : int
        Length of a required range in shared memory.

    Returns
    -------
    dict
        Reservation info about the allocated range in shared memory.
    """
    mpi_state = MPIState.get_instance()
    operation_type = common.Operation.RESERVE_SHARED_MEMORY

    operation_data = {
        "id": data_id,
        "size": data_size,
    }
    # We use a blocking send here because we have to wait for
    # completion of the communication, which is necessary for the pipeline to continue.
    send_simple_operation(
        comm,
        operation_type,
        operation_data,
        mpi_state.get_monitor_by_worker_rank(),
    )
    return mpi_recv_object(comm, mpi_state.get_monitor_by_worker_rank())
