..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

MPI communication API
"""""""""""""""""""""

MPI communication routines for controller/worker data interchange.

API
===

Simple operations involve send/receive objects of standard Python data types.
`mpi4py` library handles serialization with pickle library by default.

.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_send_object
.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_isend_object
.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_send_buffer
.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_isend_buffer
.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_recv_buffer
.. autofunction:: unidist.core.backends.mpi.core.communication.send_simple_operation
.. autofunction:: unidist.core.backends.mpi.core.communication.isend_simple_operation
.. autofunction:: unidist.core.backends.mpi.core.communication.recv_simple_operation

Complex operations involve send/receive objects of custom data types, functions and classes with native buffers support.
Several levels of serialization handle this case, including `msgpack`, `cloudpickle` and `pickle` libraries.
`pickle` library uses protocol 5 for out-of-band buffers serialization for performance reasons.
:py:func:`~unidist.core.backends.mpi.core.communication.isend_complex_operation` is an asynchronous interface for sending data.

.. autofunction:: unidist.core.backends.mpi.core.communication.send_complex_data
.. autofunction:: unidist.core.backends.mpi.core.communication.isend_complex_data
.. autofunction:: unidist.core.backends.mpi.core.communication.recv_complex_data
.. autofunction:: unidist.core.backends.mpi.core.communication.isend_complex_operation

Complex operations as above, but operating with a bytearray of already serialized data.

.. autofunction:: unidist.core.backends.mpi.core.communication.isend_serialized_operation
.. autofunction:: unidist.core.backends.mpi.core.communication.recv_serialized_data

To reduce possible contention, MPI communication module supports custom receive data functions with a busy-wait loop underneath.

.. autofunction:: unidist.core.backends.mpi.core.communication.mpi_busy_wait_recv
.. autofunction:: unidist.core.backends.mpi.core.communication.recv_operation_type
