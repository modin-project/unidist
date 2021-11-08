# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Serialization interface"""

import inspect
import sys

# Serialization libraries
if sys.version_info[1] < 8: # check the minor Python version
    import pickle5 as pkl
else:
    import pickle as pkl
import cloudpickle as cpkl
import msgpack
import gc # msgpack optimization

# Handle special case
import pandas as pd

def is_cpkl_serializable(data):
    """
    Check if the data should be serialized with cloudpickle.

    Parameters
    ----------
    data : object
        Python object

    Returns
    -------
    bool
        ``True` if the data should be serialized with cloudpickle library.
    """
    return inspect.isfunction(data) or inspect.isclass(data) or inspect.ismethod(data)

def is_pickle5_serializable(data):
    """
    Check if the data should be serialized with pickle 5 protocol.

    Parameters
    ----------
    data : object
        Python object.

    Returns
    -------
    bool
        ``True`` if the data should be serialized with pickle using protocol 5 (out-of-band data).
    """
    return isinstance(data, pd.DataFrame)

class MPISerializer:
    """
    Class for data serialization/de-serialization for MPI comminication.

    Parameters
    ----------
    buffers : list, default: None
        A list of ``PickleBuffer`` objects for data decoding.
    len_buffers : list, default: None
        A list of buffer sizes for data decoding.

    Notes
    -----
    Uses msgpack, cloudpickle and pickle libraries
    """

    def __init__(self, buffers=None, len_buffers=None):
        self.buffers = buffers if buffers else []
        self.len_buffers = len_buffers if len_buffers else []
        self._callback_counter = 0

    def _buffer_callback(self, pickle_buffer):
        """
        Callback for pickle protocol 5 out-of-band data buffers collection.

        Parameters
        ----------
        pickle_buffer: pickle.PickleBuffer
            Pickle library buffer wrapper.
        """
        self.buffers.append(pickle_buffer)
        self._callback_counter += 1
        return False

    def _dataframe_encode(self, frame):
        """
        Encode ``pandas.DataFrame`` with pickle library using protocol 5.

        Parameters
        ----------
        frame : pandas.DataFrame
            Pandas ``DataFrame`` object.

        Returns
        -------
        dict
            Dictionary with array of serialized bytes.
        """
        s_frame = pkl.dumps(frame, protocol=5, buffer_callback=self._buffer_callback)
        self.len_buffers.append(self._callback_counter)
        self._callback_counter = 0
        return {'__dataframe_custom__': True, 'as_bytes': s_frame}

    def _cpkl_encode(self, obj):
        """
        Encode with cloudpickle library.

        Parameters
        ----------
        obj : object
            Python object.

        Returns
        -------
        dict
            Dictionary with array of serialized bytes.
        """
        return {'__cloud_custom__': True, 'as_bytes': cpkl.dumps(obj)}

    def _pkl_encode(self, obj):
        """
        Encode with pickle library.

        Parameters
        ----------
        obj : object
            Python object.

        Returns
        -------
        dict
            Dictionary with array of serialized bytes.
        """
        return {'__pickle_custom__': True, 'as_bytes': pkl.dumps(obj)}

    def _encode_custom(self, obj):
        """
        Serialization hook for msgpack library.

        It encodes complex data types the library couldn`t handle.

        Parameters
        ----------
        obj : object
            Python object.
        """
        if is_pickle5_serializable(obj):
            return self._dataframe_encode(obj)
        elif is_cpkl_serializable(obj):
            return self._cpkl_encode(obj)
        else:
            return self._pkl_encode(obj)

    def serialize(self, data):
        """
        Serialize data to a byte array.

        Parameters
        ----------
        data : object
            Data to serialize.

        Notes
        -----
        Uses msgpack, cloudpickle and pickle libraries.
        """
        return msgpack.packb(data, default=self._encode_custom)

    def _decode_custom(self, obj):
        """
        De-serialization hook for msgpack library.

        It decodes complex data types the library couldn`t handle.

        Parameters
        ----------
        obj : object
            Python object.
        """
        if '__cloud_custom__' in obj:
            return cpkl.loads(obj["as_bytes"])
        elif '__pickle_custom__' in obj:
            return pkl.loads(obj["as_bytes"])
        elif '__dataframe_custom__' in obj:
            frame = pkl.loads(obj["as_bytes"], buffers=self.buffers)
            del self.buffers[:self.len_buffers.pop(0)]
            return frame
        else:
            return obj

    def deserialize(self, s_data):
        """
        De-serialize data from a bytearray.

        Parameters
        ----------
        s_data : bytearray
            Data to de-serialize.

        Notes
        -----
        Uses msgpack, cloudpickle and pickle libraries.
        """
        gc.disable()  # Performance optimization for msgpack
        unpacked_data = msgpack.unpackb(s_data, object_hook=self._decode_custom)
        gc.enable()
        return unpacked_data
