# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Serialization interface"""

import importlib
import inspect
import sys
from collections.abc import KeysView
from mpi4py.MPI import memory

# Serialization libraries
if sys.version_info[1] < 8:  # check the minor Python version
    try:
        import pickle5 as pkl
    except ImportError:
        raise ImportError(
            "Missing dependency 'pickle5'. Use pip or conda to install it."
        ) from None
else:
    import pickle as pkl
import cloudpickle as cpkl
import msgpack
import gc  # msgpack optimization

from unidist.config.backends.common.envvars import PickleThreshold

# Pickle 5 protocol compatible types check
compatible_modules = ("pandas", "numpy")
available_modules = []
for module_name in compatible_modules:
    try:
        available_modules.append(importlib.import_module(module_name))
    except ModuleNotFoundError:
        pass


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
    return (
        inspect.isfunction(data)
        or inspect.isclass(data)
        or inspect.ismethod(data)
        or data.__class__.__module__ != "builtins"
        or isinstance(data, KeysView)
    )


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
    for module in available_modules:
        if module.__name__ == "pandas" and isinstance(
            data, (module.DataFrame, module.Series)
        ):
            return True
        elif module.__name__ == "numpy" and isinstance(data, module.ndarray):
            return True
    return False


class ComplexDataSerializer:
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
    Uses a combination of msgpack, cloudpickle and pickle libraries
    """

    # Minimum buffer size for serialization with pickle 5 protocol
    PICKLE_THRESHOLD = MpiPickleThreshold.get()

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
        pickle_buffer = memory(pickle_buffer)
        if len(pickle_buffer) >= self.PICKLE_THRESHOLD:
            self.buffers.append(pickle_buffer)
            self._callback_counter += 1
            return False
        return True

    def _dataframe_encode(self, frame):
        """
        Encode with pickle library using protocol 5.

        Parameters
        ----------
        data : object
            Pickle 5 serializable object (e.g. pandas DataFrame or NumPy array).

        Returns
        -------
        dict
            Dictionary with array of serialized bytes.
        """
        s_frame = pkl.dumps(frame, protocol=5, buffer_callback=self._buffer_callback)
        self.len_buffers.append(self._callback_counter)
        self._callback_counter = 0
        return {"__pickle5_custom__": True, "as_bytes": s_frame}

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
        return {"__cloud_custom__": True, "as_bytes": cpkl.dumps(obj)}

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
        return {"__pickle_custom__": True, "as_bytes": pkl.dumps(obj)}

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
        return msgpack.packb(data, default=self._encode_custom, strict_types=True)

    def _decode_custom(self, obj):
        """
        De-serialization hook for msgpack library.

        It decodes complex data types the library couldn`t handle.

        Parameters
        ----------
        obj : object
            Python object.
        """
        if "__cloud_custom__" in obj:
            return cpkl.loads(obj["as_bytes"])
        elif "__pickle_custom__" in obj:
            return pkl.loads(obj["as_bytes"])
        elif "__pickle5_custom__" in obj:
            frame = pkl.loads(obj["as_bytes"], buffers=self.buffers)
            del self.buffers[: self.len_buffers.pop(0)]
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
        unpacked_data = msgpack.unpackb(
            s_data, object_hook=self._decode_custom, strict_map_key=False
        )
        gc.enable()
        return unpacked_data


class SimpleDataSerializer:
    """
    Class for simple data serialization/de-serialization for MPI communication.

    Notes
    -----
    Uses cloudpickle and pickle libraries as separate APIs.
    """

    def serialize_cloudpickle(self, data):
        """
        Encode with a cloudpickle library.

        Parameters
        ----------
        obj : object
            Python object.

        Returns
        -------
        bytearray
            Array of serialized bytes.
        """
        return cpkl.dumps(data)

    def serialize_pickle(self, data):
        """
        Encode with a pickle library.

        Parameters
        ----------
        obj : object
            Python object.

        Returns
        -------
        bytearray
            Array of serialized bytes.
        """
        return pkl.dumps(data)

    def deserialize_cloudpickle(self, data):
        """
        De-serialization with cloudpickle library.

        Parameters
        ----------
        obj : bytearray
            Python object.

        Returns
        -------
        object
            Original reconstructed object.
        """
        return cpkl.loads(data)

    def deserialize_pickle(self, data):
        """
        De-serialization with pickle library.

        Parameters
        ----------
        obj : bytearray
            Python object.

        Returns
        -------
        object
            Original reconstructed object.
        """
        return pkl.loads(data)
