# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""``DataID`` class functionality."""

import uuid


class DataID:
    """
    Class that holds unique identifier.

    In the case of Python backend this class holds an original object.

    Parameters
    ----------
    id_value : object
        Any comparable and hashable ID value.
    """

    def __init__(self, id_value=None):
        self._id = id_value if id_value is not None else uuid.uuid4().hex

    def __repr__(self):
        return f"DataID({self._id})"

    def __eq__(self, other):
        return hasattr(other, "_id") and self._id == other._id

    def __hash__(self):
        return hash(self._id)


def is_data_id(arg):
    """Check if argument is an instance of ``DataID``.

    Parameters
    ----------
    arg : object
        Object to check.

    Returns
    -------
    bool
        ``True`` if argument is an instance of ``DataID``.
    """
    return isinstance(arg, DataID)
