# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MPI execution backend."""


def initialize_mpi():
    """Initialize the MPI execution backend."""
    from unidist.core.backends.mpi.core import init

    init()


def check_ndarray(ndarray):
    """
    Check if the numpy.ndarray is occupying or owning the memory it is using.

    Returns
    -------
    bool
        ``True`` if the data is occupying memory.
    """
    return not ndarray.flags.owndata


def check_pandas_index(df_index):
    """
    Check if the pandas.Index is occupying or owning the memory it is using.

    Returns
    -------
    bool
        ``True`` if the data is occupying memory.
    """
    if df_index._is_multi:
        if any(
            check_ndarray(df_index.get_level_values(i)._data)
            for i in range(df_index.nlevels)
        ):
            return True
    else:
        if check_ndarray(df_index._data):
            return True
    return False


def check_data_out_of_band(value):
    """
    Check if the data is occupying or owning the memory it is using.

    Returns
    -------
    bool
        ``True`` if the data is occupying memory.

    Notes
    -----
    Only validation for numpy.ndarray, pandas.Dataframe and pandas.Series is currently supported.
    """
    # check numpy
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            if check_ndarray(value):
                return True
            return False
    except ImportError:
        pass

    # check pandas
    try:
        import pandas as pd

        if isinstance(value, pd.DataFrame):
            if any(block.is_view for block in value._mgr.blocks) or any(
                check_pandas_index(df_index)
                for df_index in [value.index, value.columns]
            ):
                return True
            return False

        if isinstance(value, pd.Series):
            if any(block.is_view for block in value._mgr.blocks) or check_pandas_index(
                value.index
            ):
                return True
            return False

        # TODO: Add like blocks for other pandas classes
    except ImportError:
        pass
    return False
