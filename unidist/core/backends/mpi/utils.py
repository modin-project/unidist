# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities used to initialize MPI execution backend."""


def initialize_mpi():
    """Initialize the MPI execution backend."""
    from unidist.core.backends.mpi.core import init

    init()

def check_ndarray(ndarray):
    if not ndarray.flags.owndata:
        return True

def check_pandas_index(df_index):
    if df_index._is_multi:
        if any(check_ndarray(df_index.get_level_values(i)._data) for i in range(df_index.nlevels)):
            return True
    else:
        if check_ndarray(df_index._data):
            return True

def check_data_out_of_band(value):
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
            if any(block.is_view for block in value._mgr.blocks) \
                or any(check_pandas_index(df_index) for df_index in [value.index, value.columns]):
                return True
            return False
                
        if isinstance(value, pd.Series):
            if any(block.is_view for block in value._mgr.blocks) or check_pandas_index(value.index):
                return True
            return False
        
        # TODO: Add like blocks for other pandas classes
    except ImportError:
        pass