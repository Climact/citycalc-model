# ----------------------------------------------------------------------------------------------------- #
# 2020, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    ROW FILTER NODE
    ============================
    KNIME options implemented:
        - Everything except deep filtering, search with wild cards,
                    filtering by missing values, filtering by RowID
"""

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


def row_filter(
    df: pd.DataFrame,
    filter_type: str,
    include: bool,
    that_column: str | None = None,
    pattern: str | None = None,
    case_sensitive: bool | None = None,
    is_reg_exp: bool | None = None,
    lower_bound_bool: bool | None = None,
    lower_bound: float | None = None,
    upper_bound_bool: bool | None = None,
    upper_bound: float | None = None,
    start_index: int | None = None,
    end_index: int | None = None,
) -> pd.DataFrame:
    if filter_type == 'StringComp_RowFilter':
        pattern = pattern if pattern is not None else pattern
        that_column_type = df[that_column].dtype
        if df[that_column].dtype.name != "category": # category type does not exist in numpy dtypes
            pattern = np.dtype(that_column_type).type(pattern)  # Cast pattern into the right type for comparison

        if not is_reg_exp:
            if case_sensitive:
                if np.issubdtype(df[that_column].dtype, np.number):
                    raise RuntimeError("Numeric filtering is only available with case sensitive match")
                if include:
                    df = df[df[that_column].str.lower() == pattern.lower()]
                else:
                    df = df[df[that_column].str.lower() != pattern.lower()]
            else:
                if include:
                    df = df[df[that_column] == pattern]
                else:
                    df = df[df[that_column] != pattern]
        else:
            df_data_type = df[that_column].dtype.name
            if df_data_type == 'category':
                df_data_type = 'object'
            if np.issubdtype(df_data_type, np.number):
                raise RuntimeError("Numeric filtering is only available without normal match (no regex)")
            if case_sensitive:
                if include:
                    df = df.loc[df[that_column].str.contains("(?i)"+pattern)] # The (?i) in the regex pattern tells the re module to ignore case
                else:
                    df = df.loc[~df[that_column].str.contains("(?i)"+pattern)] # The (?i) in the regex pattern tells the re module to ignore case
            else:
                if include:
                    df = df.loc[df[that_column].str.contains(pattern)]
                else:
                    df = df.loc[~df[that_column].str.contains(pattern)]

    elif filter_type == 'RangeVal_RowFilter':
        that_column_type = df[that_column].dtype

        if that_column_type == 'float64' or that_column_type == 'float16' or that_column_type == 'float32':
            pass
        elif that_column_type == 'int32' or that_column_type == 'int64' or that_column_type == 'int8' or that_column_type == 'int16' or that_column_type == 'uint32' or that_column_type == 'uint64' or that_column_type == 'uint8' or that_column_type == 'uint16':
            pass
        else:
            raise RuntimeError("Column's datatype {} is neither float64, nor int32, nor int64!")

        lower_bound = np.dtype(that_column_type).type(lower_bound)  # Cast bound into the right type for comparison
        upper_bound = np.dtype(that_column_type).type(upper_bound)

        if include:
            if not lower_bound_bool:
                df = df[(df[that_column] <= upper_bound)]
            elif not upper_bound_bool:
                df = df[(df[that_column] >= lower_bound)]
            else:
                df = df[(df[that_column] >= lower_bound) & (df[that_column] <= upper_bound)]
        else:
            if not lower_bound_bool:
                df = df[(df[that_column] > upper_bound)]
            elif not upper_bound_bool:
                df = df[(df[that_column] < lower_bound)]
            else:
                df = df[(df[that_column] > upper_bound) | (df[that_column] < lower_bound)]

    elif filter_type == 'MissingVal_RowFilter':
        if include:
            df = df[df[that_column].isnull()]
        else:
            df = df.dropna(subset=[that_column], axis=0)

    elif filter_type == 'RowNumber_RowFilter':
        if include:
            if end_index == -1:
                df = df.iloc[list(range(start_index, len(df.index)))]
            else:
                df = df.iloc[list(range(start_index, end_index+1))]
        else:
            if end_index >= len(df.index)-1 or end_index == -1:

                if start_index == 0:
                    df = None
                else:
                    df = df.iloc[list(range(0, start_index))]
            else:

                if start_index == 0:
                    df = df.iloc[list(range(end_index+1, len(df.index)))]
                else:
                    df = df.iloc[list(range(0, start_index))+list(range(end_index+1, len(df.index)))]

    elif filter_type == 'RowID_RowFilter':
        raise RuntimeError("Include or exclude row by ID has not been implemented")

    return df
