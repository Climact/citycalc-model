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
    MISSING VALUE NODE
    ============================
    KNIME options implemented:
        - Linear interpolation and Fixed, Previous, Next Value for double and integer. Column settings not implemented.
        Fillna not available for string and replace value can only be 0.
"""

import re
import logging

import numpy as np
import pandas as pd


def missing_value(
    df: pd.DataFrame,
    DTS_DT_O: list[str],
    FixedValue: str,
    missing_values_by_columns: list[str] = [],
    dimension_rx: str | None = None,
) -> pd.DataFrame:
    if any("String" in DTS[0] and "Fixed" in DTS[1] for DTS in DTS_DT_O):
        if dimension_rx is None:
            raise ValueError("`dimension_rx` must be specified for fixed strings")
    dimension_rx = re.compile(dimension_rx) if dimension_rx is not None else None

    for col in df:
        if df[col].dtype != np.number:
            if df[col].isna().sum() != 0:
                logging.debug("Some string columns contains Missing value. This case is not implemented in Missing Value node")

    for list in missing_values_by_columns:
        cols = list[0]
        option = list[1]
        if option == 'Previous':
            df[cols] = df[cols].fillna(method='ffill')

    for DTS in DTS_DT_O:
        if "DoNothing" in DTS[1]:
            pass
        elif "String" in DTS[0] and "Fixed" in DTS[1]:
            na_dimensions = [c for c in df.columns[df.isna().all()].to_list() if not dimension_rx.match(c)]
            string_columns = set(df.select_dtypes("object").columns.to_list() + na_dimensions)
            other_columns = [c for c in df.columns if c not in string_columns]
            df = pd.concat([df[other_columns],
                            df[string_columns].fillna((FixedValue))], axis=1)
        else:
            if "IntCell" in DTS[0]:
                numerics = ['int16', 'int32', 'int64']
            else:
                numerics = ['float16', 'float32', 'float64']
            cols = [col for col in df.columns if df[col].dtype in numerics]
            if "Fixed" in DTS[1]:  # Fixed and other than string
                FixedValue = float(FixedValue)
                df[cols] = df[cols].fillna((FixedValue))
            # Interpolation Option
            elif "Interpolation" in DTS[1]:
                df[cols] = df[cols].interpolate(method='linear')
            # Previous Value Option
            elif "Previous" in DTS[1]:
                df[cols] = df[cols].fillna(method='ffill')
            # Next Value Option
            elif "Next" in DTS[1]:
                df[cols] = df[cols].fillna(method='bfill')

    return df
