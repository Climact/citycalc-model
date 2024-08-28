# ----------------------------------------------------------------------------------------------------- #
# 2021, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    Filter dimension
    ================
    Filter some values from a given dimension with a selected mode.
    If no dimensions or wrong dimensions are displayed : run the node and then re-open the configuration panel.

    KNIME options implemented:
        - All
"""
import re

import numpy as np
import pandas as pd




def filter_dimension(
    df: pd.DataFrame,
    dimension: str,
    operation_selection: str | None = None,
    value_years: int | str | None = None,
    values_dimension: list | None = None,
    mode_selection: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if dimension == "Years":
        if operation_selection is None or value_years is None:
            raise ValueError("when dimension is `Years`, `operation_selection` and `value_years` must be specified")
        if values_dimension is not None or mode_selection is not None:
            raise ValueError("when dimension is `Years`, `values_dimension` and `mode_selection` must be None")
    else:
        if values_dimension is None or mode_selection is None:
            raise ValueError("when dimension is not `Years`, `values_dimension` and `mode_selection` must be specified")
        if operation_selection is not None or value_years is not None:
            raise ValueError("when dimension is not `Years`, `operation_selection` and `value_years` must be None")

    # Define constants
    DIMENSION = dimension
    if DIMENSION == 'Years':
        OPERATION_SELECTION = operation_selection
        # Change value for year parameter dynamically if linked to a flow variable
        VALUE_YEARS = int(value_years)

        # Compute mask
        if OPERATION_SELECTION == '=':
            mask = (df[DIMENSION] == VALUE_YEARS)
        elif OPERATION_SELECTION == '≠':
            mask = (df[DIMENSION] != VALUE_YEARS)
        elif OPERATION_SELECTION == '≥':
            mask = (df[DIMENSION] >= VALUE_YEARS)
        elif OPERATION_SELECTION == '>':
            mask = (df[DIMENSION] > VALUE_YEARS)
        elif OPERATION_SELECTION == '≤':
            mask = (df[DIMENSION] <= VALUE_YEARS)
        elif OPERATION_SELECTION == '<':
            mask = (df[DIMENSION] < VALUE_YEARS)

    else:
        VALUES_DIMENSION = values_dimension
        MODE_SELECTION = mode_selection

        # Convert VALUES_DIMENSION to numeric if DIMENSION column is numeric
        if np.issubdtype(df[DIMENSION].dtype, np.number):
            VALUES_DIMENSION = pd.to_numeric(VALUES_DIMENSION)

        # Compute mask
        if MODE_SELECTION == 'Exclude':
            mask = (~df[DIMENSION].isin(VALUES_DIMENSION))
        elif MODE_SELECTION == 'Include':
            mask = (df[DIMENSION].isin(VALUES_DIMENSION))
    
    # Filter out values
    return (df[mask], df[~mask])
