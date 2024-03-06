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
    GroupBy dimensions
    ==================
    Group the variables by selected dimensions.
    If no dimensions or wrong dimensions are displayed : run the node and then re-open the configuration panel.

    KNIME options implemented:
        - All
"""
import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


def group_by_dimensions(
    df: pd.DataFrame,
    groupby_dimensions: str,
    aggregation_method: str,
) -> pd.DataFrame:
    # Define constants
    GROUPBY_DIMENSION = groupby_dimensions

    # Compute column lists
    groups = [col for col in df.select_dtypes(exclude='float64').columns if col in GROUPBY_DIMENSION]
    aggregation_variables = [col for col in df.select_dtypes('float64').columns]

    # Group by
    df_groupby = df.groupby(by=groups, as_index=False, dropna=False)[aggregation_variables]
    if aggregation_method == "Sum":
        df = df_groupby.sum(min_count=1)
    elif aggregation_method == "Mean":
        df = df_groupby.mean()
    elif aggregation_method == "Maximum":
        df = df_groupby.max(numeric_only=True)
    elif aggregation_method == "Minimum":
        df = df_groupby.min(numeric_only=True)

    # Define OUTPUT
    return df
