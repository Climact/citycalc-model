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
    COLUMN FILTER NODE
    ==================
    KNIME options implemented:
        Columns:
        - Manual filtering
        - Regex (case sensitive and insensitive) filtering
        - Enforce exclusion
"""

import pandas as pd

from patex.utils import patternreshape


def column_filter(
    df: pd.DataFrame,
    pattern: str | None = None,
    columns_to_drop: list[str] | None = None,
) -> pd.DataFrame:
    if (pattern is not None and columns_to_drop is not None) or (
        pattern is None and columns_to_drop is None
    ):
        raise ValueError(
            "Exactly one of `pattern` and `columns_to_drop` should be specified"
        )

    if pattern is not None:
        pattern = patternreshape(pattern, True)
        df_filtered = df.filter(regex=pattern, axis=1)
    else:
        df_filtered = df
        for col in columns_to_drop:
            if col in df_filtered.columns:
                df_filtered = df_filtered.drop(col, axis=1)

    # Variables for the update
    columns_copied_from_input = df_filtered.columns.intersection(df.columns)
    columns_copied_from_input_int = [df.columns.get_loc(c) for c in columns_copied_from_input]

    return df_filtered
