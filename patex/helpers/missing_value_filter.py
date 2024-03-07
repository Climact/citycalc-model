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
    Missing Value Filter Node
    =========================
    KNIME options implemented:
        - Manual filtering
        - Regex (case sensitive and insensitive) filtering
        - Enforce exclusion
        - Filter based on missing values
"""
import pandas as pd

from patex.utils import patternreshape


def missing_value_column_filter(
    df: pd.DataFrame,
    missing_threshold: float,
    type_of_pattern: str,
    columns_to_drop: list[str] = [],
    pattern: str | None = None,
) -> pd.DataFrame:
    if type_of_pattern == 'Regex':
        pattern = patternreshape(pattern, True)
        df = df.filter(regex=pattern, axis=1)
    else:
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(col, axis=1)
            #else:
            #    debug(
            #        f'The columns {col} does not exist and cannot be filtered')

    # Drop columns based on missing threshold
    return df.dropna(axis=1, thresh=missing_threshold)
