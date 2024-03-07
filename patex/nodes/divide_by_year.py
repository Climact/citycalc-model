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
    Divide by year
    ============
    Divide the selected variable by its value at selected year (by default: baseyear).

    KNIME options implemented:
        - All
"""
import numpy as np
import pandas as pd




def divide_year(
    df: pd.DataFrame,
    in_var: str,
    output_name: str,
    reference_year: int | str,
) -> pd.DataFrame:
    # Define constants
    VARIABLE = in_var
    YEAR = int(reference_year)
    OUTPUT_NAME = output_name

    # Select variable to lag
    dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
    kept_columns = dimensions + [VARIABLE]
    df_divided = df[kept_columns]
    df_divided = df_divided[df_divided[VARIABLE].notnull()]
    for col in df_divided.columns:
        if df_divided[col].isnull().all():
            del df_divided[col]
    used_dimensions = list(df_divided.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
    used_dimensions_without_years = list(df_divided.select_dtypes(['object']).columns)

    # Select values for reference year
    df_reference = df_divided[df_divided['Years'] == YEAR].copy()
    df_reference.rename(columns={VARIABLE: 'ref_year'}, inplace=True)
    df_reference = df_reference[used_dimensions_without_years + ['ref_year']]

    # Merge reference year value to original data
    df_divided = df_divided.merge(df_reference, how='left', on=used_dimensions_without_years)

    # Divide variable by reference year
    df_divided[OUTPUT_NAME] = df_divided[VARIABLE] / df_divided['ref_year']
    df_divided.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Remove unused columns
    df_divided = df_divided[used_dimensions + [OUTPUT_NAME]]

    return df_divided
