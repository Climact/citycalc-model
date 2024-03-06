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
    Lag Variable
    ============
    Lag a variable from one year row toward the future.

    KNIME options implemented:
        - All
"""
import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


def lag_variable(
    df: pd.DataFrame,
    in_var: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Define constants
    VARIABLE = in_var
    rx = re.compile('(.*)\[(.*)\]')
    groups = rx.match(VARIABLE)
    LAGGED_VARIABLE = f'{groups[1]}_lagged[{groups[2]}]'

    # Lag Years column
    df_years = pd.DataFrame(df['Years'].drop_duplicates().sort_values(ascending=False))
    df_years['Timestep'] = (df_years.shift(1) - df_years)
    df_years.dropna(subset=['Timestep'], inplace=True)

    # Select variable to lag
    dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
    kept_columns = dimensions + [VARIABLE]
    df_variable_to_lag = df[kept_columns]
    df_variable_to_lag = df_variable_to_lag[df_variable_to_lag[VARIABLE].notnull()]
    for col in df_variable_to_lag.columns:
        if df_variable_to_lag[col].isnull().all():
            del df_variable_to_lag[col]
    used_dimensions = list(df_variable_to_lag.select_dtypes(['object', 'int', 'int32', 'int64']).columns)

    # Apply Timestep to lagged variable
    df_lagged_variable = df_variable_to_lag.merge(df_years, on='Years')
    df_lagged_variable['Years'] = (df_lagged_variable['Years'] + df_lagged_variable['Timestep']).astype(int)

    # Rename variable as lagged
    df_lagged_variable.rename(columns={VARIABLE: LAGGED_VARIABLE}, inplace=True)

    # Merge lagged variable to original variable
    df_lagged_variable = df_variable_to_lag.merge(df_lagged_variable, on=used_dimensions, how='left')
    df_lagged_variable['Timestep'].fillna(0, inplace=True)

    # Fill missing value (start year) with current value
    df_lagged_variable[LAGGED_VARIABLE].fillna(df_lagged_variable[VARIABLE], inplace=True)

    # Create the outputs
    output_table_1 = df_lagged_variable[used_dimensions + [LAGGED_VARIABLE]]
    output_table_2 = df_lagged_variable[used_dimensions + ['Timestep']]

    return (output_table_1, output_table_2)
