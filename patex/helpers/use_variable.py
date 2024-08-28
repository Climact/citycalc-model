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
    USE VARIABLE NODE
    =================
    Allow to choose which variable will be used from a given dataset.

    KNIME options implemented:
        - all
"""
import pandas as pd


def use_variable(
    input_table: pd.DataFrame,
    selected_variable: str,
) -> pd.DataFrame:
    # Copy input to output
    output_table = input_table

    # Force Years to be integer
    if "Years" in output_table.columns:
        output_table["Years"] = output_table["Years"].astype('int')

    # Select dimensions
    dimensions = list(output_table.select_dtypes(['object', 'int', 'int32', 'int64']).columns)

    # Add the selected variable
    kept_columns = dimensions + [selected_variable]

    # Select the given columns
    output_table = output_table[kept_columns]

    # Drop rows with empty variable column
    output_table = output_table[output_table[selected_variable].notnull()]

    # Drop empty dimension columns
    if output_table.empty:
        return output_table
    
    dropped_columns = []
    for col in output_table.columns:
        if output_table[col].isnull().all():
            dropped_columns.append(col)
            del output_table[col]

    return output_table
