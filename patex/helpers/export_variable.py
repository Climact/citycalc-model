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
    EXPORT VARIABLE NODE
    ====================
    Allow to export one variable from the resulting data of a sub-module.

    KNIME options implemented:
        - all
"""
import pandas as pd


def export_variable(
    selected_variable: str,
    input_table: pd.DataFrame,
) -> pd.DataFrame:
    output_table = input_table

    kept_columns = []

    # list the columns of type string (dimensions)
    columns = output_table.columns
    for col in columns:
        if output_table[col].dtype == object:
            kept_columns.append(col)

    # add Years columns
    kept_columns.insert(2, 'Years')

    # add the selected variable
    kept_columns.insert(3, selected_variable)

    # drop rows with empty variable column
    output_table = output_table[output_table[selected_variable].notnull()]

    # select the given columns
    output_table = output_table[kept_columns]

    return output_table
