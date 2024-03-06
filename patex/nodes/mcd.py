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
    MCD (MATCH, CALCULATE & DECOMPOSE)
    ==================================
    Match, Calculate and Decompose input data

    KNIME options implemented:
        -
"""
import numpy as np
import pandas as pd


def mcd(
    input_table_1: pd.DataFrame,
    input_table_2: pd.DataFrame,
    output_name: str,
    operation_selection: str,
    fill_value_bool: str = "No",
    fill_value: float = 0.0,
) -> pd.DataFrame:
    # Get common dimensions
    dimensions_1 = input_table_1.select_dtypes(['int64', 'int', 'object'])
    dimensions_2 = input_table_2.select_dtypes(['int64', 'int', 'object'])
    common_dimensions = []
    columns_2_only = list(input_table_2.columns)
    for col in dimensions_1:
        if col in dimensions_2:
            common_dimensions.append(col)
            columns_2_only.remove(col)
    columns_2_only = [input_table_2.columns.get_loc(col) for col in columns_2_only]

    # Inner or Left Outer Join (optional)
    if fill_value_bool == "No":
        how_var = "inner"
    elif fill_value_bool == "Inner Join":
        how_var = "inner"
    elif fill_value_bool == "Left [x] Outer Join":
        how_var = "left"
    else:
        raise ValueError("Wrong input for flow variable")

    # op, input_table = merge(input_table_1, input_table_2, how=how_var, on=common_dimensions)
    input_table = input_table_1.merge(input_table_2, how=how_var, on=common_dimensions)

    df = input_table

    # Select x and y
    var_xy = df.select_dtypes('float64').columns
    var_x = df[var_xy[0]]
    var_y = df[var_xy[1]]

    # Fill values (optional)
    if fill_value_bool == "Left [x] Outer Join":
        var_x.fillna(fill_value, inplace=True)
        var_y.fillna(fill_value, inplace=True)

    # Switch on operation
    options = {
        "x + y": fun_1,
        "x - y": fun_2,
        "y - x": fun_3,
        "x * y": fun_4,
        "x / y": fun_5,
        "y / x": fun_6,
        "1 + x * y": fun_7,
        "x * (1-y)": fun_8,
        "(1-x) * y": fun_9,
        "x ^ y": fun_10,
        "y ^ x": fun_11
    }
    fun = options[operation_selection]

    # Remove input data
    df = df.drop(df[var_xy].columns, axis=1)

    # Define OUTPUT
    df[output_name] = fun(var_x, var_y)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df


# Define operations available for MCD
def fun_1(var_x, var_y):
    return var_x + var_y


def fun_2(var_x, var_y):
    return var_x - var_y


def fun_3(var_x, var_y):
    return var_y - var_x


def fun_4(var_x, var_y):
    return var_x * var_y


def fun_5(var_x, var_y):
    return var_x / var_y


def fun_6(var_x, var_y):
    return var_y / var_x


def fun_7(var_x, var_y):
    return 1 + var_x * var_y


def fun_8(var_x, var_y):
    return var_x * (1 - var_y)


def fun_9(var_x, var_y):
    return (1 - var_x) * var_y


def fun_10(var_x, var_y):
    return var_x ** var_y


def fun_11(var_x, var_y):
    return var_y ** var_x


# Redefine the merge for this use case
# Nothing has changed but the fact that we expose the op object
# Source : merge from pandas (pd.core.reshape.merge)
# def merge(
#         left,
#         right,
#         how: str = "inner",
#         on=None,
#         left_on=None,
#         right_on=None,
#         left_index: bool = False,
#         right_index: bool = False,
#         sort: bool = False,
#         suffixes=("_x", "_y"),
#         copy: bool = True,
#         indicator: bool = False,
#         validate=None,
# ) -> "DataFrame":
#     op = _MergeOperation(
#         left,
#         right,
#         how=how,
#         on=on,
#         left_on=left_on,
#         right_on=right_on,
#         left_index=left_index,
#         right_index=right_index,
#         sort=sort,
#         suffixes=suffixes,
#         copy=copy,
#         indicator=indicator,
#         validate=validate,
#     )
#     return op, op.get_result()
