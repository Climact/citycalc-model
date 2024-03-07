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
    TREE MERGE GROUP METANODE
    ===================
    KNIME options implemented:
        - Everything
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


def tree_merge_groups(
    df: pd.DataFrame,
    unit: str = "unit",
    aggregation_method: str = "Product",
    aggregation_remove: str = "true",
    aggregation_pattern: str = ".*",
    new_column_name: str = "new-column",
) -> pd.DataFrame:
    columns_used_dict = {}

    output_table = df
    pat = aggregation_pattern

    name_components = new_column_name.split(',')

    grouped = output_table.groupby(output_table.columns.str.extract(pat, expand=False), axis=1)
    for name, group in grouped:
        if len(name_components) == 1:
            column_name = new_column_name + "_" + name + "[" + unit + "]"
        elif len(name_components) == 2:
            column_name = name_components[0] + "_" + name + "_" + name_components[1] + "[" + unit + "]"
        if aggregation_method == "Sum":
            output_table[column_name] = output_table[group.columns].sum(axis=1)
        elif aggregation_method == "Product":
            output_table[column_name] = output_table[group.columns].prod(axis=1)

        if aggregation_remove == 'true':
            output_table = output_table.drop(group.columns, axis=1)

        columns_used_dict[column_name] = [df.columns.get_loc(c) for c in group.columns]

    # Variables for the update
    columns_copied_from_input = output_table.columns.intersection(df.columns)
    columns_copied_from_input_int = [df.columns.get_loc(c) for c in
                                          columns_copied_from_input]

    if max([len(i) for i in columns_used_dict.values()])==1:
        # Speed up things if resulting columns are all 1-1 with their input columns
        f = lambda x: x
    elif aggregation_method == "Sum":
        f = lambda x: x.sum(axis=1)
    elif aggregation_method == "Product":
        f = lambda x: x.prod(axis=1)

    return output_table
