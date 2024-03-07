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
    GROUP BY NODE
    ============================
    KNIME options implemented:
        - All
    CAVEATS
        - Doesn't always work correctly with missing values!
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


def group_by(
    df: pd.DataFrame,
    to_group: list[str],
    pattern: list[str],
    pattern_aggregation_method: list[str],
    method_list_manual: list[str],
    aggregation_list_manual: list[str],
    to_aggregate_manual: list[str],
    name_policy: str,
) -> pd.DataFrame:
    # ------------------------------------------------------
    # Grouping Dataframe
    # ------------------------------------------------------
    grouped = df.groupby(to_group)

    # ------------------------------------------------------
    # Selecting Aggregation column selection type
    # ------------------------------------------------------

    # Implemented functions
    aggr_rename_dict = {'Mean': 'mean', 'Sum_V2.5.2': 'sum', 'First': 'first', 'Count': 'count', "Maximum": 'max', "Minimum": 'min'}
    aggr_dict_pattern = {}
    aggregation_list_pattern = []
    method_list_pattern = []

    # PATTERN based Aggregation columns
    for i,pat in enumerate(pattern):
        grouped_columns = df.filter(regex=pat)
        to_aggregate_pattern = list(grouped_columns.columns)
        for col in to_aggregate_pattern:
            if col not in to_group:
                aggr_dict_pattern[col] = aggr_rename_dict[pattern_aggregation_method[i]]
                aggregation_list_pattern.append(col)
                method_list_pattern.append(pattern_aggregation_method[i])


    # Compute the above value to create a dictionary
    aggr_methods_renamed = [aggr_rename_dict.get(item, item) for item in
                            method_list_manual]  # rename the list of aggregation method using the above dictionary
    aggr_dict_manual = dict(zip(to_aggregate_manual, aggr_methods_renamed))



    # ------------------------------------------------------
    # Grouping dictionary of columns : {'column' : 'method'}
    # ------------------------------------------------------
    aggr_dict = {**aggr_dict_manual,**aggr_dict_pattern}
    method_list = method_list_manual + method_list_pattern
    aggregation_list = aggregation_list_manual + aggregation_list_pattern

    try:
        df_out = grouped.agg(aggr_dict).reset_index()
    except AttributeError as error:
        raise error

    # ------------------------------------------------------
    # Renaming columns : {'column' : 'new name'}
    # ------------------------------------------------------
    if name_policy == "Aggregation method (column name)":
        aggr_rename_col_dict = {'Mean': 'Mean', 'Sum_V2.5.2': 'Sum', 'First': 'First*', 'Count': 'Count*'}
        aggr_methods_col = [aggr_rename_col_dict.get(item, item) for item in
                            method_list]  # rename the list of aggregation method using the above dictonary
        aggr_col_dict = dict(zip(aggregation_list, aggr_methods_col))

        aggr_col_dict = {x: y + '(' + x + ')' for x, y in aggr_col_dict.items()}

        df_out.rename(columns=aggr_col_dict, inplace=True)
        aggregation_list = [aggr_col_dict.get(item, item) for item in
                            aggregation_list]  # rename the list of aggregation method using the above dictonary
    elif name_policy == "Column name (aggregation method)":
        aggr_rename_col_dict = {'Mean': 'Mean', 'Sum_V2.5.2': 'Sum', 'First': 'First', 'Count': 'Count*'}
        aggr_methods_col = [aggr_rename_col_dict.get(item, item) for item in
                            method_list]  # rename the list of aggregation method using the above dictonary
        aggr_col_dict = dict(zip(aggregation_list, aggr_methods_col))
        aggr_col_dict = {x: x + ' (' + y + ')' for x, y in aggr_col_dict.items()}

        df_out.rename(columns=aggr_col_dict, inplace=True)
        aggregation_list = [aggr_col_dict.get(item, item) for item in
                            aggregation_list]  # rename the list of aggregation method using the above dictonary

    # df_out = df_out.sort_values(to_group)  # Make sure you reorder so output corresponds to knime

    if df_out.empty:
        error("Output of GroupBy node is Empty. Check the parameter of the node and chose REGEX or MANUAL selection, not both !")

    return df_out[to_group+aggregation_list]

# Available functions
# mean()	Compute mean of groups
# sum()	Compute sum of group values
# size()	Compute group sizes
# count()	Compute count of group
# std()	Standard deviation of groups
# var()	Compute variance of groups
# sem()	Standard error of the mean of groups
# describe()	Generates descriptive statistics
# first()	Compute first of group values
# last()	Compute last of group values
# nth()	Take nth value, or a subset if n is a list
# min()	Compute min of group values
# max()	Compute max of group values
