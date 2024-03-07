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
    UNPIVOTING NODE
    ============================
    KNIME options NOT implemented:
        - Value Columns : selection by type and wildcard and 'skip rows containing missing cells'
        - Retaining column: selection by type and wildcard
        - Hiliting
"""

import operator
import re
from functools import reduce

import pandas as pd




def unpivoting(
    df: pd.DataFrame,
    filter_type: str,
    retained_type: str,
    id_variables: list = [],
    id_values: list = [],
    to_keep: list = [],
    not_to_keep: list = [],
    a_filter: str | None = None,
    a_retained: str | None = None,
    regex_or_wildcard: str | None = None,
    entry_type: str | None = None,
    case_sensitive_filter: str | None = None,
    this_str: str = '^',
) -> pd.DataFrame:
    id_variables = id_variables.copy()
    id_values = id_values.copy()
    cols_to_keep = to_keep.copy()
    cols_not_to_keep = not_to_keep.copy()
    column_to_check = df.columns

    if filter_type == 'name_pattern':
        if entry_type == 'Regex':
            if case_sensitive_filter == 'true':
                id_values = [col for col in column_to_check if re.match(r''+this_str+a_filter,col)]
                id_variables = [col for col in column_to_check if not re.match(r'' + this_str + a_filter, col)]
            else:
                id_values = [col for col in column_to_check if re.match(r''+this_str+a_filter,col,re.IGNORECASE)]
                id_variables = [col for col in column_to_check if not re.match(r'' + this_str + a_filter, col,re.IGNORECASE)]


    # id_variables.append('RowIDs')
    df = df.melt(id_vars=id_variables, value_vars=id_values, var_name='ColumnNames', value_name='ColumnValues')

    if retained_type == "name_pattern":
        if regex_or_wildcard == 'Regex':
            if case_sensitive_filter == 'true':
                cols_to_keep = [col for col in column_to_check if re.match(r''+this_str+a_retained,col)]
                cols_not_to_keep = [col for col in column_to_check if not re.match(r'' + this_str + a_retained, col)]
            else:
                cols_to_keep = [col for col in column_to_check if re.match(r''+this_str+a_retained,col,re.IGNORECASE)]
                cols_not_to_keep = [col for col in column_to_check if not re.match(r'' + this_str + a_retained, col, re.IGNORECASE)]

    length = len(id_values)
    # df.sort_values('RowIDs', inplace = True)
    for column_to_keep in cols_to_keep:
        if column_to_keep in id_values:  # We have to create a column here because melt didn't create it
            new_df = df[df['ColumnNames']==column_to_keep]
            this_list = new_df['ColumnValues'].values
            new_val = []
            for i in range(len(this_list)):
                new_val.append([this_list[i]]*length)

            new_val = reduce(operator.concat, new_val)
            df[column_to_keep] = new_val

    for column_to_drop in cols_not_to_keep:
        if column_to_drop in id_variables:
            df = df.drop(column_to_drop, axis=1)

    return df
