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
    JOINER NODE
    ============================
    KNIME options NOT implemented:
        - Joining column on any of the value that matches from the top and bottom input
        - Hiliting
        - Duplicate column handling with 'filter duplicates', 'don't execute' and 'append custom suffix'
        - Joining columns handling with 'remove joining columns from top input'
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


def joiner(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    joiner: str,
    left_input: list[str],
    right_input: list[str],
) -> pd.DataFrame:
    if df_left.empty:
        raise ValueError("First input of Joiner is Empty")
    if df_right.empty:
        raise ValueError("Second input of Joiner is Empty")

    dico = {}

    left_column = df_left.columns.tolist()
    right_column = df_right.columns.tolist()
    length = len(right_column)
    for i in range(length):
        this_str = right_column[i]
        old_name = right_column[i]
        if this_str not in right_input:
            while this_str in (list(set().union(left_column, right_column[0:i]))):
                # FIXME: this may cause error if there is a (# not linked to a column number
                if '(#' in this_str:
                    ls = this_str.replace('(#', ' ').replace(')', ' ').split()
                    ls[-1] = "(#" + str(int(ls[-1]) + 1) + ")"
                    this_str = " ".join(ls)

                else:
                    this_str = this_str + " (#1)"
            right_column[i] = this_str
            dico[old_name] = this_str
    input_dico = dict(zip(right_input, left_input))
    dico.update(input_dico)
    new_column_names_right_input = [dico.get(c, c) for c in df_right.columns]
    df_right = df_right.rename(index=str, columns=dico)
    df_merged = df_left.merge(df_right, how=joiner, on=left_input)

    return df_merged
