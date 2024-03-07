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
    COLUMN RENAME REGEX NODE
    ============================
    KNIME options implemented:
        - All
"""

import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


def column_rename_regex(
    df: pd.DataFrame,
    search_string: str,
    replace_string: str,
) -> pd.DataFrame:
    d = {}
    for i in range(0, len(df.columns.values)):
        if search_string.startswith("("):
            matcher = re.match(search_string+".*", df.columns.values[i])
        else:
            matcher = re.match(".*"+search_string+".*", df.columns.values[i])
        if matcher is not None:
            new_replace_string = replace_string
            if '$' in replace_string:
                for j in range(1, 9):
                    pattern_str = '$' + str(j)
                    if pattern_str in replace_string:
                        new_replace_string = re.sub("\\" + pattern_str, matcher.group(j), new_replace_string)

            # df.columns.values[i] = replaceString
            d[df.columns.values[i]] = re.sub(search_string, new_replace_string, df.columns.values[i])
            if df.columns.values[i] in df.columns.values[0:i]:
                raise RuntimeError(f"A column has already been named '{df.columns.values[i]}'")
    return df.rename(columns=d)
