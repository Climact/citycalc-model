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
    String manipulation node
    ========================
    KNIME options implemented:
        - Nothing except
        - indexOf(), the modifiers for replace() are not implemented either
        - substr()
        - string()
        - join()
"""

import re

import pandas as pd




PATTERN = re.compile("\$(.*?)\$")


def string_manipulation(
    df: pd.DataFrame,
    expression: str,
    var_name: str,
) -> pd.DataFrame:
    # Find all column names in the expression thanks to the $...$ syntax:
    for var_string in re.findall(PATTERN, expression):
        # Replace all column names in the expression with their expression:
        expression = expression.replace(f"${var_string}$", f"output['{var_string}']")

    expression = "output = " + expression

    # Copy input to output
    ns = {
        'join': join,
        'indexOf': indexOf,
        'string': string,
        'substr': substr,
        'output': df.copy(),
    }

    exec(expression, ns)

    return df.assign(**{var_name: ns['output']})

def join(*arg):
    output_colmun = arg[0]
    for i in range(1, len(arg)):
        output_colmun += arg[i]
    return output_colmun

def indexOf(column_to_modify, toSearch, start=None, modifiers=None):
    if modifiers is not None:
        logging.error("the indexOf() function with modifiers != None is not implemented")
    return column_to_modify.str.index(toSearch, start)

def string(x):
    return str(x)

def substr(column_to_modify, start, length=None):
    column = column_to_modify.str[start:]
    if length is not None:
        column = column.to_frame().join(length.rename('length'))
        column = column.apply(lambda x: x.iloc[0][:x.iloc[1]], 1)
    return column
