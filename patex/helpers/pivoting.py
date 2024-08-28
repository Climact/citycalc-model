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
    PIVOTING NODE
    ============================
    KNIME options implemented:
        - Advanced settings: hiliting, process in memory, retain row ordeor, maximum unique value per group, all column naming except for 'keep original name'
        - Pivot settings: append overal totals, not ignore domain, not ignore missing values
        - Manual aggregation: Aggregation on several columns and all methods except for median, mean and sum
"""

import pandas as pd
import logging


AGGR_RENAME_COL_DICT = {
    "mean": "Mean",
    "sum": "Sum",
    "first": "First*",
    "count": "Count*",
    "max": "Maximum",
}


def pivoting(
    df: pd.DataFrame,
    agg_dict: dict,
    column_name_option: str,
    column_name_policy: str,
    list_group_columns: list[str],
    list_pivots: list[str],
) -> tuple[pd.DataFrame, None, None]:
    try:
        df = df.pivot_table(columns=list_pivots, index=list_group_columns, aggfunc=agg_dict, dropna=True, fill_value=None)
    except KeyError as e:
        logging.error(e)
        logging.error("Check your pivoting node")
        raise e

    # if len(list_pivots) > 2:
    #     warning('Renaming of Pivot table may not works properly if number of pivot is <=2")
    if column_name_policy == 'Keep original name(s)':
        if len(df.columns[0]) == 5:
            df.columns = [f'{k}_{l}_{m}_{n}+{i}' for i, k , l, m, n in df.columns]
        elif len(df.columns[0]) == 4:
            df.columns = [f'{k}_{l}_{m}+{i}' for i, k , l, m in df.columns]
        elif len(df.columns[0]) == 3:
            df.columns = [f'{k}_{l}+{i}' for i, k , l in df.columns]
        elif len(df.columns[0]) == 2:
            if column_name_option == "Pivot name":
                df.columns = [f'{k}' for i, k in df.columns]
            else:
                df.columns = [f'{k}+{i}' for i, k in df.columns]
    elif column_name_policy == 'Aggregation method (column name)':
        if len(df.columns[0]) == 4:
            if len(list_pivots) == 3:
                df.columns = [f'{j}_{k}_{l}+{AGGR_RENAME_COL_DICT[next(iter(agg_dict.values()))]}({i})' for
                              i, j, k, l in df.columns]
            elif len(list_pivots) == 2:
                df.columns = [f'{k}_{l}+{AGGR_RENAME_COL_DICT[j]}({i})' for i, j, k, l in df.columns]
            else:
                logging.error('Pivot node with "Aggregation method (column name)" only support 3 or 2 pivots.')
                raise Exception
        elif len(df.columns[0]) == 3:
            if len(list_pivots) == 2:
                df.columns = [f'{k}_{l}+{AGGR_RENAME_COL_DICT[next(iter(agg_dict.values()))]}({i})' for
                              i, k, l in df.columns]
            elif len(list_pivots) == 1:
                df.columns = [f'{k}+{AGGR_RENAME_COL_DICT[j]}({i})' for i, j, k in df.columns]
            else:
                logging.error('Pivot node with "Aggregation method (column name)" only support 2 or 1 pivots.')
                raise Exception
        elif len(df.columns[0]) == 2:
            if len(list_pivots) == 1:
                df.columns = [f'{k}+{AGGR_RENAME_COL_DICT[next(iter(agg_dict.values()))]}({i})' for i, k in
                          df.columns]
            else:
                logging.error('Pivot node with "Aggregation method (column name)" only support 1 pivots.')
                raise Exception
        else:
            raise Exception
    elif column_name_policy == 'Column name (aggregation method)':
        if len(list_pivots) == 6:
            logging.warning("PATCH for release v2.0 in Buildings in Pivot node")
            df.columns = [f'{j}_{k}_{l}_{m}_{n}_{o}+{i} ({AGGR_RENAME_COL_DICT[agg_dict[i]]})' for i, j, k, l, m, n, o in df.columns]
        elif len(list_pivots) == 5:
            logging.warning("PATCH for release v2.0 in Buildings in Pivot node")
            df.columns = [f'{j}_{k}_{l}_{m}_{n}+{i} ({AGGR_RENAME_COL_DICT[agg_dict[i]]})' for i, j, k, l, m, n
             in df.columns]
        elif len(list_pivots) == 2:
            df.columns = [f'{k}+{i} ({AGGR_RENAME_COL_DICT[j]})' for i, j, k in df.columns]
        else:
            logging.error('Pivot node with "Column name (aggregation method)" only support 1 pivots.')
            raise Exception

    df = df.reset_index(drop=False)

    logging.debug('The Pivot Node is not deserving output to the port 2 and 3')

    return (df, None, None)
