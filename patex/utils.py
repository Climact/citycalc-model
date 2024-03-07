import logging
import os.path
import pickle
import sys
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def reduce_mem_usage(props):
    """Reduce the memory usage by converting the type of the dataframe columns

    :param props: dataframe
    :return: reduced dataframe
    """
    NAlist: list[Any] = []  # Keeps track of columns that have missing values filled in.
    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings
            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all():
                NAlist.append(col)
                IsInt = False
            else:
                # test if column can be converted to an integer
                asint = props[col].fillna(0).astype(np.int64)
                result = props[col] - asint
                result = result.sum()
                if result > -0.01 and result < 0.01:
                    IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                ###############################################################################
                # Disactivated: 487 MB => 460 MB in memory (with compression) but slowing down the converter
                ###############################################################################
                if mn >= 0:
                    pass
                    # props[col] = pd.to_numeric(props[col], downcast='signed')
                else:
                    pass
                    # props[col] = pd.to_numeric(props[col], downcast='unsigned')

            # Downcast floats
            else:
                ###############################################################################
                # THE DOWNCAST of FLOAT is causing issues in the results of Power and Buildings
                ###############################################################################

                # props[col] = pd.to_numeric(props[col], downcast='float')
                pass
                # props[col] = props[col].astype(np.float32)

            # Print new column type
            # print("dtype after: ", props[col].dtype)
            # print("******************************")

        # if object, convert to categories
        else:
            ###############################################################################
            # Disactivated: 487 MB => 400 MB in memory (with compression) but creating issues in 'missing_value_node' and 'pivoting_node' because the category dtype does not exist in np.dtypes
            ###############################################################################

            pass
            # num_unique_values = len(props[col].unique())
            # num_total_values = len(props[col])
            # if num_unique_values / num_total_values < 0.5:
            #     props.loc[:, col] = props[col].astype('category')
            # else:
            #     props.loc[:, col] = props[col]

    return props, NAlist


def get_lever_value(df, column_name, selected_levers):
    df_levers = pd.DataFrame(selected_levers)
    # Add value selected as a column
    df = df.merge(df_levers, how="left", on="lever-name")
    # Add level 0
    df["level_0"] = 0
    # column to delete after loop
    columns_to_delete = [
        "level_0",
        "level_1",
        "level_2",
        "level_3",
        "level_4",
        "lever-value-selected",
    ]
    # Loop on levers values and compute it
    df_fts = df.copy()
    df_tmp = []
    for i in range(0, 4):
        # Select row based on lever-value-selected
        mask_level = (df_fts["lever-value-selected"] > i) & (
            df_fts["lever-value-selected"] <= i + 1
        )
        try:
            df_tmp_tmp = df_fts.copy().loc[mask_level, :]
            # Set useful lever values
            level_niv1 = "level_" + str(i)
            level_niv2 = "level_" + str(i + 1)
            niv1 = df_tmp_tmp[level_niv1]
            niv2 = df_tmp_tmp[level_niv2]
            diff = df_tmp_tmp["lever-value-selected"] - i
            # Calculate lever value
            df_tmp_tmp[column_name] = niv1 + ((niv2 - niv1) * diff)
            # Concatenate with df
            if not df_tmp_tmp.empty:
                for i in columns_to_delete:
                    del df_tmp_tmp[i]
                df_tmp.append(df_tmp_tmp)
        except KeyError:
            pass

    return df_tmp


MEMO = {}


def read_memoized(path):
    if path in MEMO:
        return MEMO[path]
    else:
        df = pd.read_csv(path)
        MEMO[path] = df
        return df


def import_fts_ots(
    df_ots,
    df_fts,
    df_level_data,
    path_ods_data,
    path_level_data,
    country_filter,
    max_year,
    trigram,
    metric_name,
    rename_ots_column=True,
    rename_level_data=False,
):
    for country in np.unique(country_filter.split("|")):
        if country != "EU28":
            file_name_ots = country + "_" + trigram + "_" + metric_name + "_ots.csv"
            file_name_fts = country + "_" + trigram + "_" + metric_name + "_fts.csv"
            file_name_level_data = (
                country + "_" + trigram + "_" + metric_name + "_level-data.csv"
            )

            # Read FTS data (data filtered based on max year)
            df_t = read_memoized(Path(path_ods_data, file_name_fts))
            if "Years" in df_t.columns:
                df_t = df_t[df_t["Years"] <= int(max_year)]
            df_fts = pd.concat([df_fts, df_t], ignore_index=True)

            # Read OTS data
            df_t = read_memoized(Path(path_ods_data, file_name_ots))
            if rename_ots_column:
                metric_name_ots = df_t["metric-name"][0]
                df_t.rename(columns={"ColumnValues": metric_name_ots}, inplace=True)
            df_ots = pd.concat([df_ots, df_t], ignore_index=True)

            # Read level data
            df_t = read_memoized(Path(path_level_data, file_name_level_data))
            if "Years" in df_t.columns:
                df_t = df_t[df_t["Years"] <= int(max_year)]
            if rename_level_data:
                df_t = df_t.rename(
                    columns={
                        "controlled_by": "lever_name",
                        "ambition_level": "level_name",
                    }
                )
            df_level_data = pd.concat([df_level_data, df_t], ignore_index=True)

    return df_ots, df_fts, df_level_data


def patternreshape(pattern, case_sensitive="false"):
    """One of the biggest challenge in the converter is to use the REGEX of the Knime workflow the same way in
    the converter. The pattern reshape function allows to reshape the regex pattern in order to make that possible

    :param pattern: REGEX pattern
    :param case_sensitive: boolean
    :return: reshaped pattern
    """

    # Knime and python are dealing differently with the REGEX
    if pattern.startswith(" "):
        # Remove empty space at the beginning of the pattern
        pattern = pattern[1:]
    if pattern.startswith("|"):
        # Remove 'OR' character at the beginning of the pattern
        pattern = pattern[1:]
    if pattern.endswith("|"):
        # Remove 'OR' character at the end of the pattern
        pattern = pattern[:-1]
    str_start = "^"  # For the regex to only look at the beginning of the word
    str_end = "$"  # For the regex to look at the whole word
    str_neg = "(?!"  # To avoid entering in negative lookahead
    str_bracket = "("  # To raise exception if brackets
    if str_neg not in pattern:
        if str_bracket in pattern:
            str_between_bracket = pattern[pattern.find("(") + 1 : pattern.find(")")]
            if "|" in str_between_bracket:
                raise ValueError(
                    'Remove the bracket and the OR char in the pattern ("'
                    + pattern
                    + '")'
                )
        pattern_split = pattern.split("|")
        # insert starting and ending string for each item of the pattern separated by OR char
        pattern_split = [str_start + item + str_end for item in pattern_split]
        pattern = "|".join(pattern_split)
    else:
        pattern = str_start + pattern + str_end
    if case_sensitive == "true":
        pattern = re.compile(pattern)
    else:
        pattern = re.compile(pattern, re.IGNORECASE)
    return pattern
