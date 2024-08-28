import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
from patex.helpers.globals import Globals
from patex.memory import read_memoized, read_parquet_memoized
IDX = ["key_metric-name_wo-unit"]


def handle_exceptions(metrics):
    """
    Processes the exceptions list and updates the metrics list accordingly.
    FIXME: This function should be removed once the exceptions are removed from the model.

    Args:
        metrics (list): List of metrics to filter by.

    Returns:
        tuple: A tuple containing the exceptions list and updated metrics list.
    """
    exceptions_list = []
    dict_cdt = {
        "transport-demand-pkm": 'transport-demand',
        "transport-demand-tkm": 'transport-demand',
        "elec-emissions-Mt": 'emissions',
        "heat-emissions-Mt": 'emissions'
    }
    updated_metrics = []
    for metric in metrics:
        if metric in dict_cdt.keys():
            updated_metrics.append(dict_cdt[metric])
            exceptions_list.append(metric)
        else:
            updated_metrics.append(metric)
    return exceptions_list, updated_metrics


def determine_s3path(
        path_ods_folder: str,
        dtype: str):
    """
    Determines the correct path based on the data type.

    Args:
        path_ods_folder (str): The base path to the ODS folder.
        dtype (str): The type of data to import (e.g., 'fts', 'ots', 'hts', etc.).

    Returns:
        str: The determined path for the given data type.

    Raises:
        ValueError: If the dtype is unknown.
    """
    paths = {
        "fts": f"{path_ods_folder}/projections",
        "ots": f"{path_ods_folder}/timeseries",
        "hts": f"{path_ods_folder}/timeseries",
        "rcp": f"{path_ods_folder}/timeseries",
        "cal": f"{path_ods_folder}/timeseries",
        "level-data": f"{path_ods_folder}/level_data",
        "level-definition": f"{path_ods_folder}/definitions",
        "cp": path_ods_folder
    }
    if dtype not in paths:
        raise ValueError(f"Unknown data type: {dtype}")
    return paths[dtype]


def handle_cp_dtype(
        path: str,
        dtype: str):
    """
    Processes the 'cp' data type separately.

    Args:
        path (str): The path to the 'cp' data.
        dtype (str): The data type ('cp').

    Returns:
        pd.DataFrame: The processed data for 'cp' type.
    """
    values = read_parquet_memoized(path)
    (module, name, dtype_file) = path.split("/")[-1].split("_")
    if module == "ind":
        values["module"] = module
        values["data_type"] = dtype
    return values


def import_ods_s3(
        path_ods_folder,
        dtype: str,
        modules: list[str] = None,
        regions: list[str] = None,
        metrics: list[str] = [""],
        ref_years: dict[str, list[int]] = None):
    """
    Imports data from ODS files in S3 based on the specified data type and filters.

    Args:
        path_ods_folder (str): The base path to the ODS folder.
        dtype (str): The type of data to import (e.g., 'fts', 'ots', 'hts', etc.).
        modules (list, optional): List of modules to filter by. Defaults to None.
        regions (list, optional): List of regions to filter by. Defaults to None.
        metrics (list, optional): List of metrics to filter by. Defaults to [""].
        ref_years (dict, optional): Dictionary of reference years. Defaults to None.

    Returns:
        pd.DataFrame: The combined data from the ODS files.
    """
    if ref_years is None:
        ref_years = Globals.get().ref_years

    exceptions_list, metrics = handle_exceptions(metrics)
    path = determine_s3path(path_ods_folder,dtype)

    df_list = []
    if dtype == 'cp':
        df_list.append(handle_cp_dtype(path, dtype))
    elif dtype in ['fts', 'ots', 'rcp', 'cal', 'hts']:
        handle_other_dtype(path, dtype, regions, modules, metrics, ref_years, df_list, exceptions_list)
    elif dtype == 'level-data':
        df_list.append(handle_level_data_dtype(path, modules, regions, metrics))
    elif dtype == 'level-definition':
        df_list.append(handle_level_definition_dtype(path, regions, metrics))

    df_out = pd.concat(df_list, ignore_index=True)
    if "key_metric-name_wo-unit" in df_out.columns:
        df_out.drop(columns=["key_metric-name_wo-unit"], inplace=True)
    return df_out


def handle_other_dtype(path, dtype, regions, modules, metrics, ref_years, df_list, exceptions_list):
    """
    Handle the import of data for ots, fts, hts, rcp and cal data types.
    """
    values = read_parquet_memoized(path, regions, modules, dtype)
    filtered_values = values.loc[
        (values.index.get_level_values("key_metric-name_wo-unit").isin(metrics))
    ]
    excluded_columns_tec_costs = ['hypothesis', 'Region_0', 'Source', 'source', 'key_metric-name-dim', "module",
                                  "data_type"]  # Keep Region_source
    excluded_columns = excluded_columns_tec_costs + ['Region_source']

    for id, df in filtered_values.groupby(level=IDX):
        # reset index
        df = df.reset_index()

        if dtype != "fts":
            df = apply_exceptions(df, exceptions_list)
            # Sort by values
        df = df.sort_values(by=['key_metric-name-dim', 'Years'])
        # Remove unused columns and nan values
        df = df.dropna(how='all', axis=1)
        kept_cols = [c for c in df.columns if c not in excluded_columns]
        if dtype != "fts" and df['module'].unique()[0] == 'tec' and df['key_metric-name_wo-unit'].unique()[
            0].startswith('cost'):
            kept_cols = [c for c in df.columns if c not in excluded_columns_tec_costs]
        df = df[kept_cols]

        if dtype in ["ots", "fts"]:
            if dtype == "fts":
                sort_cols = ["Region", "Years", "metric-name", "lever-name", "level_1", "level_2", "level_3", "level_4"]
                order_cols_prefix = ["Region", "Years", "metric-name", "lever-name"]
                order_cols_suffix = ["level_1", "level_2", "level_3", "level_4"]
            else:
                sort_cols = ["Region", "Years", "metric-name", "ColumnValues"]
                order_cols_prefix = ["Region", "Years", "metric-name"]
                order_cols_suffix = ["ColumnValues"]

            df = df.rename(columns={"key_metric-name": "metric-name"})
            dims = sorted([c for c in df.columns if c not in sort_cols])
            order = order_cols_prefix + dims + order_cols_suffix
            df = df[order]
        else:
            metric_w_unit = df['key_metric-name'].unique()[0]
            df = df.rename(columns={"ColumnValues": metric_w_unit})
            del df["key_metric-name"]
            dims = sorted([c for c in df.columns if c not in ["Region", metric_w_unit]])
            order = ["Region"] + dims + [metric_w_unit]
            df = df[order]
        if dtype == "rcp":
            if "Years" in df.columns:
                del df["Years"]
        # Remove Years before 2000
        if dtype not in ["rcp", "hts", "fts"]:
            if ref_years is None:
                logging.error('ref_years is None')
                return None
            years = [y for y in ref_years['historical_full'] if y >= 2000]
            mask = (df["Years"].isin(years))
            df = df.loc[mask, :]
        elif dtype == "hts":
            if ref_years is None:
                logging.error('ref_years is None')
                return None
            years = [y for y in list(set(ref_years['historical_full']) | set(ref_years['futur_full'])) if y >= 2000]
            mask = (df["Years"].isin(years))
            df = df.loc[mask, :]

        df_list.append(df)
    return df_list

def handle_level_definition_dtype(path, regions, metrics):
    values = read_parquet_memoized(path, regions)
    filtered_values = values.loc[
        (values.index.get_level_values("key_metric-name_wo-unit").isin(metrics))
    ]
    df = filtered_values.sort_values(by=['lever_name'])
    del df["module"]
    return df


def handle_level_data_dtype(path, modules, regions, metrics):
    values = read_parquet_memoized(path, regions, modules)
    filtered_values = values.loc[
        (values.index.get_level_values("key_metric-name_wo-unit").isin(metrics))
    ]
    RX_1 = re.compile("dimension_.*")
    dims = sorted([c for c in values.columns if re.match(RX_1, c)])
    return filtered_values.sort_values(by=dims)

def apply_exceptions(df, exceptions_list):
    for exception in exceptions_list:
        if exception == "transport-demand-pkm":
            df = df.loc[df["key_metric-name"].values == "transport-demand[pkm]", :]
        elif exception == "transport-demand-tkm":
            df = df.loc[df["key_metric-name"].values == "transport-demand[bn_tkm]", :]
        elif exception == "elec-emissions-Mt":
            df = df.loc[df["way-of-production"].values == "elec-plant", :]
        elif exception == "heat-emissions-Mt":
            df = df.loc[df["way-of-production"].values != "elec-plant", :]
    return df


def import_fts_ots_local(
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
            values = df_t["ambition_level"].unique().tolist()
            RX = re.compile(r"level_.*")
            if not [c for c in values if re.match(RX, str(c))]:  # If values of ambition_level != level_[0-9]
                df_t["ambition_level"] = df_t["ambition_level"].astype(int).astype(str)
                df_t["ambition_level"] = "level_" + df_t["ambition_level"]
            df_t = df_t.rename(columns={"key_metric-name": "metric-name"})  # In any case ; rename this
            if rename_level_data:
                df_t = df_t.rename(columns={
                    "ambition_level": "level_name",
                    "associated_lever": "lever_name"
                })
            df_level_data = pd.concat([df_level_data, df_t], ignore_index=True)

    return df_ots, df_fts, df_level_data


def import_fts_ots_s3(
    df_ots,
    df_fts,
    df_level_data,
    path_ods_folder,
    country_filter,
    max_year,
    trigram,
    metric_name,
    rename_ots_column=True,
    rename_level_data=False,
):
    # list of regions
    regions = np.unique(country_filter.split("|"))

    # TODO: add exception in name of metrics: elc_emissions_cal and tra_transport-demand_cal
    # remove EU28 from regions
    regions_noEU28 = [r for r in regions if r != "EU28"]
    # import dataframes
    df_fts_t = import_ods_s3(path_ods_folder, 'fts', [trigram], regions_noEU28, [metric_name])
    df_fts = pd.concat([df_fts, df_fts_t], ignore_index=True)
    df_ots_t = import_ods_s3(path_ods_folder, 'ots', [trigram], regions_noEU28, [metric_name])
    df_ots = pd.concat([df_ots, df_ots_t], ignore_index=True)
    df_level_data_t = import_ods_s3(path_ods_folder, 'level-data', [trigram],regions_noEU28,[metric_name])
    df_level_data = pd.concat([df_level_data, df_level_data_t], ignore_index=True)

    # formating fts data
    if "Years" in df_fts.columns:
        df_fts = df_fts[df_fts["Years"] <= int(max_year)]

    # formating ots data
    if rename_ots_column:
        metric_name_ots = df_ots["metric-name"][0]
        df_ots.rename(columns={"ColumnValues": metric_name_ots}, inplace=True)

    # formating level data
    level_values = df_level_data["ambition_level"].unique().tolist()
    RX = re.compile(r"level_.*")
    if not [c for c in level_values if re.match(RX, str(c))]:  # If values of ambition_level != level_[0-9]
        df_level_data["ambition_level"] = df_level_data["ambition_level"].astype(int).astype(str)
        df_level_data["ambition_level"] = "level_" + df_level_data["ambition_level"]
    df_level_data = df_level_data.rename(columns={"key_metric-name": "metric-name"})  # In any case ; rename this
    if rename_level_data:
        df_level_data = df_level_data.rename(columns={
            "ambition_level": "level_name",
            "associated_lever": "lever_name"
        })

    return df_ots, df_fts, df_level_data


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

