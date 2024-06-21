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
    IMPORT DATA METANODE
    ===================
    Import your data to your workflow.

    KNIME options implemented:
        - All
"""
import logging
from pathlib import Path
import math
import re

import numpy as np
import pandas as pd

from patex.metrics import create_validation_key
from patex.helpers.globals import Globals
from patex.utils import get_lever_value, import_fts_ots_s3, import_fts_ots_local, import_ods_s3, read_memoized

DEFAULT_FOLDER = "_interactions"
RX_dim = re.compile(r"dimension_.*")


def linear_projection(
    ambition, last_value, last_year, start_year, end_year, df, metric_name
):
    """
    For Linear > determine a and b factor (val = a * (year - last-available-year) + b)
    """
    if end_year != last_year:
        a = (ambition - last_value) / (end_year - last_year)
    else:
        a = ambition - last_value
    for year in [
        year for year in df.index if int(year) >= start_year and int(year) > last_year
    ]:
        df.loc[year, metric_name] = a * (int(year) - last_year) + last_value


def s_curve_projection(
    ambition, last_value, last_year, start_year, end_year, df, metric_name
):
    """
    For S-Curve > determine a, b and c factor (val = a * [np.power(np.exp(-([(year - lastyear) / c] - mu) / beta) + 1, -1) - b] + d
    a = target - value_lastyear
    b = np.power(np.exp(-(x0-mu)/beta) + 1, -1)
    c = end-year - lastyear - 1
    d = value_lastyear
    S-Curve parameters
    """
    x0 = 0
    Sc = 0.999
    mu = 0.5
    beta = (mu - 1) / np.log(1 / Sc - 1)
    # Apply s-curve
    a = ambition - last_value
    b = np.power(np.exp(-(x0 - mu) / beta) + 1, -1)
    c = end_year - last_year - 1
    for year in [
        year for year in df.index if int(year) >= start_year and int(year) > last_year
    ]:
        # Get x value = np.power(np.exp(-([(year - lastyear) / c] - mu) / beta) + 1, -1)
        x = np.power(
            np.exp(-(((int(year) - last_year) / c) - mu) / beta) + 1, -1
        )  # Get x value
        # Get final value = a * (x - b) + d
        df.loc[year, metric_name] = a * (x - b) + last_value


# step function with n being the number of steps
def stepn_projection(
    ambition, last_value, last_year, start_year, end_year, df, metric_name, n
):
    if n == 1:
        step_year = 0
    else:
        step_year = (end_year - start_year) / n
    for year in [
        year for year in df.index if int(year) >= start_year and int(year) > last_year
    ]:
        for i in range(n):
            if year < start_year + step_year * (i + 1):
                df.loc[year, metric_name] = (
                    last_value + (ambition - last_value) * (i + 1) / n
                )
                break
            else:
                df.loc[year, metric_name] = ambition


def calculate_lever_projections(ots, fts, dynamic_lever_params):
    """
    Calculate lever projections (dynamic levers)
    """

    # Parameters
    start_year = dynamic_lever_params["start_year"]
    end_year = dynamic_lever_params["end_year"]
    ref_year = dynamic_lever_params['reference_year']
    curve_type = dynamic_lever_params["curve_type"]
    relative = dynamic_lever_params["relative"]
    if "target" in dynamic_lever_params:
        target = dynamic_lever_params["target"]
        Tx = dynamic_lever_params["Tx"]
        # target is a value between 1 and 4. Find the interpolation of Tx for this target
        ambition = np.interp(target, [1, 2, 3, 4], Tx)
    elif "ambition" in dynamic_lever_params:
        ambition = dynamic_lever_params["ambition"]
    # Build structure of output table
    df_L1 = fts.drop(
        ["level_2", "level_3", "level_4"], axis=1
    ).copy()  # We keep L1 because we need it for later
    df = fts.drop(["level_1", "level_2", "level_3", "level_4"], axis=1)
    dim_names = list(
        df.dropna(axis=1, how="all")
        .drop(
            ["Region", "Years", "metric-name", "lever-name", "key_metric-name-dim"],
            axis=1,
        )
        .columns
    )

    df_ots = ots.copy()
    metric_name = df_ots["metric-name"].values[0]

    df = df.set_index(["Years"])
    df_L1 = df_L1.set_index(["Years"])
    df[metric_name] = 0
    df_out = pd.DataFrame()

    # Group data by region and dimensions
    for id, metric_df in df.groupby(["Region"] + dim_names):
        dim_values = [k for k in id[1:]]
        region = id[0]
        mask = df_ots["Region"] == region
        for i in range(len(dim_names)):
            mask = mask & (df_ots[dim_names[i]] == dim_values[i])
        metrics_ots = df_ots[mask]

        # Get last year and last year value from OTS
        last_year = metrics_ots["Years"].max()
        last_year_value = metrics_ots[metrics_ots["Years"] == last_year][
            metric_name
        ].values[0]
        year_list = metric_df.index

        if relative == 1:
            # In relative mode, value is expressed based on reference year value (default = 2015)
            if math.isnan(ref_year):
                ref_year = 2015
            ambition_value = ambition * metrics_ots[metrics_ots['Years'] == int(ref_year)][metric_name].values[0]
        else:
            ambition_value = ambition

        ## if start-year > last-available
        ## => follow curve of L1
        ## last-available = last value of L1 before change of direction
        if start_year > last_year:
            for year in range(last_year + 1, start_year + 1):
                if year in year_list:
                    metric_df.loc[year, metric_name] = df_L1.loc[year, "level_1"]
            last_year = start_year
            last_year_value = metric_df.loc[last_year, metric_name]

        # Calculate projection following a linear dynamic
        if curve_type == "linear":
            linear_projection(
                ambition_value,
                last_year_value,
                last_year,
                start_year,
                end_year,
                metric_df,
                metric_name,
            )

        # Calculate projection following a s-curve dynamic
        elif curve_type == "s-curve":
            s_curve_projection(
                ambition_value,
                last_year_value,
                last_year,
                start_year,
                end_year,
                metric_df,
                metric_name,
            )

        # Calculate projection following a step dynamic
        elif curve_type == "step1":
            stepn_projection(
                ambition_value,
                last_year_value,
                last_year,
                start_year,
                end_year,
                metric_df,
                metric_name,
                1,
            )
        elif curve_type == "step2":
            stepn_projection(
                ambition_value,
                last_year_value,
                last_year,
                start_year,
                end_year,
                metric_df,
                metric_name,
                2,
            )
        elif curve_type == "step4":
            stepn_projection(
                ambition_value,
                last_year_value,
                last_year,
                start_year,
                end_year,
                metric_df,
                metric_name,
                4,
            )

        # For year > end-year => set end-year value (= target)
        for year in [year for year in year_list if int(year) >= end_year]:
            metric_df.loc[year, metric_name] = ambition_value
        df_out = pd.concat([df_out, metric_df])

    return df_out.drop(["lever-name", "metric-name"], axis=1).reset_index()


def dynamic_levers_2_lever_projections(dynamic_levers, input_table, input_table_level_data, lever_name, metric_dim, df_fts, ambition_level_name="ambition_level"):
    metric = dynamic_levers[lever_name].copy()
    tmp_fts = df_fts[
        df_fts["key_metric-name-dim"] == metric_dim
        ]
    # When specifying the lever as dynamic at a lever scale, we need to retrieve the
    # definition of the targets (Tx) and the relative value to apply them to all the
    # metrics of the lever
    dft = input_table_level_data[
        input_table_level_data["key_metric-name-dim"]
        == metric_dim
        ]
    dft = dft.sort_values(by=[ambition_level_name], ascending=True)
    Tx = dft['target'].values.tolist()
    relative = dft['relative'].tolist()[
        0]  # relative is the same for all level, take the first
    reference_year = dft['reference_year'].tolist()[
        0]  # ref year is the same for all level, take the first
    # value here then
    metric["relative"] = int(relative)
    metric["Tx"] = Tx
    metric["reference_year"] = reference_year
    df_lever_projections = calculate_lever_projections(input_table, tmp_fts, metric)
    return df_lever_projections


def dynamic_metrics_2_lever_projections(input_table, input_table_level_data, metric, tmp_fts, ambition_level_name="ambition_level"):
    metric_dim = metric["metrics"]
    tmp_tmp_fts = tmp_fts[
        tmp_fts["key_metric-name-dim"] == metric_dim
        ]
    # if we specify a target instead of a couple 'ambition'-'relative' we need to
    # retrieve the definition of the targets (Tx) and the relative value
    if "target" in metric:
        dft = input_table_level_data[
            input_table_level_data['key_metric-name-dim'] == metric_dim]
        dft = dft.sort_values(by=[ambition_level_name], ascending=True)
        Tx = dft['target'].values.tolist()
        relative = dft['relative'].tolist()[
            0]  # relative is the same for all level, take the first
        reference_year = dft['reference_year'].tolist()[
            0]  # ref year is the same for all level, take the first
        # value here then
        metric["relative"] = int(relative)
        metric["Tx"] = Tx
        metric["reference_year"] = reference_year
    df_lever_projections = calculate_lever_projections(input_table, tmp_tmp_fts, metric)
    return df_lever_projections, metric_dim


def import_data(
    trigram: str,
    variable_name: str = "metric-name",
    variable_type: str = "OTS/FTS",
) -> pd.DataFrame:

    max_year = Globals.get().max_year
    country_filter = Globals.get().country_filter
    levers = Globals.get().levers
    dynamic_levers = Globals.get().dynamic_levers
    path_ods = Globals.get().s3_ods
    local = Globals.get().local
    # --------------------------------------------------------#
    # IMPORT DATA
    # Initialize tables
    input_table = pd.DataFrame()
    input_table_fts = pd.DataFrame()
    input_table_level_data = pd.DataFrame()
    # Get variable
    naming_dict = {
        "OTS (only)": "ots",
        "OTS/FTS": "fts",
        "Calibration": "cal",
        "CP": "cp",
        "RCP": "rcp",
        "HTS": "hts",
    }
    var_type = naming_dict[variable_type]

    # Define local file path
    path_level_data = None
    if local:
        path_ods = Path(local, "_common", "_ods")
        path_level_data = Path(local, "_common", "_level_data")

    # Initialize dynamic levers dataframe
    df_dyn_levers = pd.DataFrame()

    ## ------------- CP --------------- ##
    if variable_type == "CP":
        file_name = variable_name + "_" + var_type
        if local:
            file_name += ".csv"
            input_table = read_memoized(Path(path_ods, file_name))
        else:
            path = f'{path_ods}/{file_name}'
            input_table = import_ods_s3(path,var_type)

    ## ------------- OTS/FTS --------------- ##
    elif variable_type == "OTS/FTS":
        # Get file names
        if local:
            input_table, input_table_fts, input_table_level_data = import_fts_ots_local(
                input_table,
                input_table_fts,
                input_table_level_data,
                path_ods,
                path_level_data,
                country_filter,
                max_year,
                trigram,
                variable_name,
            )
        else:
            input_table, input_table_fts, input_table_level_data = import_fts_ots_s3(
                input_table,
                input_table_fts,
                input_table_level_data,
                path_ods,
                country_filter,
                max_year,
                trigram,
                variable_name,
            )

        # Get lever_name
        # It's possible to have more than one lever for a given variable
        # (each one controlling a specific dimension)
        lever_name_list = list(set(input_table_fts["lever-name"]))

        # Get the dynamic levers and their values
        if dynamic_levers:
            dynamic_levers_list = [
                lever for lever in dynamic_levers if lever in lever_name_list
            ]
            if dynamic_levers_list:
                # Create validation key for FTS and level data
                not_dim_cols = [
                    "Region",
                    "Year",
                    "metric-name",
                    "lever-name",
                    "reference_year",
                    "level_1",
                    "level_2",
                    "level_3",
                    "level_4",
                ]
                dim_cols_fts = [
                    col
                    for col in input_table_fts.columns
                    if col not in not_dim_cols
                ]
                input_table_fts = create_validation_key(
                    input_table_fts, "metric-name", dimensions=dim_cols_fts
                )
                dim_cols_level_data = [c for c in input_table_level_data.columns if re.match(RX_dim, c)]
                input_table_level_data = create_validation_key(
                    input_table_level_data,
                    "metric-name",
                    dimensions=dim_cols_level_data,
                )
                for lever in dynamic_levers_list:
                    # check if dynamic_levers[lever] is a list. If it is a list, it means that the user specify
                    # the metrics that are dynamic for that lever. If it is not a list, it means that all the
                    # metrics are dynamic for that lever
                    dynamic_lever_by_metric = isinstance(
                        dynamic_levers[lever], list
                    )
                    tmp_fts = input_table_fts[
                        input_table_fts["lever-name"] == lever
                    ].copy()
                    if dynamic_lever_by_metric:
                        for metric in dynamic_levers[lever]:
                            df_lever_projections, metric_dim = dynamic_metrics_2_lever_projections(input_table,
                                                                                                   input_table_level_data,
                                                                                                   metric, tmp_fts)
                            df_dyn_levers = pd.concat([df_dyn_levers,df_lever_projections])
                            # remove data from input_table_fts if metric=metric
                            input_table_fts = input_table_fts[
                                input_table_fts["key_metric-name-dim"] != metric_dim
                            ]
                    else:
                        for metric_dim in tmp_fts["key_metric-name-dim"].unique():
                            df_lever_projections = dynamic_levers_2_lever_projections(dynamic_levers, input_table,
                                                                                      input_table_level_data, lever,
                                                                                      metric_dim, tmp_fts)
                            df_dyn_levers = pd.concat([df_dyn_levers, df_lever_projections])
                        input_table_fts = input_table_fts[
                            input_table_fts["lever-name"] != lever
                        ]
                        lever_name_list.remove(lever)
                del input_table_fts["key_metric-name-dim"]
                del df_dyn_levers["key_metric-name-dim"]

        lever_name_list = [lever for lever in lever_name_list]
        # Change OTS and FTS tables
        del input_table["metric-name"]
        # input_table_fts = input_table_fts[input_table_fts['lever-name'].isin(lever_name_list[1])]

    ## ------------- Other than CP and OTS/FTS --------------- ##
    ## cal, hts, rcp, ots only
    else:
        for country in np.unique(country_filter.split("|")):
            if country != "EU28":
                file_name = (
                    country
                    + "_"
                    + trigram
                    + "_"
                    + variable_name
                    + "_" + var_type
                    + ".csv"
                )
                if local:
                    df_t = read_memoized(Path(path_ods, file_name))
                else:
                    df_t = import_ods_s3(path_ods, var_type, [trigram], [country], [variable_name])
                # Filter data based on max year (if possible)
                if "Years" in df_t.columns:
                    df_t = df_t[df_t["Years"] <= max_year]
                input_table = pd.concat([input_table, df_t], ignore_index=True)
        if "metric-name" in input_table.columns:
            metric_name = input_table["metric-name"][0]
            input_table.rename(columns={"ColumnValues": metric_name}, inplace=True)
            del input_table["metric-name"]

    ## ******************* FOR ALL ******************* ##
    ## For all : Years type = int
    if "Years" in input_table.columns:
        input_table["Years"] = input_table["Years"].astype(int)
    df = input_table

    # --------------------------------------------------------#
    # If OTS/FTS : import data frame + apply lever selection and concat fts / ots
    if variable_type == "OTS/FTS":
        # Import OTS
        df_ots = input_table
        # Import FTS
        df = input_table_fts

        if not df.empty:
            metric_name_fts = df["metric-name"][df.index[0]]
            # Get lever values
            selected_levers = {"lever-name": [], "lever-value-selected": []}
            for var_name, var_value in levers.items():
                if var_name in lever_name_list:
                    selected_levers["lever-name"].append(var_name)
                    selected_levers["lever-value-selected"].append(var_value)

            df_fts = get_lever_value(df, metric_name_fts, selected_levers)

            # Add OTS values
            output_table = pd.concat([df_ots] + df_fts, ignore_index=True)

            # Remove unused columns
            columns_to_delete = ["metric-name", "lever-name"]
            for i in columns_to_delete:
                try:
                    del output_table[i]
                except KeyError as e:
                    logging.warning(
                        "The column {} does not exist in the dataframe for the and could not be deleted".format(
                            i
                        )
                    )
            df = output_table
        else:
            df = df_ots

    df = pd.concat([df, df_dyn_levers])
    # -- For all data types ---
    # Rename Region as Country
    if "Region" in df.columns:
        df = df.rename(columns={"Region": "Country"})
    # Force Years to be int
    if "Years" in df.columns:
        df["Years"] = df["Years"].astype(int)

    return df

