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
    FUEL MIX
    ==================================
    KNIME options implemented:
        -
"""
import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


def fuel_mix(
    input_energy: pd.DataFrame,
    switch_values: pd.DataFrame,
    values_to_from: pd.DataFrame,
    col_energy: str = "energy-demand[TWh]",
    col_category_from: str = "category_fffuels",
    col_category_to: str = "category_biofuels",
    col_energy_carrier: str = "energy-carrier",
) -> pd.DataFrame:
    # Variables
    col_fuel_switch = "fuel-mix[%]"  ## Should be retrieve in a moregeneric way ?! (in Knime and Python)
    dimension_energy_to = "energy-carrier-to"
    dimension_energy_from = "energy-carrier-from"
    category_from_to = "category-from-to"

    # -------------------------------------------
    ## IN FUEL-SWITCH values : we want to keep only the one linked to
    # - category from
    # AND
    # - category to

    # -------------------------------------------
    # VALUES FROM-TO
    values_to_from = values_to_from.rename({col_category_to: dimension_energy_to},
                                           axis='columns')  # Rename column "to" in energy-carrier-to
    values_to_from = values_to_from.rename({col_category_from: dimension_energy_from},
                                           axis='columns')  # Rename column "to" in energy-carrier-to

    # -------------------------------------------
    # VALUES TO
    values_to = values_to_from[[dimension_energy_to]].copy()  # Keep column "category to"
    # Keep only first duplicte values
    values_to.sort_values(dimension_energy_to, inplace=True)
    values_to.drop_duplicates(subset=dimension_energy_to, keep="first", inplace=True)
    # Keep only switch values linked to "values to"
    switch_values = pd.merge(switch_values, values_to, on=dimension_energy_to, how='inner')

    # -------------------------------------------
    # VALUES FROM
    values_from = values_to_from[[dimension_energy_from, dimension_energy_to]].copy()  # Keep column "category from"
    # -------------------------------------------
    # Keep only switch values linked to "values to"
    # IF "all" in category_from ==> all = all possible values in "values_from"
    mask_all = (switch_values[dimension_energy_from] == "all")
    switch_values_from_all = switch_values.loc[mask_all, :]
    switch_values_from_not_all = switch_values.loc[~mask_all, :]

    # IF NOT ALL
    if not switch_values_from_not_all.empty:
        values_from_not_all = values_from.copy()
        del values_from_not_all[dimension_energy_to]  # Remove it from values-from
        switch_values_from_not_all = pd.merge(switch_values_from_not_all, values_from_not_all,
                                              on=dimension_energy_from, how='inner')

    # IF ALL : apply correlation between to and from
    if not switch_values_from_all.empty:
        values_from_all = values_from.copy()
        del switch_values_from_all[
            dimension_energy_from]  # Remove it from switch table ==> we remove "all" values here
        switch_values_from_all = pd.merge(switch_values_from_all, values_from_all, on=dimension_energy_to,
                                          how='inner')

    # CONCAT ALL and NOT ALL
    switch_values = pd.concat([switch_values_from_all, switch_values_from_not_all], ignore_index=True)

    # -------------------------------------------
    # FINAL FUEL SWITCH TABLE : have only one column with energy-carrier

    # FROM
    switch_values_from = switch_values.copy()
    switch_values_from = switch_values_from.rename({dimension_energy_from: col_energy_carrier}, axis='columns')
    switch_values_from = switch_values_from.rename({dimension_energy_to: "correlation"}, axis='columns')
    switch_values_from["category-from-to"] = "from"
    # TO
    switch_values_to = switch_values.copy()
    switch_values_to = switch_values_to.rename({dimension_energy_to: col_energy_carrier}, axis='columns')
    switch_values_to["category-from-to"] = "to"
    switch_values_to["correlation"] = "x"
    del switch_values_to[dimension_energy_from]
    # Remove duplicates for "TO"
    all_columns = switch_values_to.columns.tolist()
    switch_values_to.sort_values(all_columns, inplace=True)
    switch_values_to.drop_duplicates(subset=all_columns, keep="first", inplace=True)
    # FINAL
    input_switch = pd.concat([switch_values_to, switch_values_from], ignore_index=True)
    # Remove duplicates
    input_switch = input_switch.drop_duplicates()

    # -------------------------------------------
    # APPLY FUEL SWITCH
    # Column names
    columns_name_input_switch = input_switch.columns.tolist()
    columns_name_input_energy = input_energy.columns.tolist()

    ## *********************************************************** ##
    # Common column names ==> use to merge tables
    common_columns = list(set(columns_name_input_switch) & set(columns_name_input_energy))
    # Merge table
    merge_table = pd.merge(input_switch, input_energy, on=common_columns, how='right')

    ## *********************************************************** ##
    ## ENERGY CARRIER = FROM ==> GIVES : FROM and SWITCH
    mask_from = (merge_table[category_from_to] == "from")
    # From
    table_from = merge_table.copy().loc[mask_from, :]
    table_from[col_energy] = table_from[col_energy] * (1 - table_from[col_fuel_switch])
    del table_from["correlation"]
    # Switch
    table_switch = merge_table.copy().loc[mask_from, :]
    table_switch[col_energy] = table_switch[col_energy] * table_from[col_fuel_switch]
    del table_switch[col_energy_carrier]
    table_switch = table_switch.rename({"correlation": col_energy_carrier}, axis='columns')
    table_switch[category_from_to] = "to"  # Change here to allow groupby in table_to

    ## *********************************************************** ##
    ## ENERGY CARRIER = TO ==> SUM : TO and SWITCH
    mask_to = (merge_table[category_from_to] == "to")
    # To
    table_to = merge_table.copy().loc[mask_to, :]
    del table_to["correlation"]
    # Concat with switch
    table_to = pd.concat([table_to, table_switch], ignore_index=True)
    # Get string columns for fuel switch + Years
    str_columns = table_to.select_dtypes(exclude=np.number).columns.tolist()
    str_columns.append("Years")
    # Group by str_columns
    table_to = table_to.groupby(str_columns, as_index=False).sum()

    ## *********************************************************** ##
    ## ENERGY CARRIER = REST (not TO and not FROM)
    mask_in = mask_from | mask_to
    table_out = merge_table.loc[~mask_in, :]
    del table_out["correlation"]

    ## *********************************************************** ##
    ## CONCATENATE table_out, table_to, table_from
    output_table = pd.concat([table_out, table_to, table_from], ignore_index=True)
    # Remove category_from_to / energy-switch
    del output_table[category_from_to]
    del output_table[col_fuel_switch]

    # Return Table
    return output_table
