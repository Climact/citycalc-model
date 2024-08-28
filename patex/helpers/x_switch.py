# ----------------------------------------------------------------------------------------------------- #
# 2021, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    X SWITCH
    This node allows to switch from a vector to another one (ex. energy-carrier = gas-ff to gas-bio)
    Selected Variables are :
    - Metric needed to be change because of switch (ex. energy-demand[TWh])
    - Categories used to define "vector from" and "vector to"
    - Dimension used as vector (ex. energy-carrier)
    Please note :
    - Dimensions defined in fuel-switch table should have te same name as vector + "-to" and "-from"
    Example : if vector = energy-carrier > dimensions in fuel-switch table = energy-carrier-to and energy-carrier-from
    - Metrics linked to switch should always contains "-switch" and "[" in its name
    ==================================
    KNIME options implemented:
        -
"""
import re

import pandas as pd


def x_switch(
    demand_table: pd.DataFrame,
    switch_table: pd.DataFrame,
    correlation_table: pd.DataFrame,
    col_energy: str = "energy-demand[TWh]",
    category_from_selected: str = "fffuels",
    category_to_selected: str = "biofuels",
    col_energy_carrier: str = "energy-carrier",
) -> pd.DataFrame:
    # Variables
    dim_to = col_energy_carrier + "-to"
    dim_from = col_energy_carrier + "-from"

    # Correlation : keep only value of interest
    ## Keep only values for category-from and category-to
    mask = (correlation_table['category-to'] == category_to_selected) & (correlation_table['category-from'] == category_from_selected)
    correlation_table = correlation_table.loc[mask, :]
    del correlation_table['category-to']
    del correlation_table['category-from']
    ## category-from => renamed as carrier - and - category-to => renamed as carrier-to
    correlation_table = correlation_table.rename(columns=lambda x: re.sub('.*-from', 'carrier', x))
    correlation_table = correlation_table.rename(columns=lambda x: re.sub('.*-to', 'carrier-to', x))
    ## Save it for ratio
    ratio_table = correlation_table.copy()
    ## Remove line when missing
    correlation_for_all = correlation_table[["carrier", "carrier-to"]]
    correlation_for_all = correlation_for_all.drop_duplicates()
    ## Correlation "from" only
    correlation_from_table = correlation_table[["carrier"]]
    correlation_from_table = correlation_from_table.drop_duplicates()
    ## Correlation "to" only
    correlation_to_table = correlation_table[["carrier-to"]]
    correlation_to_table = correlation_to_table.drop_duplicates()

    # Switch table :
    ## Rename : switch-col => switch[%] / dimension-from => carrier / dimension-to => carrier-to
    switch_col = ""
    for col in switch_table.columns:
        if col.find("switch") >= 0 and col.find("[") >= 0:
            switch_col = col
    switch_table = switch_table.rename(columns={dim_from: "carrier", dim_to: "carrier-to", switch_col: "switch[%]"})
    ## If carrier (dim-from) contains all as category => replace all by possible value
    mask = (switch_table["carrier"] == "all")
    switch_all = switch_table.loc[mask, :]
    switch_specific = switch_table.loc[~mask, :]
    del switch_all["carrier"]
    switch_all = switch_all.merge(correlation_for_all, how="inner", on="carrier-to")
    switch_table = pd.concat([switch_all, switch_specific], ignore_index=True)
    ## Keep only row of interest => Inner join on category from
    switch_table = switch_table.merge(correlation_from_table, how="inner", on="carrier")
    ## Keep only row of interest => Inner join on category to
    switch_table = switch_table.merge(correlation_to_table, how="inner", on="carrier-to")
    ## Add ratio table
    common_columns = list(set(ratio_table) & set(switch_table))
    switch_table = switch_table.merge(ratio_table, how="inner", on=common_columns)

    # Demand table : rename columns
    ## energy-demand => demand[unit] - and - dimension_energy => carrier
    demand_table = demand_table.rename(
        columns={col_energy: "demand[unit]", col_energy_carrier: "carrier"})

    # Get values for demand to be retrieved and added
    ## Get common cols
    common_columns = list(set(demand_table) & set(switch_table))
    ## Merge switch and demand => get demand to be switch
    change_table = demand_table.merge(switch_table, on=common_columns, how="inner")
    change_table["retrieve[unit]"] = change_table["demand[unit]"] * change_table["switch[%]"]
    del change_table["demand[unit]"]
    del change_table["switch[%]"]
    ## Apply ratio on retrieve value
    add_table = change_table.copy()
    add_table["retrieve[unit]"] = add_table["retrieve[unit]"] * add_table["ratio[-]"]
    del add_table["ratio[-]"]
    del change_table["ratio[-]"]
    ## Group by (for retrieve and add values) => defines useful common columns
    retrieve_gpby = []
    add_gpby = []
    for col in change_table:
        if col != "retrieve[unit]":
            if col != "carrier-to":
                retrieve_gpby.append(col)
            if col != "carrier":
                add_gpby.append(col)
    ### demand to be added => Group by on all column except carrier
    add_table = add_table.groupby(by=add_gpby, as_index=False, dropna=False).sum(numeric_only=True)
    add_table = add_table.rename(columns={"retrieve[unit]": "add[unit]"})
    ### demand to be retrieve => Group by on all column except carrier-to
    retrieved_table = change_table.groupby(by=retrieve_gpby, as_index=False).sum(numeric_only=True)

    # Apply switch to demand
    ## Get common cols
    common_columns = list(set(demand_table) & set(retrieved_table))
    ## demand = demand - retrieve => if retrieve missing : set to 0
    demand_table = demand_table.merge(retrieved_table, on=common_columns, how="left")
    demand_table["retrieve[unit]"] = demand_table["retrieve[unit]"].fillna(0)
    demand_table["demand[unit]"] = demand_table["demand[unit]"] - demand_table["retrieve[unit]"]
    del demand_table["retrieve[unit]"]
    ## demand = demand + add => if add missing : set to 0
    add_table = add_table.rename(columns={"carrier-to": "carrier"})
    demand_table = demand_table.merge(add_table, on=common_columns, how="left")
    demand_table["add[unit]"] = demand_table["add[unit]"].fillna(0)
    demand_table["demand[unit]"] = demand_table["demand[unit]"] + demand_table["add[unit]"]
    del demand_table["add[unit]"]
    ## for carrier(-to) that were not present in demand table => keep value and concat them with demand table
    list_new_carrier = list(set(switch_table['carrier-to']) - set(demand_table['carrier']))
    if len(list_new_carrier) > 0:
        new_table = add_table[add_table["carrier"].isin(list_new_carrier)]
        new_table = new_table.rename(columns={"add[unit]": "demand[unit]"})
        demand_table = pd.concat([demand_table, new_table], ignore_index=True)

    # Rename column as initial
    demand_table = demand_table.rename(
        columns={"demand[unit]": col_energy, "carrier": col_energy_carrier})

    return demand_table
