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
    BUILDING STOCK NODE
    ==================
    KNIME options implemented:
        xxx
"""

from typing import Any, List
import pandas as pd
import numpy as np

from patex.nodes.globals import Globals


# Functions
def apply_mask(df=pd.DataFrame(), dim=str, value=Any, op=str) -> pd.DataFrame():
    """
    Apply mask and return a copy of the initial df
    :param df: initial dataframe
    :param dim: column name used for filter
    :param value: value used as filter
    :param op: operator used for filter (>=, ==, ...)
    :return: df filtered according to mask options
    """
    if op == "==":
        mask = (df[dim] == value)
    elif op == "!=":
        mask = (df[dim] != value)
    elif op == ">=":
        mask = (df[dim] >= value)
    elif op == ">":
        mask = (df[dim] > value)
    elif op == "<=":
        mask = (df[dim] <= value)
    elif op == "<":
        mask = (df[dim] < value)
    elif op == "is_not_nan":
        mask = ~(df[dim].isna())
    elif op == "in_list":
        mask = (df[dim].isin(value))
    return df.loc[mask, :].copy()


def merge_on_common_cols(df1=pd.DataFrame(), df2=pd.DataFrame(), method=str) -> pd.DataFrame():
    """
    Merge two dataframe based on common columns. If no common columns, we cross merge dataframes
    :param df1: left dataframe
    :param df2: right dataframe
    :param method: how we merge frames (inner, left, ...)
    :return: merge dataframes
    """
    # If Years in df, be sure they are int type
    if 'Years' in df1.columns.tolist():
        df1["Years"] = df1["Years"].astype(int)
    if 'Years' in df2.columns.tolist():
        df2["Years"] = df2["Years"].astype(int)
    # Get common cols
    common_cols = list(set(df1.columns).intersection(df2.columns))
    # Apply merge
    if common_cols:
        df = pd.merge(df1, df2, how=method, on=common_cols)
    else:
        df1["cross-col"] = 1
        df2["cross-col"] = 1
        df = pd.merge(df1, df2, how=method, on=["cross-col"])
        del df["cross-col"]
        del df1["cross-col"]
        del df2["cross-col"]
    return df


def avoid_timestep(df=pd.DataFrame(), yrs=list(), init_yrs=list()):
    if not df.empty:
        dims = [c for c in df.select_dtypes(include=["object"]).columns if c in df.columns]
        metrics = [c for c in df.select_dtypes(include=["float"]).columns if c in df.columns][0]
        df['concat_dims'] = df[dims].agg('-'.join, axis=1)
        df_dims_list = df.copy()  # save dims list for mapping
        df_pivot = pd.pivot_table(df, values=metrics, index=['Years'], columns=['concat_dims'],
                                  aggfunc="sum").reset_index()
        df_yrs = pd.DataFrame({"Years": yrs})
        df_pivot = df_pivot.merge(df_yrs, on=['Years'], how='right')
        df_pivot.sort_values(by=['Years'], inplace=True)
        df_pivot = df_pivot.interpolate(method='linear', limit_direction='forward', axis=0)
        df_melt = pd.melt(df_pivot, id_vars=['Years'], value_vars=df['concat_dims'].unique().tolist())
        df_melt.rename(columns={"variable": "concat_dims", "value": metrics}, inplace=True)
        del df_dims_list[metrics]
        del df_dims_list['Years']
        df_dims_list = df_dims_list.drop_duplicates()
        df = df_melt.merge(df_dims_list, on=['concat_dims'], how='left')
        del df['concat_dims']
    return df


def get_columns_list_with_types(df=pd.DataFrame(), dtypes=List, exclude=[], include=[]):
    columns = df.select_dtypes(include=dtypes).columns.tolist()
    if exclude:
        for k in exclude:
            columns.remove(k)
    if include:
        columns = columns + include
    return columns


def fill_missing_year(df=pd.DataFrame(), yrs=List, value='min-max', metrics=List):
    dims = get_columns_list_with_types(df=df, dtypes=['object'])
    metric = get_columns_list_with_types(df=df, dtypes=['float'])
    df_fin = pd.DataFrame()
    df_yr = pd.DataFrame({"Years": yrs})
    # Fill missing years : based on max and min years
    for k in ["min", "max"]:
        limit_val = df.groupby(dims)["Years"].max().reset_index()
        oper = ">"
        if k == "min":
            limit_val = df.groupby(dims)["Years"].min().reset_index()
            oper = "<"
        limit_val = merge_on_common_cols(df1=df, df2=limit_val, method='inner')
        limit_val.rename(columns={"Years": "limit-year"}, inplace=True)
        df1 = merge_on_common_cols(df1=df_yr, df2=limit_val, method='right')
        df1 = apply_mask(df=df1, dim="Years", value=df1["limit-year"], op=oper)
        if value != 'min-max':  # Define value for this years (by default, we use min/max Years values)
            for m in metrics:
                df1[m] = value
        del df1["limit-year"]
        df_fin = pd.concat([df_fin, df1], ignore_index=True)
    df_fin = pd.concat([df_fin, df], ignore_index=True)
    return df_fin


def apply_lag_per_dims(df=pd.DataFrame(), df_lag=pd.DataFrame(), lagged_metric=str, delay_metric=str,
                       keep_lag_col=False):
    yrs = df["Years"].unique().tolist()
    df = merge_on_common_cols(df1=df, df2=df_lag, method='inner')
    df["Years"] = df["Years"] + df[delay_metric]
    df = fill_missing_year(df=df, yrs=yrs, value=0, metrics=[lagged_metric])
    if not keep_lag_col:
        del df[delay_metric]
    return df


def get_cumsum_by_dims(df=pd.DataFrame(), cum_name=str, metric_to_cum=str):
    str_columns = get_columns_list_with_types(df=df, dtypes=['object'])
    sorted_columns = get_columns_list_with_types(df=df, dtypes=['object'], include=["Years"])
    df = df.sort_values(by=sorted_columns)
    df[cum_name] = df.groupby(str_columns)[metric_to_cum].cumsum()
    return df


# Function : STOCK LOGIC
def stock_logic(demand=pd.DataFrame(), order=pd.DataFrame(), mix=pd.DataFrame(), change_status=pd.DataFrame(),
                switch=pd.DataFrame(), switch_delay=pd.DataFrame(), ban=pd.DataFrame(), trigger_point=str,
                delay_metric=str, ban_category=str, detruction_method='worst_first', historical_status=str,
                baseyear=int):
    # 1. Retrieve parameters from input table
    #logging.info(f"--- Step 1 : Get parameters ---")
    # Order columns
    order_value_col = [x for x in order.columns if x.find("[") > 0][0]
    # Switch columns
    switch_cat_col = order.columns.tolist()[0]  # column name for category used as switch (ex. PEB column)
    switch_from_col = switch_cat_col + "-from"
    switch_to_col = switch_cat_col + "-to"
    switch_val_col = [x for x in switch.columns if x.find("[") > 0][0]
    # Demand columns
    demand_col = [x for x in demand.columns if x.find("[") > 0][0]
    demand_unit = demand_col[demand_col.find("[") + 1:demand_col.find("]")]
    # Mix columns
    mix_col = [x for x in mix.columns if x.find("[") > 0][0]
    # Change status columns
    change_status_col = [x for x in change_status.columns if x.find("[") > 0][0]
    # Ban columns
    year_value = [x for x in ban.columns if x.find("[") > 0][0]
    # Years values
    historical_max_year = mix["Years"].max()
    max_year = demand["Years"].max()
    initial_years_list = demand["Years"].unique().tolist()
    nostep_year_list = range(min(initial_years_list), max(initial_years_list) + 1)
    # New column names
    yrly_change_col = "yearly-change[" + demand_unit + "]"
    cum_change_col = "cum-change[" + demand_unit + "]"
    add_demand_col = "additional-demand[" + demand_unit + "]"
    removal_col = "removals[nb]"
    year_cat = 'year-category'

    # Reformat order dataframe and get values from it
    last_category = \
    apply_mask(df=order, dim=order_value_col, value=order[order_value_col].max(), op="==")[switch_cat_col].iloc[0]
    order = order.sort_values(by=[order_value_col])
    del order[order_value_col]

    # Avoid more than 1 year step
    demand = avoid_timestep(df=demand, yrs=nostep_year_list, init_yrs=initial_years_list)
    change_status = avoid_timestep(df=change_status, yrs=nostep_year_list, init_yrs=initial_years_list)
    switch = avoid_timestep(df=switch, yrs=nostep_year_list, init_yrs=initial_years_list)

    # ----------------------------- RECOMPUTE DEMAND depending of DECONSTRUCTION allocation ----------------------------- #
    # 2. Initial number of buildings per category
    #logging.info(f"--- Step 2 : Recompute intial demand depending on deconstruction rate ---")
    # 2.a. Initial number (same as baseyear values)
    demand_per_cat = merge_on_common_cols(df1=demand, df2=mix, method='inner')
    demand_per_cat[demand_col] = demand_per_cat[demand_col] * demand_per_cat[mix_col]
    del demand_per_cat[mix_col]
    demand_per_cat = fill_missing_year(df=demand_per_cat, yrs=nostep_year_list)
    # 2.b. Deconstruction
    gp_columns = get_columns_list_with_types(df=demand_per_cat, dtypes=['object'], exclude=[switch_cat_col],
                                             include=["Years"])
    demand_without_cat = demand_per_cat.groupby(gp_columns)[
        demand_col].sum().reset_index()  # Values are the same in future (same as baseyear)
    demand_without_cat.rename(columns={demand_col: "total[nb]"}, inplace=True)
    deconstruction_per_cat = merge_on_common_cols(df1=demand, df2=demand_without_cat, method='inner')
    deconstruction_per_cat[removal_col] = deconstruction_per_cat["total[nb]"] - deconstruction_per_cat[demand_col]
    for col in [demand_col, "total[nb]"]:
        del deconstruction_per_cat[col]
    # GET METHOD
    # If mix method : apply mix
    if detruction_method == "same_as_mix":
        full_mix = fill_missing_year(df=mix, yrs=initial_years_list)
        deconstruction_per_cat["total-for-all-cat[nb]"] = deconstruction_per_cat[removal_col]
        deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=full_mix, method='inner')
        deconstruction_per_cat[removal_col] = deconstruction_per_cat[removal_col] * deconstruction_per_cat[mix_col]
        mask = (deconstruction_per_cat[
                    switch_cat_col] == last_category)  # For last category, we allows 100% of the value (not mix)
        deconstruction_per_cat.loc[mask, removal_col] = deconstruction_per_cat.loc[mask, "total-for-all-cat[nb]"]
        del deconstruction_per_cat[mix_col]
    # If worst method : define same values for all renovation category
    if detruction_method == 'worst_first':
        deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=order, method='inner')
        deconstruction_per_cat["total-for-all-cat[nb]"] = deconstruction_per_cat[removal_col]
    # COMPUTE VALUES
    # Recompute deconstruction (min values of : initial stock, deconstruction rate and remaining deconstruction buildings)
    previous_cat = []
    for i, row in order.iterrows():
        cat = row[switch_cat_col]
        prev_values = apply_mask(df=deconstruction_per_cat, dim=switch_cat_col, value=previous_cat, op="in_list")
        if prev_values.empty:
            deconstruction_per_cat["prev_values[nb]"] = 0
        else:
            gp_columns = get_columns_list_with_types(df=deconstruction_per_cat, dtypes=['object'],
                                                     exclude=[switch_cat_col], include=["Years"])
            prev_values = prev_values.groupby(gp_columns)[removal_col].sum().reset_index()
            prev_values.rename(columns={removal_col: "prev_values[nb]"}, inplace=True)
            deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=prev_values, method="inner")
        # Get minimum values amongst available values (initial stock, deconstruction rate and remaining deconstruction buildings)
        deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=demand_per_cat, method="inner")
        df1 = apply_mask(df=deconstruction_per_cat, dim=switch_cat_col, value=cat, op="==")
        df2 = apply_mask(df=deconstruction_per_cat, dim=switch_cat_col, value=cat, op="!=")
        # For df1 : recompute some values
        df1["prev_values[nb]"] = df1["total-for-all-cat[nb]"] - df1["prev_values[nb]"]
        mask = (df1["prev_values[nb]"] < 0)
        df1.loc[mask, "prev_values[nb]"] = 0
        df1[removal_col] = df1[[removal_col, "prev_values[nb]", demand_col]].min(axis=1)
        # After end-year : we keep last values (no deconstruction if this category is 100% BAN)
        end_year_df = apply_mask(df=ban, dim=year_cat, value="end-year", op="==")
        df1 = merge_on_common_cols(df1=df1, df2=end_year_df, method="inner")
        df1 = apply_mask(df=df1, dim="Years", value=df1[year_value], op="<=")
        df1 = fill_missing_year(df=df1, yrs=nostep_year_list, value='min-max', metrics=[removal_col])
        for col in [year_cat, year_value]:
            del df1[col]
        # Concatenate deconstruction(df1/df2) and reformat df
        deconstruction_per_cat = pd.concat([df1, df2], ignore_index=True)
        for col in ["prev_values[nb]", demand_col]:
            del deconstruction_per_cat[col]
        # Add category to list of previous categories
        previous_cat.append(cat)
    # 2.c Add deconstruction to initial demand
    del deconstruction_per_cat["total-for-all-cat[nb]"]
    demand_per_cat = merge_on_common_cols(df1=demand_per_cat, df2=deconstruction_per_cat, method="left")
    demand_per_cat[removal_col] = demand_per_cat[removal_col].fillna(0)
    demand_per_cat[demand_col] = demand_per_cat[demand_col] - demand_per_cat[removal_col]
    del demand_per_cat[removal_col]
    # 2.d Demand_per_cat_first_year
    demand_per_cat_first_yr = apply_mask(df=demand_per_cat, dim="Years", value=baseyear,
                                         op="==")  ## baseyear instead of demand_per_cat["Years"].min()
    demand_per_cat_first_yr.rename(columns={demand_col: "init_nb"}, inplace=True)
    kept_cols = get_columns_list_with_types(df=demand_per_cat_first_yr, dtypes=['object'], include=["init_nb", "Years"])
    demand_per_cat_first_yr = demand_per_cat_first_yr[kept_cols].copy()
    demand_per_cat_first_yr = fill_missing_year(df=demand_per_cat_first_yr, yrs=nostep_year_list)

    # 3. Empty tables (for output storage)
    cum_cat = pd.DataFrame()
    cum_cat_per_yr = pd.DataFrame()
    yrly_renovation = pd.DataFrame()
    building_stock = pd.DataFrame()
    negative_values = pd.DataFrame()
    ban_demand = pd.DataFrame()

    # ------------------------------ LOOP --------------------------------------- #
    # 5. Loop on categories defined by order
    #logging.info(f"--- Step 5 : Loop on categories ---")
    for i, row in order.iterrows():
        # Get current cat
        cat = row[switch_cat_col]
        #logging.info(f"Category : {cat}")
        # Get start-year and end-year for BAN
        index_cols = get_columns_list_with_types(df=ban, dtypes=['object'], exclude=[year_cat])
        ban_values = apply_mask(df=ban, dim=switch_cat_col, value=cat, op="==")
        ban_values = pd.pivot_table(ban_values, values=year_value, index=index_cols, columns=[year_cat],
                                    aggfunc="sum").reset_index()
        # Keep only years before ban end-year
        if not ban_values.empty:
            change_status_cat = merge_on_common_cols(df1=ban_values, df2=change_status, method="inner")
            change_status_cat = apply_mask(df=change_status_cat, dim="Years", value=change_status_cat["end-year"],
                                           op="<")
        else:
            change_status_cat = change_status.copy()
        # Reset demand and removals
        if not negative_values.empty:
            negative_values[switch_cat_col] = cat  # we change cat from previous to current
            demand_per_cat = merge_on_common_cols(df1=demand_per_cat, df2=negative_values, method="left")
            demand_per_cat = demand_per_cat.fillna(0)
            demand_per_cat[demand_col] = demand_per_cat[demand_col] + demand_per_cat["losses[nb]"]
            del demand_per_cat["losses[nb]"]
            deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=negative_values,
                                                          method="left")
            deconstruction_per_cat = deconstruction_per_cat.fillna(0)
            deconstruction_per_cat[removal_col] = deconstruction_per_cat[removal_col] - deconstruction_per_cat[
                "losses[nb]"]
            del deconstruction_per_cat["losses[nb]"]
        if not cum_cat_per_yr.empty:
            prev_val = apply_mask(df=cum_cat_per_yr, dim=switch_cat_col, value=cat, op="==")
            demand_per_cat = merge_on_common_cols(df1=demand_per_cat, df2=prev_val, method="left")
            demand_per_cat[add_demand_col] = demand_per_cat[add_demand_col].fillna(0)
            demand_per_cat[demand_col] = demand_per_cat[demand_col] + demand_per_cat[add_demand_col]
            del demand_per_cat[add_demand_col]

        # ------------------------------- TRIGGER POINT DEMAND ------------------------------- #
        # Compute yearly rate of renovation under trigger points (sales, location, ...)
        demand_i = merge_on_common_cols(df1=demand_per_cat, df2=change_status_cat, method="inner")
        demand_i[yrly_change_col] = demand_i[change_status_col] * demand_i[demand_col]
        # For yearly rate : remove the part of trigger points that doesn't change of category
        no_change_switch = apply_mask(df=switch, dim=switch_from_col, value=cat, op="==")
        no_change_switch = apply_mask(df=no_change_switch, dim=switch_to_col, value=cat, op="==")
        for col in [switch_from_col, switch_to_col]:
            del no_change_switch[col]
        if not no_change_switch.empty:
            demand_i = merge_on_common_cols(df1=demand_i, df2=no_change_switch, method="inner")
            demand_i[yrly_change_col] = demand_i[yrly_change_col] * (1 - demand_i[switch_val_col])
        # Avoid too many trigger point => depends on current buildings demand
        gp_columns = get_columns_list_with_types(df=demand_i, dtypes=['object'], exclude=[trigger_point],
                                                 include=["Years"])
        yrly_demand_tot = demand_i.groupby(gp_columns)[yrly_change_col].sum().reset_index()
        yrly_demand_tot.rename(columns={yrly_change_col: "tot[nb]"}, inplace=True)
        cum_yrl_demand = get_cumsum_by_dims(df=yrly_demand_tot, cum_name="cum-tot[nb]", metric_to_cum="tot[nb]")
        cum_yrl_demand_lag = apply_lag_per_dims(df=cum_yrl_demand, df_lag=pd.DataFrame({"delay[yr]": [1]}),
                                                lagged_metric="cum-tot[nb]", delay_metric="delay[yr]")
        cum_yrl_demand_lag.rename(columns={"cum-tot[nb]": "cum-tot-lag[nb]"}, inplace=True)
        del cum_yrl_demand_lag["tot[nb]"]
        cum_yrl_demand = merge_on_common_cols(df1=cum_yrl_demand, df2=cum_yrl_demand_lag, method="left")
        ratio_df = merge_on_common_cols(df1=demand_per_cat, df2=cum_yrl_demand, method="inner")
        ratio_df["ratio"] = 1  # default value
        mask = (ratio_df["cum-tot[nb]"] > ratio_df[
            demand_col])  # If we overpass the demand, we set ratio to 0 (no trigger point)
        ratio_df.loc[mask, "ratio"] = 0
        mask = (ratio_df["cum-tot[nb]"] > ratio_df[demand_col]) & (
                    ratio_df["cum-tot-lag[nb]"] < ratio_df[demand_col])  # Compute ratio
        ratio_df.loc[mask, "ratio"] = (ratio_df.loc[mask, demand_col] - ratio_df.loc[mask, "cum-tot-lag[nb]"]) / \
                                      ratio_df.loc[mask, "tot[nb]"]
        kept_cols = get_columns_list_with_types(df=ratio_df, dtypes=['object'], include=["Years", "ratio"])
        ratio_df = ratio_df[kept_cols].copy()
        demand_i = merge_on_common_cols(df1=demand_i, df2=ratio_df, method="inner")
        demand_i[yrly_change_col] = demand_i[yrly_change_col] * demand_i["ratio"]
        # Apply delay
        demand_i = apply_lag_per_dims(df=demand_i, df_lag=switch_delay, lagged_metric=yrly_change_col,
                                      delay_metric=delay_metric, keep_lag_col=True)
        # Add missing Years : 0% (no additional yearly renovation)
        demand_i = fill_missing_year(df=demand_i, yrs=nostep_year_list, value=0, metrics=[yrly_change_col])
        # Compute cumulative values of categories under trigger points
        demand_i = get_cumsum_by_dims(df=demand_i, cum_name=cum_change_col, metric_to_cum=yrly_change_col)

        # ------------------------------------ BAN DEMAND ------------------------------------ #
        if not ban_values.empty:
            # Get demand switched thanks to trigger points (sales, location, ...)
            filtered_demand = apply_mask(df=demand_i, dim="Years",
                                         value=demand_i["end-year"] + demand_i[delay_metric] - 1, op="==")
            del filtered_demand[delay_metric]
            gp_columns = get_columns_list_with_types(df=filtered_demand, dtypes=['object'], exclude=[trigger_point])
            if gp_columns != []:
                ban_demand = pd.DataFrame()
                ban_demand["activation_nb"] = filtered_demand.groupby(gp_columns)[cum_change_col].sum()
                ban_demand["nb_init"] = filtered_demand.groupby(gp_columns)[demand_col].mean()
                for k in ["end-year", "steady-year", "start-year"]:
                    ban_demand[k] = filtered_demand.groupby(gp_columns)[k].mean()
                ban_demand = ban_demand.reset_index()
            else:
                dict_val = {
                    "activation_nb": [filtered_demand[cum_change_col].sum()],
                    "nb_init": [filtered_demand[demand_col].mean()]
                }
                for k in ["end-year", "steady-year", "start-year"]:
                    dict_val[k] = filtered_demand[k].mean()
                ban_demand = pd.DataFrame(dict_val)
            # Ban demand (= initial demand (including new from previous categories and removals) - switched (thanks to activation levers))
            ban_demand["tot_ban"] = ban_demand["nb_init"] - ban_demand["activation_nb"]
            mask = (ban_demand["tot_ban"] < 0)
            ban_demand.loc[mask, "tot_ban"] = 0
            # Apply BAN rate => depends on ban start-/end-/steady-year (& dimensions)
            ban_demand["den-years"] = ban_demand["end-year"] - 0.5 * ban_demand["start-year"] - 0.5 * ban_demand[
                "steady-year"] + 0.5
            ban_demand[yrly_change_col] = ban_demand["tot_ban"] / ban_demand["den-years"]
            ban_demand["coef[-]"] = ban_demand[yrly_change_col] / (ban_demand["steady-year"] - ban_demand["start-year"])
            ban_demand = merge_on_common_cols(df1=ban_demand, df2=pd.DataFrame({"Years": nostep_year_list}),
                                              method="inner")
            mask = (ban_demand["Years"] <= ban_demand["start-year"])  # 0 before start-year
            ban_demand.loc[mask, yrly_change_col] = 0
            mask = (ban_demand["Years"] > ban_demand["end-year"])  # 0 after end-year
            ban_demand.loc[mask, yrly_change_col] = 0
            mask = (ban_demand["Years"] > ban_demand["start-year"]) & (ban_demand["Years"] < ban_demand[
                "steady-year"])  # linear interpolation between start year and steady year
            ban_demand.loc[mask, yrly_change_col] = (ban_demand.loc[mask, "Years"] - ban_demand.loc[
                mask, "start-year"]) * ban_demand.loc[mask, "coef[-]"]
            # Compute cumulative values of categories under BAN
            ban_demand = get_cumsum_by_dims(df=ban_demand, cum_name=cum_change_col, metric_to_cum=yrly_change_col)
            # Define trigger point = BAN
            ban_demand[trigger_point] = ban_category

        # ----------------------------------- TOTAL DEMAND ----------------------------------- #
        # Concatenate BAN values with TRIGGER POINTS
        col_to_keep = get_columns_list_with_types(df=demand_i, dtypes=['object'],
                                                  include=[yrly_change_col, cum_change_col, "Years"])
        demand_i = demand_i[col_to_keep]
        if not ban_demand.empty:  # CHANGE : Replace ban_values BY ban_demand
            ban_demand = ban_demand[col_to_keep]
            demand_i = pd.concat([demand_i, ban_demand], ignore_index=True)

        # ------------------------------------- OUTPUTS -------------------------------------- #
        # Renovation depth ; recompute 100% (exclude cat=>cat switch ; ex. G=>G)
        depth_i = apply_mask(df=switch, dim=switch_from_col, value=cat, op="==")
        depth_i = apply_mask(df=depth_i, dim=switch_to_col, value=depth_i[switch_from_col], op="!=")
        depth_i = apply_mask(df=depth_i, dim=switch_to_col, value=depth_i[switch_from_col], op="!=")
        gp_columns = get_columns_list_with_types(df=depth_i, dtypes=['object'], include=["Years"],
                                                 exclude=[switch_to_col])
        tot_depth_i = depth_i.groupby(gp_columns)[switch_val_col].sum().reset_index()
        tot_depth_i.rename(columns={switch_val_col: "tot[%]"}, inplace=True)
        depth_i = merge_on_common_cols(df1=depth_i, df2=tot_depth_i, method="inner")
        depth_i[switch_val_col] = depth_i[switch_val_col] / depth_i["tot[%]"]
        renovation_depth = merge_on_common_cols(df1=demand_i, df2=depth_i, method="outer")
        renovation_depth[cum_change_col] = renovation_depth[cum_change_col] * renovation_depth[switch_val_col]
        renovation_depth[yrly_change_col] = renovation_depth[yrly_change_col] * renovation_depth[switch_val_col]

        # Compute values for next categories & outputs
        # a. Yearly renovation depth (keep category-from / category-to) => Allows costs computation
        col_to_keep = get_columns_list_with_types(df=renovation_depth, dtypes=['object'], exclude=[switch_cat_col],
                                                  include=[yrly_change_col, "Years"])
        yrly_renovation_i = renovation_depth[col_to_keep].copy()
        # Remove NaN values (no renovation)
        yrly_renovation_i = apply_mask(df=yrly_renovation_i, dim=yrly_change_col, op="is_not_nan")
        yrly_renovation = pd.concat([yrly_renovation, yrly_renovation_i], ignore_index=True)
        # b. Cumulated renovation demand by Years / dims (excluding trigger-point) - for each categories-to => Use for next building stock
        gp_columns = get_columns_list_with_types(df=renovation_depth, dtypes=['object'],
                                                 exclude=[trigger_point, switch_cat_col, switch_from_col],
                                                 include=["Years"])
        cumsum_renovation_i = renovation_depth.groupby(gp_columns)[cum_change_col].sum().reset_index()
        cumsum_renovation_i.rename(columns={switch_to_col: switch_cat_col, cum_change_col: add_demand_col},
                                   inplace=True)
        if not cum_cat_per_yr.empty:
            cum_cat_per_yr = merge_on_common_cols(df1=cum_cat_per_yr, df2=cumsum_renovation_i, method="outer")
            gp_columns = get_columns_list_with_types(df=cum_cat_per_yr, dtypes=['object'], include=["Years"])
            cum_cat_per_yr = cum_cat_per_yr.groupby(gp_columns)[add_demand_col].sum().reset_index()
        else:
            cum_cat_per_yr = cumsum_renovation_i.copy()
        # c. Cumulated renovation demand by Years / dims (excluding trigger-point and renovation cat) => Add this to building stock
        gp_columns = get_columns_list_with_types(df=renovation_depth, dtypes=['object'],
                                                 exclude=[trigger_point, switch_cat_col, switch_from_col,
                                                          switch_to_col], include=["Years"])
        cumsum_renovation_i = renovation_depth.groupby(gp_columns)[cum_change_col].sum().reset_index()
        if not cum_cat_per_yr.empty:
            cum_cat_per_yr_i = apply_mask(df=cum_cat_per_yr, dim=switch_cat_col, value=cat, op="==")
            del cum_cat_per_yr_i[switch_cat_col]
            cumsum_renovation_i = merge_on_common_cols(df1=cumsum_renovation_i, df2=cum_cat_per_yr_i, method="left")
        else:
            cumsum_renovation_i[add_demand_col] = 0
        demand_per_cat_i = apply_mask(df=demand_per_cat, dim=switch_cat_col, value=cat, op="==")
        cumsum_renovation_i = merge_on_common_cols(df1=demand_per_cat_i, df2=cumsum_renovation_i, method="outer")
        cumsum_renovation_i = cumsum_renovation_i.fillna(0)
        # filter : keep Years < max Years (as we have delay in renovation)
        cumsum_renovation_i = apply_mask(df=cumsum_renovation_i, dim="Years", value=max_year, op="<=")
        building_stock = pd.concat([building_stock, cumsum_renovation_i], ignore_index=True)
        # d. Get negative values and change deconstruction_per_cat
        negative_values = building_stock.loc[(building_stock[switch_cat_col] == cat), :].copy()
        negative_values["stock[nb]"] = negative_values[demand_col] - negative_values[cum_change_col]
        negative_values["losses[nb]"] = 0  # default values
        mask = (negative_values["stock[nb]"] < 0)
        negative_values.loc[mask, "losses[nb]"] = negative_values.loc[mask, "stock[nb]"]
        kept_cols = get_columns_list_with_types(df=negative_values, dtypes=['object'], include=["Years", "losses[nb]"])
        negative_values = negative_values[kept_cols].copy()
        deconstruction_per_cat = merge_on_common_cols(df1=deconstruction_per_cat, df2=negative_values, method="left")
        deconstruction_per_cat = deconstruction_per_cat.fillna(0)
        deconstruction_per_cat[removal_col] = deconstruction_per_cat[removal_col] + deconstruction_per_cat["losses[nb]"]
        del deconstruction_per_cat["losses[nb]"]
        # e. Keep only 2050 cumsum values => Add this to total new values (concat + groupby)
        cumsum_tot_renovation_i = apply_mask(df=renovation_depth, dim="Years", value=max_year, op="==")
        gp_columns = get_columns_list_with_types(df=cumsum_tot_renovation_i, dtypes=['object'],
                                                 exclude=[trigger_point, switch_cat_col, switch_from_col])
        cumsum_tot_renovation_i = cumsum_tot_renovation_i.groupby(gp_columns)[cum_change_col].sum().reset_index()
        cumsum_tot_renovation_i.rename(columns={switch_to_col: switch_cat_col, cum_change_col: "prev_nb"}, inplace=True)
        if not cum_cat.empty:
            cum_cat = merge_on_common_cols(df1=cum_cat, df2=cumsum_tot_renovation_i, method="outer")
            gp_columns = get_columns_list_with_types(df=cum_cat, dtypes=['object'])
            cum_cat = cum_cat.groupby(gp_columns)["prev_nb"].sum().reset_index()
        else:
            cum_cat = cumsum_tot_renovation_i.copy()

    # 6. Get historical vs renovated (consumption differs)
    pct_histo = "pct-historic[%]"
    pct_historic = merge_on_common_cols(df1=demand_per_cat, df2=cum_cat_per_yr, method="outer")
    pct_historic = pct_historic.fillna(0)
    pct_historic[pct_histo] = pct_historic[add_demand_col] / (pct_historic[add_demand_col] + pct_historic[demand_col])
    pct_historic[historical_status] = "renovated"
    pct_historic_2 = pct_historic.copy()
    pct_historic_2[pct_histo] = 1 - pct_historic_2[pct_histo]
    pct_historic_2[historical_status] = "existing-occupied"
    pct_historic = pd.concat([pct_historic, pct_historic_2], ignore_index=True)

    # 7. Apply historical & renovated status to values
    building_stock = merge_on_common_cols(df1=building_stock, df2=pct_historic, method="left")
    building_stock = merge_on_common_cols(df1=building_stock, df2=demand_per_cat_first_yr,
                                          method="left")  ## Add init_nb
    building_stock = merge_on_common_cols(df1=building_stock, df2=deconstruction_per_cat, method="left")
    building_stock["Years"] = building_stock["Years"].astype(int)
    metrics = get_columns_list_with_types(df=building_stock, dtypes=['float'], exclude=[pct_histo])
    for mt in metrics:
        building_stock[mt] = building_stock[mt] * building_stock[pct_histo]
    # Compute building stock
    building_stock = building_stock.fillna(0)
    building_stock["building-stock[m2]"] = building_stock["init_nb"] + building_stock[add_demand_col] - building_stock[
        cum_change_col] - building_stock[removal_col]
    building_stock["building-stock[m2]"] = building_stock["building-stock[m2]"].round(0)
    del building_stock[pct_histo]
    for mt in metrics:
        del building_stock[mt]

    # 8. Add historic-mean
    mask_historic = (mix[switch_cat_col] == "historic-mean")
    mix_hist = mix.loc[mask_historic, :].copy()
    demand_hist = merge_on_common_cols(df1=demand, df2=mix_hist, method="inner")
    demand_hist[demand_col] = demand_hist[demand_col] * demand_hist[mix_col]
    demand_hist[historical_status] = "existing-occupied"
    del demand_hist[mix_col]
    # Building stock
    demand_hist["building-stock[m2]"] = demand_hist[demand_col]
    building_stock = pd.concat([building_stock, demand_hist], ignore_index=True)
    del building_stock[demand_col]
    # Yearly renovation
    renovation_hist = merge_on_common_cols(df1=demand_hist, df2=change_status, method="inner")
    renovation_hist.rename(columns={switch_cat_col: switch_from_col}, inplace=True)
    renovation_hist = merge_on_common_cols(df1=renovation_hist, df2=switch, method="inner")
    renovation_hist[yrly_change_col] = renovation_hist[demand_col] * renovation_hist[change_status_col] * \
                                       renovation_hist[switch_val_col]
    del renovation_hist[change_status_col]
    del renovation_hist[switch_val_col]
    del renovation_hist[demand_col]
    yrly_renovation = pd.concat([yrly_renovation, renovation_hist], ignore_index=True)
    # demand_historic_mean[historical_status] = "existing-occupied"

    # 9. Reset initial time step
    yrly_renovation = apply_mask(df=yrly_renovation, dim="Years", value=initial_years_list, op="in_list")
    building_stock = apply_mask(df=building_stock, dim="Years", value=initial_years_list, op="in_list")

    # 10. Return results
    #logging.info(f"--- Step 10 : Return results ---")
    return yrly_renovation, building_stock


def buildings_stock_logic(demand_df, order_df, mix_df, status_change_df, switch_df, switch_delay_df, ban_df) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Inputs (parameters => USERS)
    ban_category_name = "BAN"  # User value used for BAN (cfr values inside activation column)
    trigger_point_name = "trigger-point"
    delay_metric_name = "renovation-delay[years]"
    historical_status_name = "area-type"

    # Baseyear
    baseyear = Globals.get().base_year

    # Apply function
    renovation_rate, stock = stock_logic(demand=demand_df.copy(), order=order_df.copy(), mix=mix_df.copy(),
                                         change_status=status_change_df.copy(),
                                         switch=switch_df.copy(),
                                         switch_delay=switch_delay_df.copy(), ban=ban_df.copy(),
                                         detruction_method='worst_first',  # detruction_method='worst_first', 'same_as_mix'
                                         trigger_point=trigger_point_name,
                                         delay_metric=delay_metric_name,
                                         ban_category=ban_category_name,
                                         historical_status=historical_status_name,
                                         baseyear=baseyear)

    return renovation_rate, stock

