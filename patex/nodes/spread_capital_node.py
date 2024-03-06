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
    COMPUTE COSTS
    ==============
    Calculate costs (OPEX or CAPEX) of the products/materials/vectors of the first input
     using cost data from the second input.

    KNIME options implemented:
        - NONE
"""
import pandas as pd
import numpy as np

from patex.nodes.node import Globals


def spread_capital(
    output_table: pd.DataFrame,
    df_wacc: pd.DataFrame,
) -> pd.DataFrame:
    LIFETIME = 25
    CAPEX_METRIC = next(col for col in output_table.columns if "[MEUR]" in col or "[EUR]" in col)

    # Apply WACC
    base_year = Globals.get().base_year
    output_table["n_yr"] = output_table["Years"] - base_year
    common_cols = np.intersect1d(df_wacc.columns, output_table.columns)
    output_table = output_table.merge(df_wacc, on=common_cols.tolist(), how='inner')
    output_table[CAPEX_METRIC] /= (1 + output_table['wacc[%]']) ** output_table['n_yr']

    # Spread capex through time if required
    lever_spread_capex = Globals.get().levers['lever_spread_capex']
    if lever_spread_capex >= 2:
        # Preprocessing for interpolation and spreading
        dims_cols = output_table.select_dtypes(include=["object"]).columns.intersection(output_table.columns)
        output_table['concat_dims'] = output_table[dims_cols].agg('-'.join, axis=1)
        output_table.sort_values(by=['concat_dims', 'Years'], inplace=True)

        # Calculate coefficients for lifetime cost
        output_table['coef1'] = (1 + output_table['wacc[%]']) ** LIFETIME
        output_table['coef2'] = (output_table['coef1'] - 1) / output_table['wacc[%]'].replace(0, np.nan)
        output_table['coef2'].fillna(LIFETIME, inplace=True)
        output_table['lifetime_cost'] = output_table[CAPEX_METRIC] * output_table['coef1'] / output_table['coef2']

        # Efficient calculation of spread_value for each unique concat_dims
        def spread_values(df):
            years = df['Years'].astype(int).values
            all_years = np.arange(years.min(), years.max() + 1)
            interpolated_costs = np.interp(all_years, years, df['lifetime_cost'].values)
            interpolated_costs_cumsum = interpolated_costs.cumsum()
            interpolated_costs_cumsum = pd.Series(interpolated_costs_cumsum, index=all_years)
            df['spread_value'] = (
                interpolated_costs_cumsum.loc[years].values
                - interpolated_costs_cumsum.reindex(years - LIFETIME, fill_value=0).values
            )
            return df

        # Apply spreading function to each group defined by concat_dims
        output_table = output_table.groupby('concat_dims', group_keys=False).apply(spread_values)

        # Cleanup and final adjustments
        output_table[CAPEX_METRIC] = output_table['spread_value']
        output_table.drop(columns=['n_yr', 'concat_dims', 'coef1', 'coef2', 'lifetime_cost', 'spread_value', 'wacc[%]'], inplace=True)
    else:
        output_table[CAPEX_METRIC] = output_table[CAPEX_METRIC] * (1 + output_table["wacc[%]"])
        output_table.drop(columns=['n_yr', 'wacc[%]'], inplace=True)

    return output_table
