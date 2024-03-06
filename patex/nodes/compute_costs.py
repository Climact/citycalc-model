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

from patex.nodes.node import Context, Globals, PythonNode, SubNode


class ComputeCosts(PythonNode, SubNode):
    def __init__(
        self,
        cost_type: str = "CAPEX",
        activity_variable: str = "final-transport-demand[vkm]",
        include_unit_costs: bool = False,
    ):
        super().__init__()
        self.cost_type = cost_type
        self.activity_variable = activity_variable
        self.include_unit_costs = include_unit_costs


    def init_ports(self):
        self.in_ports = {1: None, 2: None, 3: None, 4: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        for child in model:
            for grandchild in child:
                if 'activity_variable-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['activity_variable'] = value
                if 'boolean-input-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['include_unit_costs'] = value
                if 'single-selection-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['cost_type'] = value

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    # TODO `module_name` should not be optional
    def apply(self, df_activity, df_unit_costs, df_price_indices, df_wacc, module_name=None) -> pd.DataFrame:
        trigrams = {'agriculture': 'agr',
                    'buildings': 'bld',
                    'climate': 'clm',
                    'ccus': 'ccu',
                    'electricity_supply': 'elc',
                    'employment': 'emp',
                    'industry': 'ind',
                    'lifestyles': 'lfs',
                    'materials': 'mat',
                    'social impacts': 'sip',
                    'transboundary': 'trb',
                    'transport': 'tra',
                    'technology': 'tec',
                    'minerals': 'min',
                    'water': 'wat',
                    'air_pollution': 'air'
                    }
        mod_tri = trigrams.get(module_name)

        # Pandas generates a warning when we set data on a copy of a dataframe. The objective is to
        # warn the user who would want to modify the original dataframe through the copy.
        # Since this is not a typical practice, we disable the warning, which speeds up the function.
        pd.options.mode.chained_assignment = None

        baseyear = Globals.get().base_year
        activity_column = self.activity_variable
        cost_type = self.cost_type
        include_unit_costs = self.include_unit_costs
        origin_col = 'Region_source'
        activity_unit = activity_column.split("[")[1].split("]")[0]
        methodo_col = "methodo-val"
        pli_column = 'price-indices[%]'

        # Rename and split dataframes
        #   1. Dataframe with activity informations
        df_activity['Years'] = df_activity['Years'].astype('int64')
        #   2. Dataframe with costs information (evolution description of capex and opex costs)
        df_unit_costs = df_unit_costs[df_unit_costs['sector'] == mod_tri]
        df_lifetime = df_unit_costs[df_unit_costs['methodo-val'] == 'lifetime'].copy()
        # Pivot table => evolution method and co should be in column and not lines
        cost_cols = ''.join([col for col in df_unit_costs.columns if "[" in col])
        index_cols = [col for col in df_unit_costs.select_dtypes(include=["object"]).columns if col != methodo_col]
        df_unit_costs = df_unit_costs.pivot(index=index_cols, columns=methodo_col, values=cost_cols).reset_index()
        df_unit_costs['evolution-method'] = df_unit_costs['evolution-method'].astype('int64')
        #   3. Dataframe with sharing price informations

        # COSTS :
        # Keep capex or opex according to user choice
        df_unit_costs = df_unit_costs[df_unit_costs['cost-type'] == cost_type.lower()]

        # ACTIVITY :
        # Rename column (make them more generic)
        df_activity.rename(columns={activity_column: 'activity'}, inplace=True)
        # Sum activity by Years and technologies (at EU level)
        dims_cols = [c for c in df_activity.select_dtypes(include=["object"]).columns if c in df_unit_costs.columns]
        dims_with_year = dims_cols.copy()
        dims_with_year.append("Years")
        df_activity_sum = df_activity.groupby(by=dims_with_year).sum().reset_index()

        ## Learning rate methodology
        ## -------------------------
        # Formula used : Cost = A * Ccap ^ b
        # with : Cost > specific capital costs (eg $/kW)
        #        A > Specific capital costs at a total (initial) cumulative capacity of 1
        #        Ccap > Total cumulative installed capacity (eg gigawatts)
        #        b > Learning elasticity
        #
        # 2 ^ b is called progress ratio (PR)
        # 1 - PR is called learning rate (LR)
        #
        # Example : A learning elasticity of -0.32, for example, yields a progress rate of 0.80 and a learning rate of 20%.
        # This means that the specific capital cost of newly installed capacity decreases by 20% for each doubling
        # of total installed capacity.
        # source : http://pure.iiasa.ac.at/id/eprint/6787/1/RR-03-002.pdf

        # Identify rows to apply a learning rate method
        mask = (df_unit_costs['evolution-method'] == 2) | (df_unit_costs['evolution-method'] == 3)
        df_unit_costs_LR = df_unit_costs.loc[mask, :]

        # Sort cumsum by technologies dimensions and Years
        df_activity_LR = df_activity.sort_values(by=dims_with_year)
        df_activity_LR['cumsum'] = df_activity_LR.groupby(dims_cols)['activity'].transform(pd.Series.cumsum)

        # Merge unit costs and activity
        df_activity_LR = df_activity_LR.merge(df_unit_costs_LR, on=dims_cols, how='inner')

        # Compute learning rate (= activity^b)
        df_activity_LR['learning'] = df_activity_LR['cumsum'] ** df_activity_LR['b-factor']

        # Compute 'a' factor (= Cost / Ccap ^ b = Cost in 2015 / learning)
        df_activity_LR = df_activity_LR.sort_values(by=dims_with_year)
        df_factors = df_activity_LR.loc[df_activity_LR['activity'] != 0, :].groupby(by=dims_cols).first().reset_index()
        df_factors['a-factor'] = df_factors['baseyear'] / df_factors['learning']
        kept_cols = dims_cols.copy()
        for val in ['a-factor']:
            kept_cols.append(val)
        df_factors = df_factors[kept_cols]

        # Keep only Years >= baseyear
        df_activity_LR = df_activity_LR.loc[df_activity_sum['Years'] >= baseyear, :]

        # Force technology dimension to be string (when missing values)
        # Adds 'a' factor to activity dataframe and calculate costs (= A * Ccap ^ b for all years)
        for col in dims_cols:
            df_activity_LR[col] = df_activity_LR[col].astype(object)
            df_factors[col] = df_factors[col].astype(object)
        df_activity_LR = df_activity_LR.merge(df_factors, on=dims_cols, how='inner')

        # Calculate costs
        df_activity_LR['unit_cost'] = df_activity_LR['a-factor'] * df_activity_LR['learning']

        ## Linear evolution methodology
        ## ----------------------------
        # Formula used : Cost = c + delta * d
        # with : c > Specific cost at baseyear
        #        delta > time step as 2050 - year
        #        d > slope as (Cost 2050 - Cost baseyear) / (2050-baseyear)

        # Identify rows to apply a linear method
        kept_cols = dims_cols.copy()
        for val in ['d-factor', 'baseyear', origin_col]:
            kept_cols.append(val)
        df_unit_costs_linear = df_unit_costs.loc[df_unit_costs['evolution-method'] == 1, :]
        df_unit_costs_linear = df_unit_costs_linear[kept_cols]

        # Keep only Years >= baseyear
        df_activity_linear = df_activity_sum.loc[df_activity_sum['Years'] >= baseyear, :]

        # Merge unit costs and activity
        df_activity_linear = df_activity_linear.merge(df_unit_costs_linear, on=dims_cols, how='inner')

        # Calculate costs
        df_activity_linear['unit_cost'] = df_activity_linear['d-factor'] * (df_activity_linear['Years'] - baseyear) + \
                                          df_activity_linear['baseyear']

        ## Assemble unit costs from both methodologies and calculate total costs
        ## ---------------------------------------------------------------------

        # Combine unit costs for both methods
        kept_cols = dims_with_year.copy()
        for val in ['unit_cost', origin_col]:
            kept_cols.append(val)
        output_table = pd.concat([df_activity_linear[kept_cols], df_activity_LR[kept_cols]])

        ## Apply WACC (Capex only)
        ## ---------------------------------------------------------------------
        lever_spread_capex = Globals.get().levers['lever_spread_capex']
        if cost_type == "CAPEX":
            output_table["n_yr"] = output_table["Years"] - baseyear
            common_cols = list(np.intersect1d(df_wacc.columns, output_table.columns))
            output_table = output_table.merge(df_wacc, on=common_cols, how='inner')
            output_table['unit_cost'] = output_table['unit_cost'] / (
                        (1 + output_table['wacc[%]']) ** output_table['n_yr'])
            del output_table["n_yr"]

        ## Spread capex through time if required (part 1)
        ## ---------------------------------------------------------------------
        if cost_type == "CAPEX" and lever_spread_capex >= 2:
            # Get lifetime and associate it to each dims
            for col in ['cost-type', 'methodo-val', 'Region_source', 'sector']:
                if col in df_lifetime.columns.to_list():
                    del df_lifetime[col]
            cost_name = [c for c in df_lifetime.select_dtypes(include=["float"]).columns if c in df_lifetime.columns][0]
            df_lifetime.rename(columns={cost_name: 'lifetime[years]'}, inplace=True)
            common_cols = list(np.intersect1d(df_lifetime.columns, output_table.columns))
            output_table = output_table.merge(df_lifetime, on=common_cols, how='inner')

        # Merge with original activity table
        output_table = pd.merge(df_activity, output_table, on=dims_with_year)

        # Merge with price level index and calculate total cost
        # Total cost [M€] = unit cost * activity [number] * PLI / 1e6
        # Note : PLI = 100(%) when source-country = country (no need to adapt costs)
        df_price_indices = df_price_indices[df_price_indices['type'] == "pli"]  # PLI = Price Level Index
        del df_price_indices['type']
        output_table = pd.merge(df_price_indices, output_table, on='Country')
        mask = (output_table["Country"] == output_table[origin_col])
        output_table.loc[mask, pli_column] = 100
        output_table['cost'] = output_table['unit_cost'] * output_table['activity'] * output_table[
            pli_column] / 100 / 1e6

        # For rows where activity is zero, resulting cost must be zero (LR methodology could give NaNs)
        output_table.loc[output_table['activity'] == 0, 'cost'] = 0
        output_table.drop([pli_column, origin_col, 'activity'], axis='columns', inplace=True)

        # Spread capex through time if required (part 2)
        if cost_type == "CAPEX" and lever_spread_capex >= 2:
            # Preprocessing for interpolation and spreading
            dims_cols = output_table.select_dtypes(include=["object"]).columns.intersection(output_table.columns)
            output_table['concat_dims'] = output_table[dims_cols].agg('-'.join, axis=1)
            output_table.sort_values(by=['concat_dims', 'Years'], inplace=True)

            # Calculate coefficients for lifetime cost
            output_table['coef1'] = (1 + output_table['wacc[%]']) ** output_table['lifetime[years]']
            output_table['coef2'] = (output_table['coef1'] - 1) / output_table['wacc[%]'].replace(0, np.nan)
            output_table['coef2'].fillna(output_table['lifetime[years]'], inplace=True)
            output_table['lifetime_cost'] = output_table['cost'] * output_table['coef1'] / output_table['coef2']
    
            # Efficient calculation of spread_value for each unique concat_dims
            def spread_values(df):
                years = df['Years'].astype(int).values
                all_years = np.arange(years.min(), years.max() + 1)
                interpolated_costs = np.interp(all_years, years, df['lifetime_cost'].values)
                interpolated_costs_cumsum = interpolated_costs.cumsum()
                interpolated_costs_cumsum = pd.Series(interpolated_costs_cumsum, index=all_years)
                df['spread_value'] = (
                    interpolated_costs_cumsum.loc[years].values
                    - interpolated_costs_cumsum.reindex(years - df['lifetime[years]'].values, fill_value=0).values
                )
                return df

            # Apply spreading function to each group defined by concat_dims
            output_table = output_table.groupby('concat_dims', group_keys=False).apply(spread_values)

            # Cleanup and final adjustments
            output_table['cost'] = output_table['spread_value']
            output_table.drop(columns=['concat_dims', 'coef1', 'coef2', 'lifetime_cost', 'spread_value', 'wacc[%]', 'lifetime[years]'], inplace=True)
        elif cost_type == "CAPEX" and lever_spread_capex < 2:
            output_table['cost'] = output_table['cost'] * (1 + output_table["wacc[%]"])
            del output_table["wacc[%]"]

        # Rename technology codes
        if cost_type == 'CAPEX':
            prefix = 'capex'
        else:
            prefix = 'opex'

        # Prepare dataframe to be sent to Knime
        cost_name = prefix + '[MEUR]'
        unit_cost_name = 'unit-' + prefix + '[MEUR/unit]'
        output_table.rename(columns={'cost': cost_name}, inplace=True)
        output_table[cost_name] = output_table[cost_name].astype(float)
        if include_unit_costs == 'true':
            output_table.rename(columns={'unit_cost': unit_cost_name}, inplace=True)
        else:
            del output_table['unit_cost']

        # Reset index
        output_table.reset_index(inplace=True, drop=True)
        return output_table

if __name__ == '__main__':
    id = '5983'
    relative_path = '/Users/climact/XCalc/dev/transport_prototype/workflows/transport_processing/2_2 Transpor (#0)/Cost Calcula (#5983)/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    baseyear = '2015'
    module_name = 'transport'
    node = ComputeCosts(id, relative_path, node_type, knime_workspace, baseyear, module_name)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in_1_new.csv')
    node.in_ports[2] = pd.read_csv('/Users/climact/Desktop/in_2.csv')
    node.build_node()
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)
