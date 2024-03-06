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
    GRAPH VISUALISATION METANODE
    ============================
    KNIME options implemented:
        - NONE
"""
import re
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class CostCalculationNodeEU(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.local_flow_vars = {}
        self.pattern_a = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        # Check code version
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The Cost Calculation node (" + self.xml_file + ") is not connected to the Cost Calculation metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-04-17 12:15:24' and timestamp != '2020-05-27 13:12:33':
                self.logger.error(
                    "The template of the EUCalc - Cost Calculation metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['cost_type'] = ('STRING', "CAPEX")
        self.local_flow_vars['module'] = ('STRING', "agriculture")
        self.local_flow_vars['activity_filter_pattern'] = ('STRING', ".*activity_(.+)\[.*")
        self.local_flow_vars['include_unit_costs'] = ('STRING', "false")


        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-1420": "activity_filter_pattern",
                          "single-selection-1419": "cost_type",
                          "boolean-input-1436": "include_unit_costs",
                          "single-selection-355": "module"}

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        self.pattern_a = re.compile(self.local_flow_vars['activity_filter_pattern'][1], re.IGNORECASE)

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Cost Calculation")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Cost Calculation")


        input_table_1 = self.read_in_port(1)
        input_table_2 = self.read_in_port(2)
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
        mod_tri = trigrams.get(self.local_flow_vars['module'][1])

        # Pandas generates a warning when we set data on a copy of a dataframe. The objective is to
        # warn the user who would want to modify the original dataframe through the copy.
        # Since this is not a typical practice, we disable the warning, which speeds up the function.
        pd.options.mode.chained_assignment = None

        try:
            baseyear = int(self.flow_vars['baseyear'][1])
        except KeyError:
            self.logger.error("PATCH: baseyear = 2015 in Cost Calculation of Storage")
            baseyear = 2015
        pli_column = 'ots_tec_price-indices_pli[%]'
        activity_filter_pattern = self.local_flow_vars['activity_filter_pattern'][1]

        # Rename and split dataframes
        df_activity = input_table_1
        df_unit_costs = input_table_2[input_table_2['sector'] == mod_tri]
        df_price_indices = input_table_2.loc[input_table_2['sector'].isna(), ['Country', pli_column]]

        df_unit_costs['evolution_method'] = df_unit_costs['evolution_method'].astype('int64')
        # Adds the b factor and baseyear costs
        if self.local_flow_vars['cost_type'][1] == 'CAPEX':
            df_unit_costs.rename(columns={'capex_b_factor': 'b_factor', 'capex_d_factor': 'd_factor',
                                          'capex_baseyear': 'unit_cost_baseyear'}, inplace=True)
        elif self.local_flow_vars['cost_type'][1] == 'OPEX':
            df_unit_costs.rename(columns={'opex_b_factor': 'b_factor', 'opex_d_factor': 'd_factor',
                                          'opex_baseyear': 'unit_cost_baseyear'}, inplace=True)

        # Define index for faster lookups
        df_activity['Years'] = df_activity['Years'].astype('int64')
        df_activity.set_index(['Years', 'Country'], inplace=True)

        # Extract technology code from activity
        df_activity.columns = df_activity.columns.str.extract(activity_filter_pattern, expand=False)
        df_activity = df_activity.loc[:, df_activity.columns.notna()]

        # Transform activity into long format
        df_activity_full = df_activity.stack().to_frame('activity')
        df_activity_full.index.set_names('technology_code', level=-1, inplace=True)
        df_activity_full.index.set_names('Country', level=-2, inplace=True)
        df_activity_full.reset_index(inplace=True)

        # Sums all countries as cost calculations happen at European level
        df_activity = df_activity.groupby(by='Years').sum()

        ## Learning rate methodology
        ## -------------------------

        # Identify columns to apply a learning rate method
        df_unit_costs_LR = df_unit_costs.loc[
                           (df_unit_costs['evolution_method'] == 2) | (df_unit_costs['evolution_method'] == 3), :]

        # Identify columns that have to be accumulated over time and replace metric with cumulative sum
        columns_to_accumulate = list(
            set(df_unit_costs.loc[df_unit_costs['evolution_method'] == 2, 'technology_code'].tolist()).intersection(
                df_activity.columns))
        df_activity[columns_to_accumulate] = df_activity.loc[:, columns_to_accumulate].cumsum()

        # Changes to long format
        df_activity_LR = df_activity.stack().to_frame('activity')
        df_activity_LR.index.set_names('technology_code', level=-1, inplace=True)

        # Merge unit costs and activity
        # df_activity_LR=df_activity_LR.reset_index().merge(df_unit_costs_LR.reset_index(), on='technology_code', how='inner', suffixes=['','_y'])
        df_activity_LR = df_activity_LR.reset_index().merge(df_unit_costs_LR, on='technology_code', how='inner',
                                                            suffixes=['', '_y'])

        # Calculate 'learning' = activity^b
        df_activity_LR['learning'] = df_activity_LR['activity'] ** df_activity_LR['b_factor']

        # Calculate factor 'a' in Cost=a*activity^b for baseyear
        df_factors = df_activity_LR.loc[df_activity_LR['activity'] != 0, :].groupby(
            by='technology_code').first().reset_index()
        df_factors['a_factor'] = df_factors['unit_cost_baseyear'] / df_factors['learning']

        # Adds 'a' factor to activity dataframe and calculate Cost=a*activity^b for all years
        df_activity_LR = df_activity_LR.loc[df_activity_LR['Years'] >= baseyear, :]
        df_activity_LR = df_activity_LR.merge(df_factors[['a_factor', 'technology_code']], on=['technology_code'], how='left')
        df_activity_LR.loc[:, 'unit_cost'] = df_activity_LR.loc[:, 'learning'] * df_activity_LR.loc[:, 'a_factor']

        ## Linear evolution methodology
        ## ----------------------------

        df_unit_costs_linear = df_unit_costs.loc[df_unit_costs['evolution_method'] == 1, :]
        df_activity_linear = df_activity.loc[df_activity.index.get_level_values('Years') >= baseyear,
                             :].stack().to_frame('activity')
        df_activity_linear.index.set_names('technology_code', level=-1, inplace=True)
        df_activity_linear = df_activity_linear.reset_index().merge(
            df_unit_costs_linear[['d_factor', 'unit_cost_baseyear', 'technology_code']], on='technology_code',
            how='inner')
        df_activity_linear.loc[:, 'unit_cost'] = df_activity_linear['d_factor'] * (
                    df_activity_linear['Years'] - baseyear) + df_activity_linear['unit_cost_baseyear']

        ## Assemble unit costs from both methodologies and calculate total costs
        ## ---------------------------------------------------------------------

        # Combine unit costs for both methods
        output_table = pd.concat([df_activity_linear[['unit_cost', 'technology_code', 'Years']],
                                  df_activity_LR[['unit_cost', 'technology_code', 'Years']]])

        # Merge with full activity table
        output_table = pd.merge(df_activity_full, output_table, on=['Years', 'technology_code'])

        # Merge with price level index and calculate total cost
        output_table = pd.merge(df_price_indices, output_table, on='Country')
        output_table.loc[:, 'cost'] = output_table.loc[:, 'unit_cost'] * output_table.loc[:, 'activity'] * output_table[
            pli_column] / 100 / 1e6

        # For columns where activity is zero, resulting cost must be zero (otherwise LR methodology could give NaNs)
        output_table.loc[output_table['activity'] == 0, 'cost'] = 0
        output_table.drop([pli_column, 'activity'], axis='columns', inplace=True)

        # Rename technology codes
        if self.local_flow_vars['cost_type'][1] == 'CAPEX':
            prefix = 'capex'
        else:
            prefix = 'opex'

        # Prepare dataframe to be sent to Knime
        output_table['Years'] = output_table['Years'].astype('str')
        if self.local_flow_vars['include_unit_costs'][1] == 'true':
            output_table = output_table.set_index(['Years', 'Country', 'technology_code'])[['cost', 'unit_cost']].unstack('technology_code')
            output_table.columns = [mod_tri + '_' + prefix + '_' + c[1] + '[MEUR]' if c[0] == 'cost' else mod_tri + '_unit-' + prefix + '_' +c[1] + '[EUR/unit]' for c in output_table.columns]
        else:
            output_table = output_table.set_index(['Years', 'Country', 'technology_code'])[['cost']].unstack(
                'technology_code')
            output_table.columns = [mod_tri + '_' + prefix + '_' + c[1] + '[MEUR]' for c in output_table.columns]

        output_table.reset_index(inplace=True)

        self.save_out_port(1, output_table)
        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Cost Calculation")

