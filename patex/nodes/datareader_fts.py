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
    DATAB READER FTS METANODE
    ============================
"""
import math
import os
import pickle
import re
import zlib
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from patex.nodes.node import Node
from patex.utils import get_size
from patex.utils import reduce_mem_usage


class DataReaderFTS(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None,google_sheet_prefix=None):
        self.dataframe = pd.DataFrame()
        self.column = None  # Name of the lever
        self.value = None  # Name of the flow variable containing the value for the lever_selection
        self.local_flow_vars = {}
        self.lever_column = []
        self.columns_fts = []
        self.output_table_fts = None
        self.mask_1 = None
        self.mask_2 = None
        self.mask_3 = None
        self.mask_4 = None
        self.compressed = False
        self.google_sheet_prefix = google_sheet_prefix
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        self.log_timer(None, 'START', 'DataReader FTS metanode')

        # Check code version
        # workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        # if workflow_template_information is None:
        #     raise Exception(
        #         "The tree merge node (" + self.xml_file + ") is not connected to the EUCalc - Tree merge metanode template.")
        # else:
        #     timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
        #     if timestamp != '2019-11-13 14:43:09' and timestamp != "2020-04-21 15:38:24" and timestamp != '2020-05-28 12:52:10':
        #         self.logger.error(
        #             "The template of the EUCalc - DataReader FTS metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['pattern'] = ('STRING', "ots_.*")
        self.local_flow_vars['folder'] = ('STRING', "_interactions/data")
        self.local_flow_vars['lever_flowvar'] = ('STRING', "lever_name")
        self.local_flow_vars['lever_file'] = ('STRING', "lever_name")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-562": "folder",
                          "string-input-553": "pattern",
                          "string-input-561": "lever_flowvar",
                          "string-input-555": "lever_file",
                          }

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        path = self.local + "/" + self.local_flow_vars['folder'][1] + "/" + self.google_sheet_prefix
        ots_files = os.listdir(path)
        pattern = re.compile(self.local_flow_vars['pattern'][1])
        current_file = [filename for filename in ots_files if pattern.match(filename)]
        if len(current_file) > 1:
            raise Exception("the number of files to import in the fts import node should be 1")

        path_file = path + "/" + current_file[0]
        output_table = pd.read_csv(path_file)
        #self.logger.info(
        #    'Size of Python node {} in memory: {:12,} bytes before reducing memory'.format(str(self.id),get_size(output_table)))
        output_table['Years'] = output_table['Years'].astype(str)

        output_table, NAlist= reduce_mem_usage(output_table)
        #self.logger.info(
        #    'Size of Python node {} in memory: {:12,} bytes after reducing memory'.format(str(self.id),get_size(output_table)))

        self.lever_column = [col for col in output_table.columns if 'lever' in col]
        self.columns_fts = [col for col in output_table.columns if 'fts' in col]
        self.output_table_fts = output_table[self.columns_fts]
        self.mask_1 = (output_table[self.lever_column[0]] == 1)
        self.mask_2 = (output_table[self.lever_column[0]] == 2)
        self.mask_3 = (output_table[self.lever_column[0]] == 3)
        self.mask_4 = (output_table[self.lever_column[0]] == 4)

        if get_size(output_table) > 50000000: #if file size is greater than 50MB, we compress the dataframe
            self.compressed = True
            self.dataframe = zlib.compress(pickle.dumps(output_table))
        else:
            self.dataframe = output_table


        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "DataReader FTS metanode")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'DataReader FTS metanode')
        if self.compressed:
            output_table = pickle.loads(zlib.decompress(self.dataframe))
        else:
            output_table = self.dataframe.copy()

        value_type = self.flow_vars[self.local_flow_vars['lever_flowvar'][1]][0]
        if value_type == "DATAFRAME":  # if we specify the position of the levers for each country
            value = self.flow_vars[self.local_flow_vars['lever_flowvar'][1]][1]
            df_value = value[['Country', 'levers']]
            df_value = df_value.rename(columns={'levers': self.lever_column[0]})

            df_value_min = df_value.copy()
            df_value_max = df_value.copy()
            df_value_min[self.lever_column] = np.floor(df_value_min[self.lever_column].values)
            df_value_max[self.lever_column] = np.ceil(df_value_max[self.lever_column].values)

            keys = list(df_value.columns.values)
            i1 = self.dataframe.set_index(keys).index
            i2_min = df_value_min.set_index(keys).index
            i2_max = df_value_max.set_index(keys).index
            table_min = self.dataframe[i1.isin(i2_min)].copy()
            table_max = self.dataframe[i1.isin(i2_max)].copy()
            table_min = table_min.reset_index()
            table_max = table_max.reset_index()

            df_value_allCountries = table_min.copy()
            df_value_allCountries = df_value_allCountries.loc[:, ["Country", 'Years']]
            df_value_allCountries = df_value_allCountries.merge(df_value, on = 'Country', how='left')
            df_value_allCountries = df_value_allCountries.reset_index()

            table = table_min[self.columns_fts] + (table_max[self.columns_fts] - table_min[self.columns_fts]).mul((df_value_allCountries[self.lever_column[0]] - table_max[self.lever_column[0]]), axis=0)
            output_table.loc[:, self.columns_fts] = table
        elif value_type == "DICTIONARY":
            value = self.flow_vars[self.local_flow_vars['lever_flowvar'][1]][1]
            if len(value) == 1: #if there is only the default position of lever
                value_default = float(value["default"])
                level_min = math.floor(value_default)
                level_max = math.ceil(value_default)
                mask_min = (output_table[self.lever_column[0]] == int(level_min))
                mask_max = (output_table[self.lever_column[0]] == int(level_max))
                table_min = self.output_table_fts.loc[mask_min].values
                table_max = self.output_table_fts.loc[mask_max].values
                table = table_min + (table_max - table_min) * (value_default - level_min)

                if level_min == 1:
                    output_table = output_table.loc[self.mask_1]
                elif level_min == 2:
                    output_table = output_table.loc[self.mask_2]
                elif level_min == 3:
                    output_table = output_table.loc[self.mask_3]
                elif level_min == 4:
                    output_table = output_table.loc[self.mask_4]
                output_table.loc[:, self.columns_fts] = table

            else:
                value_default = float(value["default"])
                level_min = math.floor(value_default)
                level_max = math.ceil(value_default)
                mask_min = (output_table[self.lever_column[0]] == int(level_min))
                mask_max = (output_table[self.lever_column[0]] == int(level_max))
                table_min = self.output_table_fts.loc[mask_min].values
                table_max = self.output_table_fts.loc[mask_max].values
                if level_min == 1:
                    output_table_temp = output_table.loc[self.mask_1]
                elif level_min == 2:
                    output_table_temp = output_table.loc[self.mask_2]
                elif level_min == 3:
                    output_table_temp = output_table.loc[self.mask_3]
                elif level_min == 4:
                    output_table_temp = output_table.loc[self.mask_4]
                table = table_min + (table_max - table_min) * (value_default - level_min)
                output_table_temp.loc[:, self.columns_fts] = table

                for country, country_value in value.items():
                    if country != "default":
                        value_country = float(value[country])
                        level_min = math.floor(value_country)
                        level_max = math.ceil(value_country)
                        mask_min = (output_table[self.lever_column[0]] == int(level_min))
                        mask_max = (output_table[self.lever_column[0]] == int(level_max))
                        mask_country = (output_table["Country"] == country)
                        table_min_country = self.output_table_fts.loc[mask_min & mask_country].values
                        table_max_country = self.output_table_fts.loc[mask_max & mask_country].values
                        output_table_temp.loc[mask_country, self.columns_fts] = table_min_country + (table_max_country - table_min_country) * (value_country - level_min)
                output_table = output_table_temp

        else:
            ## DECIMAL integration
            value = float(self.flow_vars[self.local_flow_vars['lever_flowvar'][1]][1])
            level_min = math.floor(value)
            level_max = math.ceil(value)
            mask_min = (output_table[self.lever_column[0]] == int(level_min))
            mask_max = (output_table[self.lever_column[0]] == int(level_max))
            table_min = self.output_table_fts.loc[mask_min].values
            table_max = self.output_table_fts.loc[mask_max].values
            table = table_min + (table_max - table_min) * (value - level_min)
            if level_min == 1:
                output_table = output_table.loc[self.mask_1]
            elif level_min == 2:
                output_table = output_table.loc[self.mask_2]
            elif level_min == 3:
                output_table = output_table.loc[self.mask_3]
            elif level_min == 4:
                output_table = output_table.loc[self.mask_4]
            output_table.loc[:, self.columns_fts] = table

            # output_table = output_table.loc[mask_min]

        self.save_out_port(1, output_table)

        ## INTERGER VALUES for levers position
        # value = int(self.flow_vars[self.local_flow_vars['lever_flowvar'][1]][1])

        # mask = (self.dataframe[self.local_flow_vars['lever_file'][1]] == value)
        # self.save_out_port(1, self.dataframe.loc[mask])
        del output_table
        #gc.collect()
        output_table = pd.DataFrame()
        # logger
        t = timer() - start
        self.log_timer(t, "END",  'DataReader FTS metanode')
