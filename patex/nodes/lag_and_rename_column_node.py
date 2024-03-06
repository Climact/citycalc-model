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
    LAG AND RENAME COLUMN METANODE
    ============================
    KNIME options implemented:
        - All
"""

import os
import xml.etree.cElementTree as ET
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class LagAndRenameColumnNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.outer_flow_vars = {}
        self.column_to_lag = []
        self.new_column_name = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            raise Exception(
                "The lag and rename column node (" + self.xml_file + ") is not connected to the TRA_lag_and_rename_column metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-09-05 11:04:49' and timestamp != "2019-07-05 18:18:20" and timestamp != "2019-08-22 08:37:59" and timestamp != "2019-04-26 17:01:45":
                self.logger.error(
                    "The template of the TRA_lag_and_rename_column metanode was updated. Please check the version that you're using in " + self.xml_file)

        self.outer_flow_vars = dict(self.flow_vars)

        #  Set default values of the flow variable parameters:

        if 'column-to-lag' not in self.flow_vars:
            self.flow_vars['column-to-lag'] = ('STRING', "Country")
        if 'new-column-name' not in self.flow_vars:
            self.flow_vars['new-column-name'] = ('STRING', "column-name")

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        paths = []
        values = []
        for child in model:
            path = child.get('key')
            paths.append(path)
            for grandchild in child:
                value = grandchild.get('value')
                values.append(value)

            if "string-input" in path:
                input_id = path.split("string-input-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "String Input (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()

                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                variable_name = inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value')

                self.flow_vars[variable_name] = ('STRING', value)
            elif "column-selection" in path:
                input_id = path.split("column-selection-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "Column Selection (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()

                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                variable_name = inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value')

                self.flow_vars[variable_name] = ('STRING', value)
            else:
                raise Exception("Unknown quickform node used in metanode (#" + str(
                    self.id) + '). Settings file is : ' + self.xml_file)

        self.column_to_lag = self.flow_vars['column-to-lag'][1]
        self.new_column_name = self.flow_vars['new-column-name'][1]

        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Lag and Rename column")


    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Lag and Rename column")

        output_table = self.read_in_port(1)
        output_table[self.new_column_name] = output_table[self.column_to_lag].shift(+1)

        output_table.Years = pd.to_numeric(output_table.Years)

        mask_first_year = (output_table['Years'] == output_table.Years.min())
        mask_second_year = (output_table['Years'] == output_table.Years.min()+1)
        output_table.loc[mask_first_year, self.new_column_name] = output_table.loc[mask_second_year, self.new_column_name].values

        output_table.Years = output_table.Years.astype(str)

        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Lag and Rename column")

        self.save_out_port(1, output_table)
        self.flow_vars = self.outer_flow_vars # Make sure we filter out all the new flow variables created

