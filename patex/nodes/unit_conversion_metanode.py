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
    UNIT CONVERSION METANODE
    ===================
"""

import re
from timeit import default_timer as timer

from patex.nodes.node import Node


class UnitConversionNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.local_flow_vars = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        # Check code version
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The tree merge node (" + self.xml_file + ") is not connected to the EUCalc - Unit conversion metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-05-16 10:42:15':
                self.logger.error(
                    "The template of the EUCalc - Unit conversion metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['column_selection'] = ('STRING', "elc_opex_.+")
        self.local_flow_vars['new_unit'] = ('STRING', "")
        self.local_flow_vars['conversion_factor'] = ('STRING', "1")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-1245": "column_selection",
                          "string-input-1246": "new_unit",
                          "double-input-1247": "conversion_factor"}

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "Unit Conversion")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "Unit Conversion")

        input_table = self.read_in_port(1)
        # Copy input to output
        columns_used = [col for col in input_table.columns if re.match(self.local_flow_vars['column_selection'][1], col)]

        input_table[columns_used] = input_table[columns_used] * float(self.local_flow_vars['conversion_factor'][1])

        input_table.columns = [re.sub('\[.*\]', '[' + self.local_flow_vars['new_unit'][1] + ']', c) if c in columns_used else c
                               for c in input_table.columns]

        self.save_out_port(1, input_table)
        # logger
        t = timer() - start
        self.log_timer(t, "END", "Unit Conversion")
