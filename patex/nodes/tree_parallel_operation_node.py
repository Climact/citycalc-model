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
    TREE PARALLEL METANODE
    ===================
    KNIME options implemented:
        - Everything
    Exceptions:
       - Returns an exception if the user tries to create a column with a name that's already taken
       - Returns an exception if the timestamp of the metanode is not aligned
"""

import os
import xml.etree.cElementTree as ET
from timeit import default_timer as timer

from patex.nodes.aggregator import aggregate
from patex.nodes.node import Node


class TreeParallelOperationNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.local_flow_vars = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        # Check code versionning
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The tree parallel operation node (" + self.xml_file + ") is not connected to the EUCalc - Tree parallel operation metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-05-28 20:53:15' and timestamp != "2019-08-26 12:47:07" and timestamp  != "2020-03-06 09:01:32":
                self.logger.error(
                    "The template of the EUCalc - Tree parallel operation metanode was updated. Please check the version that you're using in " + self.xml_file)

        #  Set default values of the flow variable parameters:
        if 'unit_var' not in self.local_flow_vars:
            self.local_flow_vars['unit_var'] = ('STRING', "unit")
        if 'aggregation_method' not in self.local_flow_vars:
            self.local_flow_vars['aggregation_method'] = ('STRING', "Product")
        if 'aggregation-remove' not in self.local_flow_vars:
            self.local_flow_vars['aggregation-remove'] = ('INTEGER', 1)
        if 'index_pattern' not in self.local_flow_vars:
            self.local_flow_vars['index_pattern'] = ('STRING', "var")
        if 'aggregation_pattern' not in self.local_flow_vars:
            self.local_flow_vars['aggregation_pattern'] = ('STRING', "identifier")
        if 'new_name_start' not in self.local_flow_vars:
            self.local_flow_vars['new_name_start'] = ('STRING', "fts_new-column")

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

                self.local_flow_vars[variable_name] = ('STRING', value)

            elif "single-selection" in path:
                input_id = path.split("single-selection-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "Single Selection (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()

                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                variable_name = inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value')

                self.local_flow_vars[variable_name] = ('STRING', value)

            elif "boolean-input" in path:
                input_id = path.split("boolean-input-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "Boolean Input (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()

                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                variable_name = inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value')

                if value == 'true':
                    int_value = 1
                else:
                    int_value = 0

                self.local_flow_vars[variable_name] = ('STRING', int_value)
            elif "column-filter" in path:
                pass
            else:
                self.logger.error("Unknown quickform node used in metanode (#" + str(
                    self.id) + '). Settings file is : ' + self.xml_file)

        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Tree Parallel operation")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Tree Parallel operation")

        # load input:
        df = self.read_in_port(1)

        name_components = self.local_flow_vars['new_name_start'][1].split(',')
        if len(name_components) == 1:
            new_name_start = name_components[0]
            new_name_suffix = ''
        elif len(name_components) == 2:
            new_name_start = name_components[0]
            new_name_suffix = '_' + name_components[1]

        # if (self.id == "2232") | (self.id == "2231"):
        #     self.logger.warning("PATCH for buildings calibration node")
        #     output = aggregate(df, self.local_flow_vars['unit_var'][1], self.local_flow_vars['aggregation_method'][1],
        #                    self.local_flow_vars['index_pattern'][1], self.flow_vars['original_pattern'][1],
        #                    self.local_flow_vars['new_name_start'][1], self.local_flow_vars['aggregation-remove'][1], 'CAL')
        # elif self.id == "2237":
        #     self.logger.warning("PATCH for buildings calibration node")
        #     # unit_pattern = re.compile(".*\\[(.*)].*")
        #     # matcher_unit = re.match(self.unit_pattern, df.columns)
        #     # colUnit = matcher_unit.group(1)
        #     output = aggregate(df, "TWh", self.local_flow_vars['aggregation_method'][1],
        #                    self.local_flow_vars['index_pattern'][1], self.flow_vars['original_pattern'][1],
        #                    self.flow_vars['new_name_calibration'][1], self.local_flow_vars['aggregation-remove'][1], 'PARALLEL_OP')
        # else:


        output = aggregate(df, self.local_flow_vars['unit_var'][1], self.local_flow_vars['aggregation_method'][1],
                       self.local_flow_vars['index_pattern'][1], self.local_flow_vars['aggregation_pattern'][1],
                       new_name_start, self.local_flow_vars['aggregation-remove'][1], 'PARALLEL_OP', new_name_suffix = new_name_suffix)

        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Tree Parallel operation")

        self.save_out_port(1, output)
