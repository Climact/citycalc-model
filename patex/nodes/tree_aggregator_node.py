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
    TREE AGGREGATOR NODE
    ============================
    KNIME options implemented:
        - Nothing - not sure this node is actually needed on its own
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class TreeAggregatorNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Tree Aggregator")


    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Tree Aggregator")
        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Tree Aggregator")
        """"#  Set default values of the flow variable parameters:

        if 'unit_var' not in self.flow_vars:
            self.flow_vars['unit_var'] = "unit"
        if 'aggregation_method' not in self.flow_vars: 
            self.flow_vars['aggregation_method'] = "Product"
        if 'aggregation_remove' not in self.flow_vars:
            self.flow_vars['aggregation_remove'] = "true"
        if 'index_pattern' not in self.flow_vars:
            self.flow_vars['index_pattern'] = "var"
        if 'aggregation_pattern' not in self.flow_vars:
            self.flow_vars['aggregation_pattern'] = "identifier"
        if 'new_name_start' not in self.flow_vars:
            self.flow_vars['new_name_start'] = "fts_new-column"

        # Hardcoded flow variable values set for testing purposes:

        self.flow_vars['index_pattern'] = "fts_eff_car_(.*)\[.*"
        self.flow_vars['aggregation_pattern'] = "var"

        # load input:

        df = self.read_in_port(1)
        print(df)

        output = aggregate(df, self.flow_vars['unit_var'], self.flow_vars['aggregation_method'],
                           self.flow_vars['index_pattern'], self.flow_vars['aggregation_pattern'],
                           self.flow_vars['new_name_start'], self.flow_vars['aggregation_remove'], 'AGGREGATOR')"""
