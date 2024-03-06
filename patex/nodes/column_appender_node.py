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
    COLUMN APPENDER NODE
    ============================
    KNIME options implemented:
        - Only default options
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class ColumnAppenderNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df_left = self.read_in_port(1)
        df_right = self.read_in_port(2)

        output_table = pd.concat([df_left.reset_index(drop=True), df_right.reset_index(drop=True)], axis=1)
    #In case of optimization is possible : here are somme alternatives.
        #output_table = df_left.join(df_right)
        #output_table = df_left.append(df_right)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, output_table)



