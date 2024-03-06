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
    EXTRACT COLUMN HEADER NODE
    ============================
    KNIME options NOT implemented:
        - Restraining columns based on selected column type
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node

"""
    Written by Azfar Umer and Cartuyvels Jacques under the supervision of Matton Vincent (vm@climact.com)
    August-September 2018, Climact, Louvain-la-Neuve
    ----------
    Options not implemented:
    - Restraining columns based on selected column type
"""


class ExtractColumnHeaderNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.replace_header = []
        self.new_header = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        self.replace_header = model.find(self.xmlns + "entry[@key='replaceColHeader']").get('value')
        self.new_header = model.find(self.xmlns + "entry[@key='unifyHeaderPrefix']").get('value')

        new_head_type = model.find(self.xmlns + "entry[@key='coltype']").get('value')

        if new_head_type != "All":
            self.logger.error(
                "We must select all column type for restrained columns (Extract column header node " + id + ", xml file : "+ str(self.xml_file)+")")

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        id = str(self.id) + " " + self.xml_file
        df = self.read_in_port(1)

        old_column = df.columns
        old_column_len = len(old_column)

        df_header = pd.DataFrame()
        if self.replace_header == 'true':
            for i in range(old_column_len):
                new_head = self.new_header + str(i)
                df_header[new_head] = pd.Series(old_column[i])
                df.rename(index=str, columns={old_column[i]: new_head}, inplace=True)

        else:
            for i in range(old_column_len):
                df_header[old_column[i]] = pd.Series(old_column[i])

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df_header)
        self.save_out_port(2, df)
