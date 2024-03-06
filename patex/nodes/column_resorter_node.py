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
    COLUMN RESORTER NODE
    ============================
    KNIME options implemented:
        - All
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class ColumnResorterNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

        # model = self.xml_root.find(self.xmlns + "config[@key='model']")
        # xml_columns = model.find(self.xmlns + "config[@key='ColumnOrder']").findall(self.xmlns + "entry")
        #
        # self.new_order = []
        # for xml_column in xml_columns:
        #     if xml_column.get('value') == "<any unknown new column>":
        #         self.unknown_new = int(xml_column.get('key'))
        #     elif xml_column.get('key') != 'array-size':
        #         self.new_order.append(xml_column.get('value'))

    def run(self):         #
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)
        # TODO : Implement column resorter and
        #  use more efficient way to reorder columns : cols = df.columns.tolist()
        # current_order = list(df.columns)
        #
        # new_order = self.new_order.copy()
        #
        # for col in current_order:
        #     if col not in new_order:
        #         new_order.insert(self.unknown_new, col)
        #
        #
        # df = df[new_order]
        # #df = df.sort_values(new_order, axis=1)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df)

