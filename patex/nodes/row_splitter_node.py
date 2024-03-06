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
    ROW SPLITTER NODE
    ============================
    KNIME options implemented:
        - None
"""

from patex.nodes.node import Node


class RowSplitterNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    def build_node(self):
        self.logger.error("Row Splitter Node not implemented (cfr. xml file : " + str(self.xml_file))

    def run(self):
        # TODO add code here for row splitter
        raise Exception("Row Splitter Node not implemented (cfr. xml file : " + str(self.xml_file))
