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
    CELL SPLITTER NODE
    ============================
    KNIME options implemented:
        - None
"""
from patex.nodes.node import Node


class StringManipulationMetaNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        pass

    def run(self):
        output_table = self.read_in_port(1)
        output_table["pivot"] = output_table["energycarrier"] + "_" + output_table["subsector"] + "_" + output_table[
            "buildingtype"]

        self.save_out_port(1, output_table)