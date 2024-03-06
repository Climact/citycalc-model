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
    CONSTANT IMPORT METANODE
    ============================
    KNIME options implemented:
        - None
"""

from patex.nodes.node import Node


class ConstantImportNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        self.logger.error("Constant_import Node not implemented (cfr. xml file : " + str(self.xml_file))


    def run(self):
        # TODO add code here for importing constant values
        raise Exception("Constant_import Node not implemented (cfr. xml file : " + str(self.xml_file))
        self.save_out_port(1, "")
