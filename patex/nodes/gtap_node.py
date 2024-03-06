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
    GTAP METANODE
    ============================
    KNIME options implemented:
        - None
"""
from patex.nodes.node import Node


class GtapNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None, 3: None, 4: None, 5: None, 6: None, 7: None, 8: None, 9: None, 10: None}
        self.out_ports = {1: None}

    def build_node(self):
        self.logger.warning('The GTAP node is not yet implemented. The xml file is :' + str(self.xml_file))

    def run(self):
        # TODO add code here for GTAP metanode
        self.logger.warning(
            'The GTAP node is not yet implemented. The xml file is :' + str(self.xml_file))
        self.save_out_port(1, self.read_in_port(1))
