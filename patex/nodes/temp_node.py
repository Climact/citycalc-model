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
    QUALITY CHECK METANODE
    ===================

"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class TempNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):

        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.logger.debug("Maximum number of input port for a TempNode is 10")
        self.in_ports = {1: None, 2: None, 3: None, 4:None, 5:None, 6:None, 7:None, 8:None, 9:None, 10:None}
        self.out_ports = {}

    def build_node(self):
        start = timer()
        # Check code timestamp

        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD', 'Temp Node')

    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'Temp Node')

        # logger
        t = timer() - start
        self.log_timer(t, 'END', 'Temp Node')