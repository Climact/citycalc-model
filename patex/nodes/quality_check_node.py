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
    ======================

"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class QualityCheckNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {}

    def build_node(self):
        start = timer()
        # Check code timestamp

        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD', 'Quality Check')

    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'Quality Check')

        # logger
        t = timer() - start
        self.log_timer(t, 'END', 'Quality Check')