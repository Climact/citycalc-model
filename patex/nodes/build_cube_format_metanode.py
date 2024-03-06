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
    BUILD CUBE FORMAT METANODE
    ==========================
    KNIME options implemented:
        - None
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class BuildCubeFormatNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD', "(EMPTY) Build cube format")


    def run(self):
        start = timer()
        self.log_timer(None, 'START', "(EMPTY) Build cube format")

        # logger
        t = timer() - start
        self.log_timer(t, 'END', "(EMPTY) Build cube format")
