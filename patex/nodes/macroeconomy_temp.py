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


class MacroEconomyTemp(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):

        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.logger.debug("Maximum number of input port for a TempNode is 10")
        self.in_ports = {1: None, 2: None, 3: None, 4:None, 5:None, 6:None, 7:None, 8:None}
        self.out_ports = {1: None, 2: None, 3: None, 4:None}

    def build_node(self):
        start = timer()
        # Check code timestamp

        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD', 'MacroEconomy Module')

    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'MacroEconomy Module')
        self.save_out_port(1, self.read_in_port(3))
        self.save_out_port(2, self.read_in_port(4))
        self.save_out_port(3, self.read_in_port(5))
        self.save_out_port(4, self.read_in_port(6))
        # logger
        t = timer() - start
        self.log_timer(t, 'END', 'MacroEconomy Module')