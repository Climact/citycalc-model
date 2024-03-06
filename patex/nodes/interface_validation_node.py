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
    INTERFACE VALIDATION NODE
    ============================
    KNIME options implemented:
        - None
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class InterfaceValidationNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", '(EMPTY) Interface validation')


    def run(self):
        start = timer()
        self.log_timer(None, 'START', "(EMPTY) Interface validation")

        # logger
        t = timer() - start
        self.log_timer(t, "RUN", '(EMPTY) Interface validation')

        self.save_out_port(1, self.read_in_port(1))
