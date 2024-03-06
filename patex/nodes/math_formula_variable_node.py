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
    MATH FORMULA VARIABLE NODE
    ============================
    KNIME options NOT implemented:
        - Selection of column by wildcards
        - Use of row index and row count
        - All special mathematical function except for COL_MIN, COL_MAX, COL_MEAN, COL_MEDIAN, pi, e ,COL_SUM, COL_STDDEV, COL_VAR, ln
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class MathFormulaVariableNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)


    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        self.logger.error("The math Formula (Variable) node is not implemented in the converter: " + self.xml_file)
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        self.logger.error("The math Formula (Variable) node is not implemented in the converter: " + self.xml_file)

        # logger
        t = timer() - start
        self.log_timer(t, "END")