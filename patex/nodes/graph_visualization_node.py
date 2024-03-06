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
    GRAPH VISUALISATION METANODE
    ============================
    KNIME options implemented:
        - NONE
"""

from patex.nodes.node import Context, PythonNode, SubNode


class GraphVisualizationNode(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None}

    def run(self):
        pass
