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
    ASSEMBLE ALL OUTPUT METANODE
    ============================
    KNIME options implemented:
        - None
"""

from patex.nodes.node import PythonNode, SubNode


class AssembleAllOutputNode(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None, 2: None, 3: None, 4: None, 5: None, 6: None, 7: None, 8:None, 9:None, 10:None,
                         11:None, 12:None, 13:None, 14:None, 15:None, 16:None}
        self.out_ports = {1: None}

    def run(self):
        pass
