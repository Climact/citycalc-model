# ----------------------------------------------------------------------------------------------------- #
# 2021, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    Merge Variables
    ===============
    KNIME options implemented:
        - None
"""
from patex.nodes.node import Context, PythonNode, NativeNode


class MergeVariablesNode(PythonNode, NativeNode):
    knime_name = "Merge Variables"

    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None, 2: None, 3: None}
        self.out_ports = {1: None}

    def run(self):
        pass
