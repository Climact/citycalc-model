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
    WRITE TO DATABASE METANODE
    ============================
    KNIME options NOT implemented:
        - Fail if an error occurs
        - Insert null for missing columns not checked
        - ALL sql type except for varchar, integer, numeric
"""

from patex.nodes.node import Context, PythonNode, SubNode


class WriteToDBNode(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None, 2: None}

    def run(self):
        # TODO add code here for write to database node
        pass
