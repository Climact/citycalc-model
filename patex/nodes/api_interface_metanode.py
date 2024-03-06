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
    API INTERFACE METANODE
    ======================
    KNIME options implemented:
        - None
"""

import pandas as pd

from patex.nodes.node import PythonNode, SubNode


class APIinterfaceNode(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.out_ports = {1: pd.DataFrame()}

    def run(self):
        pass
