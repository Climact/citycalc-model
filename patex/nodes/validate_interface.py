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
    VALIDATE INTERFACE NODE
    =======================
    KNIME options implemented:
        - ALL
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class ValidateInterface(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def apply(self, df) -> pd.DataFrame:
        return df
