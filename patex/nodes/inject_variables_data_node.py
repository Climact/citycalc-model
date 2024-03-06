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
    INJECT VARIABLES DATA NODE
    ============================
    KNIME options implemented:
        - All
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class InjectVariablesDataNode(PythonNode, NativeNode):
    knime_name = "Inject Variables (Data)"

    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def apply(self, df) -> pd.DataFrame:
        return df

    def run(self):
        input_table = self.read_in_port(1)

        self.save_out_port(1, input_table)
