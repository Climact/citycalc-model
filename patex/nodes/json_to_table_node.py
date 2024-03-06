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
    JSON TO TABLE NODE
    ============================
    KNIME options implemented:
        - None
"""

from patex.nodes.node import Context, PythonNode, NativeNode


class JSONtoTableNode(PythonNode, NativeNode):
    knime_name = "JSON to Table"

    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def apply(self):
        # TODO add code here for converting JSON into table
        raise Exception("Json To Table Node not implemented (cfr. xml file : " + str(self.xml_file))
