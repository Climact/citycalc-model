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
    JSON READER NODE
    ============================
    KNIME options implemented:
        - NONE
"""

from patex.nodes.node import Context, PythonNode, NativeNode


class JSONReaderNode(PythonNode, NativeNode):
    knime_name = "JSON Reader"

    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.out_ports = {1: None}

    def apply(self):
        # TODO add code here for JSON reader
        raise Exception("Json Reader Node not implemented (cfr. xml file : " + str(self.xml_file))
