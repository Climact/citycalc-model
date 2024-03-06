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
    CALIBRATION METANODE
    ===================
    KNIME options implemented:
        - Everything
    Exceptions:
       - Returns an exception if the timestamp of the metanode is not aligned
"""
from patex.nodes.node import Node


class CalibrationMultiDimensionNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.model = []
        self.paths = []
        self.values = []
        self.calibration_pattern = []
        self.unit_pattern = []
        self.output_table_2 = None
        self.output_table_1 = None
        self.colUnit = None
        self.columns_copied_from_input_int = []
        self.columns_copied_from_input = []
        self.local_flow_vars = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None, 3 : None}

    def build_node(self):
        self.logger.error('The EUCalc Calibration (6 dimensions) metanode is not yet implemented. The xml file is :' + str(self.xml_file))

    def run(self):
        raise Exception ('The EUCalc Calibration (6 dimensions) metanode is not yet implemented. The xml file is :' + str(self.xml_file))

