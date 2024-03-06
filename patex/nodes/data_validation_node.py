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
    DATA VALIDATION METANODE
    ============================
    KNIME options implemented:
        - None
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class DataValidationNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", 'EUCALC Data validation')


    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'EUCALC Data validation')

        df = self.read_in_port(1)

        if df.isna().sum().sum() != 0:
            self.logger.debug("Data validation metanode #" + str(self.id) + ": There is missing values (" + str(
                df.isna().sum().sum()) + ") in the dataframe - XML:"+self.xml_file)

        # logger
        t = timer() - start
        self.log_timer(t, "END", 'EUCALC Data validation')

        self.save_out_port(1, df)
