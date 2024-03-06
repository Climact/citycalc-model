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
    TRANSPOSE NODE
    ============================
    KNIME options implemented:
        - There is no option for this node
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class TransposeNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)
        self.logger.warning("The Transpose in Knime and Python may differ when the row ids are not set up correctly: by default Knime uses the row ids 'RowXX' while pandas uses 'XX'. Predefine the row ids in Knime to avoid errors. Config file: "+self.xml_file)
        df1 = df.transpose()
        # Optimization opportunity ? other similar functions exist : df1=pd.DataFrame(df) OR df1 = np.transpose(df)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df1)
