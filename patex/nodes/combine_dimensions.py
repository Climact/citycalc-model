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
    COMBINE DIMENSIONS
    ==================
    KNIME options implemented:
        - ALL
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class CombineDimensions(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "Combine Dimensions")

    def run(self):
        start = timer()
        self.log_timer(None, "START", "Combine Dimensions")

        # Copy input to output
        input_table = self.read_in_port(1)
        df = input_table

        # Select dimensions
        dimensions = list(df.select_dtypes(['object']).columns)
        dimensions.sort()

        # Fill missing dimensions
        df[dimensions] = df[dimensions].fillna("na")

        # Aggregate them in Dimensions column
        df['Dimensions'] = df[dimensions].agg('_'.join, axis=1)

        # Drop dimensions columns
        for col in dimensions:
            del df[col]

        output_table = df

        # logger
        t = timer() - start
        self.log_timer(t, "RUN", "Combine Dimensions")

        self.save_out_port(1, output_table)
