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
    COLUMN MERGER NODE
    ============================
    KNIME options implemented:
        - All
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class ColumnMergerNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.primary_column = []
        self.secondary_column = []
        self.output_placement = []
        self.output_name = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.primary_column = model.find(self.xmlns + "entry[@key='primaryColumn']").get('value')
        self.secondary_column = model.find(self.xmlns + "entry[@key='secondaryColumn']").get('value')
        self.output_placement = model.find(self.xmlns + "entry[@key='outputPlacement']").get('value')
        self.output_name = model.find(self.xmlns + "entry[@key='outputName']").get('value')

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)

        # Replace Primary Column
        if self.output_placement == 'ReplacePrimary':
            df[self.primary_column] = df[self.primary_column].fillna(df[self.secondary_column])
        # Replace Secondary Column
        elif self.output_placement == 'ReplaceSecondary':
            df[self.secondary_column] = df[self.primary_column].fillna(df[self.secondary_column])
        # Replace Both Column
        elif self.output_placement == 'ReplaceBoth':
            df[self.primary_column] = df[self.primary_column].fillna(df[self.secondary_column])
            del df[self.secondary_column]
        else:
            df[self.output_name] = df[self.primary_column].fillna(df[self.secondary_column])
            df.assign(output_name=df[self.output_name])

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, self.read_in_port(1))
