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
    ADD TRIGRAM
    ===========
    KNIME options implemented:
        - ALL
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class AddSpecificTrigram(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.local_flow_vars = {}
        self.convert_dict = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        # Read configuration input => gives trigram
        self.local_flow_vars['trigram'] = ('STRING', "trigram")

        # Find values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'trigram-' in child.get('key'):
                    value = grandchild.get('value')
                    self.local_flow_vars['trigram'] = ('STRING', value)

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "Add Trigram")

    def run(self):
        start = timer()
        self.log_timer(None, "START", "Add Trigram")

        df = self.read_in_port(1)

        # Find variables
        dimensions = list(df.select_dtypes(['int', 'object']).columns)
        variables = [c for c in df.columns if c not in dimensions]
        df = df[dimensions + variables]

        # Rename variables
        ## trigram = self.convert_dict[self.flow_vars['module-name'][1]] + '_'
        trigram = self.local_flow_vars['trigram'][1] + "_"
        variables = [trigram + c for c in variables]

        # Create output
        df.columns = dimensions + variables
        output_table = df

        # logger
        t = timer() - start
        self.log_timer(t, "END", "Add Trigram")

        self.save_out_port(1, output_table)


if __name__ == '__main__':
    id = '8253'
    relative_path = f'/Users/climact/patex-container/dev/lifestyle/workflows/lifestyle_processing/1_1 Lifestyl (#0)/Add Trigram (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = AddTrigram(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_run_pycharm.csv', index=False)
    node.update()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_update_pycharm.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_update_pycharm.csv'), pd.read_csv('/Users/climact/Desktop/out_run_pycharm.csv'), )
