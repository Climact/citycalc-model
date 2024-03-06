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
    EXPORT VARIABLE NODE
    ====================
    Allow to export one variable from the resulting data of a sub-module.

    KNIME options implemented:
        - all
"""
import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class ExportVariableNode(PythonNode, SubNode):
    def __init__(
        self,
        selected_variable: str,
    ):
        super().__init__()
        self.selected_variable = selected_variable
        self.kept_columns = []

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        for child in model:
            for grandchild in child:
                if 'selected-variable-' in child.get('key'):
                    selected_variable = grandchild.get('value')

        self = PythonNode.init_wrapper(cls,
            selected_variable=selected_variable,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table

        self.kept_columns = []

        # list the columns of type string (dimensions)
        columns = output_table.columns
        for col in columns:
            if output_table[col].dtype == object:
                self.kept_columns.append(col)

        # add Years columns
        self.kept_columns.insert(2, 'Years')

        # add the selected variable
        self.kept_columns.insert(3, self.selected_variable)

        # drop rows with empty variable column
        output_table = output_table[output_table[self.selected_variable].notnull()]

        # select the given columns
        output_table = output_table[self.kept_columns]

        return output_table

    # def update(self):
    #     output_table = self.in_ports[1]

    #     # drop rows with empty variable column
    #     output_table = output_table[output_table[self.selected_variable].notnull()]

    #     # select the given columns
    #     output_table = output_table[self.kept_columns]

    #     self.out_ports[1] = output_table


if __name__ == '__main__':
    id = '5057'
    relative_path = '/Users/climact/XCalc/dev/transport_prototype/metanodes/2.2 Transport' \
                    '/export_varia (#5057)/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = ExportVariableNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.build_node()
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out.csv')
