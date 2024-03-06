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
    USE VARIABLE NODE
    =================
    Allow to choose which variable will be used from a given dataset.

    KNIME options implemented:
        - all
"""
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class UseVariableNode(PythonNode, SubNode):
    def __init__(self, selected_variable: str):
        super().__init__()
        self.selected_variable = selected_variable

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")

        # Find values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'selected-variable-' in child.get('key'):
                    value = grandchild.get('value')
                    selected_variable = value

        self = PythonNode.init_wrapper(cls, selected_variable=selected_variable)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table

        # Force Years to be integer
        if "Years" in output_table.columns:
            output_table["Years"] = output_table["Years"].astype('int')

        # Select dimensions
        dimensions = list(output_table.select_dtypes(['object', 'int', 'int32', 'int64']).columns)

        # Add the selected variable
        self.kept_columns = dimensions + [self.selected_variable]

        # Select the given columns
        output_table = output_table[self.kept_columns]

        # Drop rows with empty variable column
        output_table = output_table[output_table[self.selected_variable].notnull()]

        # Drop empty dimension columns
        if output_table.empty:
            return output_table
        
        self.dropped_columns = []
        for col in output_table.columns:
            if output_table[col].isnull().all():
                self.dropped_columns.append(col)
                del output_table[col]

        return output_table

    # def update(self):
    #     # Select the given columns
    #     output_table = self.in_ports[1][self.kept_columns]

    #     # Drop rows with empty variable column
    #     output_table = output_table[output_table[self.selected_variable].notnull()]

    #     # Drop empty dimension columns
    #     for col in self.dropped_columns:
    #         del output_table[col]

    #     self.out_ports[1] = output_table


if __name__ == '__main__':
    id = '4874'
    relative_path = '/Users/climact/XCalc/dev/transport_prototype/metanodes/2.2 Transport' \
                    '/use_variable (#4874)/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = UseVariableNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.build_node()
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out.csv')
