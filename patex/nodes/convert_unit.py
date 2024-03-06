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
    Convert Unit
    ============
    Convert unit of the selected variable.
    
    KNIME options implemented:
        - All
"""
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class ConvertUnit(PythonNode, SubNode):
    def __init__(
        self,
        in_var: str,
        output_unit: str,
        conversion_factor: float,
    ):
        super().__init__()
        self.in_var = in_var
        self.output_unit = output_unit
        self.conversion_factor = conversion_factor

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        for child in model:
            for grandchild in child:
                if 'in-var-' in child.get('key'):
                    value = grandchild.get('value')
                    in_var = value
                if 'output-unit-' in child.get('key'):
                    value = grandchild.get('value')
                    output_unit = value
                if 'conversion-factor-' in child.get('key'):
                    value = grandchild.get('value')
                    conversion_factor = float(value)

        self = PythonNode.init_wrapper(cls,
            in_var=in_var,
            output_unit=output_unit,
            conversion_factor=conversion_factor,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        # Define constants
        IN_VAR = self.in_var
        CONVERSION_FACTOR = self.conversion_factor

        # Apply conversion
        df[IN_VAR] = df[IN_VAR] * CONVERSION_FACTOR

        # Rename column
        column_name = IN_VAR.split('[')[0] + '[' + self.output_unit + ']'
        df.rename(columns={IN_VAR: column_name}, inplace=True)

        return df


if __name__ == '__main__':
    id = '680'
    relative_path = f'/Users/climact/XCalc/dev/prototype/Workflows/generic_flow/Convert Unit (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = ConvertUnit(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)
