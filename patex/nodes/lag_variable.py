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
    Lag Variable
    ============
    Lag a variable from one year row toward the future.

    KNIME options implemented:
        - All
"""
import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class LagVariable(PythonNode, SubNode):
    def __init__(
        self,
        in_var: str,
    ):
        super().__init__()
        self.in_var = in_var

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        for child in model:
            for grandchild in child:
                if 'in-var-' in child.get('key'):
                    in_var = grandchild.get('value')

        self = PythonNode.init_wrapper(cls, in_var=in_var)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> tuple[pd.DataFrame, pd.DataFrame]:
        # Define constants
        VARIABLE = self.in_var
        rx = re.compile('(.*)\[(.*)\]')
        groups = rx.match(VARIABLE)
        LAGGED_VARIABLE = f'{groups[1]}_lagged[{groups[2]}]'

        # Lag Years column
        df_years = pd.DataFrame(df['Years'].drop_duplicates().sort_values(ascending=False))
        df_years['Timestep'] = (df_years.shift(1) - df_years)
        df_years.dropna(subset=['Timestep'], inplace=True)

        # Select variable to lag
        dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
        kept_columns = dimensions + [VARIABLE]
        df_variable_to_lag = df[kept_columns]
        df_variable_to_lag = df_variable_to_lag[df_variable_to_lag[VARIABLE].notnull()]
        for col in df_variable_to_lag.columns:
            if df_variable_to_lag[col].isnull().all():
                del df_variable_to_lag[col]
        used_dimensions = list(df_variable_to_lag.select_dtypes(['object', 'int', 'int32', 'int64']).columns)

        # Apply Timestep to lagged variable
        df_lagged_variable = df_variable_to_lag.merge(df_years, on='Years')
        df_lagged_variable['Years'] = (df_lagged_variable['Years'] + df_lagged_variable['Timestep']).astype(int)

        # Rename variable as lagged
        df_lagged_variable.rename(columns={VARIABLE: LAGGED_VARIABLE}, inplace=True)

        # Merge lagged variable to original variable
        df_lagged_variable = df_variable_to_lag.merge(df_lagged_variable, on=used_dimensions, how='left')
        df_lagged_variable['Timestep'].fillna(0, inplace=True)

        # Fill missing value (start year) with current value
        df_lagged_variable[LAGGED_VARIABLE].fillna(df_lagged_variable[VARIABLE], inplace=True)

        # Create the outputs
        output_table_1 = df_lagged_variable[used_dimensions + [LAGGED_VARIABLE]]
        output_table_2 = df_lagged_variable[used_dimensions + ['Timestep']]

        return (output_table_1, output_table_2)


if __name__ == '__main__':
    id = '7157'
    port = 2
    relative_path = f'/Users/climact/XCalc/dev/transport/workflows/transport_processing/2_2 Transpor (#0)/Lag Variable (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = LagVariable(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[port].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)

    a = pd.read_csv('/Users/climact/Desktop/out_pycharm.csv')
    dimensions = list(a.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
    a = a.sort_values(dimensions).reset_index(drop=True)
    b = pd.read_csv('/Users/climact/Desktop/out_knime.csv').sort_values(dimensions).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_like=True)
