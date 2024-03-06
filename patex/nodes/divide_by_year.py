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
    Divide by year
    ============
    Divide the selected variable by its value at selected year (by default: baseyear).

    KNIME options implemented:
        - All
"""
import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class DivideYear(PythonNode, SubNode):
    def __init__(
        self,
        in_var: str,
        output_name: str,
        reference_year: int | str,
    ):
        super().__init__()
        self.in_var = in_var
        self.output_name = output_name
        self.reference_year = reference_year

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")

        # Find values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'in-var-' in child.get('key'):
                    in_var = grandchild.get('value')
                if 'output-name-' in child.get('key'):
                    output_name = grandchild.get('value')
                if 'reference-year-' in child.get('key'):
                    reference_year = grandchild.get('value')

        self = PythonNode.init_wrapper(cls,
            in_var=in_var,
            output_name=output_name,
            reference_year=reference_year,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            for key, var in self.flow_vars_params.items():
                if self.reference_year == var:
                    self.knime_input_mapping["reference_year"] = var
        return self

    def apply(self, df, reference_year = None) -> pd.DataFrame:
        # Define constants
        VARIABLE = self.in_var
        YEAR = int(reference_year if reference_year is not None else self.reference_year)
        OUTPUT_NAME = self.output_name

        # Select variable to lag
        dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
        kept_columns = dimensions + [VARIABLE]
        df_divided = df[kept_columns]
        df_divided = df_divided[df_divided[VARIABLE].notnull()]
        for col in df_divided.columns:
            if df_divided[col].isnull().all():
                del df_divided[col]
        used_dimensions = list(df_divided.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
        used_dimensions_without_years = list(df_divided.select_dtypes(['object']).columns)

        # Select values for reference year
        df_reference = df_divided[df_divided['Years'] == YEAR].copy()
        df_reference.rename(columns={VARIABLE: 'ref_year'}, inplace=True)
        df_reference = df_reference[used_dimensions_without_years + ['ref_year']]

        # Merge reference year value to original data
        df_divided = df_divided.merge(df_reference, how='left', on=used_dimensions_without_years)

        # Divide variable by reference year
        df_divided[OUTPUT_NAME] = df_divided[VARIABLE] / df_divided['ref_year']
        df_divided.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Remove unused columns
        df_divided = df_divided[used_dimensions + [OUTPUT_NAME]]

        return df_divided


if __name__ == '__main__':
    id = '7156'
    relative_path = f'/Users/climact/XCalc/dev/transport/workflows/transport_processing/2_2 Transpor (#0)/Divide by ye (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = DivideYear(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)

    a = pd.read_csv('/Users/climact/Desktop/out_pycharm.csv')
    dimensions = list(a.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
    a = a.sort_values(dimensions).reset_index(drop=True)
    b = pd.read_csv('/Users/climact/Desktop/out_knime.csv').sort_values(dimensions).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_like=True)
