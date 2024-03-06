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
    Filter dimension
    ================
    Filter some values from a given dimension with a selected mode.
    If no dimensions or wrong dimensions are displayed : run the node and then re-open the configuration panel.

    KNIME options implemented:
        - All
"""
import re

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class FilterDimension(PythonNode, SubNode):
    def __init__(
        self,
        dimension: str,
        operation_selection: str | None = None,
        value_years: int | str | None = None,
        values_dimension: list | None = None,
        mode_selection: str | None = None,
    ):
        if dimension == "Years":
            if operation_selection is None or value_years is None:
                raise ValueError("when dimension is `Years`, `operation_selection` and `value_years` must be specified")
            if values_dimension is not None or mode_selection is not None:
                raise ValueError("when dimension is `Years`, `values_dimension` and `mode_selection` must be None")
        else:
            if values_dimension is None or mode_selection is None:
                raise ValueError("when dimension is not `Years`, `values_dimension` and `mode_selection` must be specified")
            if operation_selection is not None or value_years is not None:
                raise ValueError("when dimension is not `Years`, `operation_selection` and `value_years` must be None")

        super().__init__()
        self.dimension = dimension
        self.operation_selection = operation_selection
        self.value_years = value_years
        self.values_dimension = values_dimension
        self.mode_selection = mode_selection

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}
        
        model = xml_root.find(xmlns + "config[@key='model']")
        rx = re.compile('[0-9]+')
        for child in model:
            for grandchild in child:
                if child.get('key').startswith('dimension-'):
                    for ggrandchild in grandchild:
                        if "0" == ggrandchild.get('key'):
                            value = ggrandchild.get('value')
                    kwargs["dimension"] = value
                # Find values when dimension is years
                if kwargs.get("dimension") == "Years":
                    if 'operation-selection-' in child.get('key'):
                        for ggrandchild in grandchild:
                            if "0" == ggrandchild.get('key'):
                                value = ggrandchild.get('value')
                        kwargs["operation_selection"] = value
                    elif 'value-years-' in child.get('key'):
                        value = grandchild.get('value')
                        kwargs["value_years"] = value
                # Find values when dimension is not years
                else:
                    if 'values-dimension-' in child.get('key'):
                        value = []
                        for ggrandchild in grandchild:
                            if rx.match(ggrandchild.get('key')):
                                value.append(ggrandchild.get('value'))
                        kwargs["values_dimension"] = value
                    elif 'mode-selection-' in child.get('key'):
                        for ggrandchild in grandchild:
                            if "0" == ggrandchild.get('key'):
                                value = ggrandchild.get('value')
                        kwargs["mode_selection"] = value

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            for key, var in self.flow_vars_params.items():
                if self.value_years == var:
                    self.knime_input_mapping["value_years"] = var
        return self

    def apply(self, df, value_years = None) -> tuple[pd.DataFrame, pd.DataFrame]:

        # with open("filter_dimension_output_new.txt", "a") as f:
        #     print({
        #         "id": self.id,
        #         "dimension": self.dimension,
        #         "operation_selection": self.operation_selection,
        #         "value_years": self.value_years,
        #         "values_dimension": self.values_dimension,
        #         "mode_selection": self.mode_selection,
        #     }, file=f)

        # Define constants
        DIMENSION = self.dimension
        if DIMENSION == 'Years':
            OPERATION_SELECTION = self.operation_selection
            # Change value for year parameter dynamically if linked to a flow variable
            VALUE_YEARS = int(value_years if value_years is not None else self.value_years)

            # Compute mask
            if OPERATION_SELECTION == '=':
                mask = (df[DIMENSION] == VALUE_YEARS)
            elif OPERATION_SELECTION == '≠':
                mask = (df[DIMENSION] != VALUE_YEARS)
            elif OPERATION_SELECTION == '≥':
                mask = (df[DIMENSION] >= VALUE_YEARS)
            elif OPERATION_SELECTION == '>':
                mask = (df[DIMENSION] > VALUE_YEARS)
            elif OPERATION_SELECTION == '≤':
                mask = (df[DIMENSION] <= VALUE_YEARS)
            elif OPERATION_SELECTION == '<':
                mask = (df[DIMENSION] < VALUE_YEARS)

        else:
            VALUES_DIMENSION = self.values_dimension
            MODE_SELECTION = self.mode_selection

            # Convert VALUES_DIMENSION to numeric if DIMENSION column is numeric
            if np.issubdtype(df[DIMENSION].dtype, np.number):
                VALUES_DIMENSION = pd.to_numeric(VALUES_DIMENSION)

            # Compute mask
            if MODE_SELECTION == 'Exclude':
                mask = (~df[DIMENSION].isin(VALUES_DIMENSION))
            elif MODE_SELECTION == 'Include':
                mask = (df[DIMENSION].isin(VALUES_DIMENSION))
        
        # Filter out values
        return (df[mask], df[~mask])


if __name__ == '__main__':
    id = '7164'
    relative_path = f'/Users/climact/XCalc/dev/prototype/Workflows/generic_flow/Filter dimen (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = FilterDimension(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm_1.csv', index=False)
    node.out_ports[2].to_csv('/Users/climact/Desktop/out_pycharm_2.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_pycharm_1.csv'), pd.read_csv('/Users/climact/Desktop/out_1_python.csv'))
    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_pycharm_2.csv'), pd.read_csv('/Users/climact/Desktop/out_2_python.csv'))
