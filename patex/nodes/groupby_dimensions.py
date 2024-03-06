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
    GroupBy dimensions
    ==================
    Group the variables by selected dimensions.
    If no dimensions or wrong dimensions are displayed : run the node and then re-open the configuration panel.

    KNIME options implemented:
        - All
"""
import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class GroupByDimensions(PythonNode, SubNode):
    def __init__(
        self,
        groupby_dimensions: str,
        aggregation_method: str,
    ):
        super().__init__()
        self.groupby_dimensions = groupby_dimensions
        self.aggregation_method = aggregation_method
        self.groups = []
        self.aggregation_variables = []

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")

        # Find values of the flow variables provided by user
        rx = re.compile('[0-9]+')
        for child in model:
            for grandchild in child:
                if 'groupby-dimensions-' in child.get('key'):
                    value = []
                    for ggrandchild in grandchild:
                        if rx.match(ggrandchild.get('key')):
                            value.append(ggrandchild.get('value'))
                    groupby_dimensions = value
                if 'agg-method-' in child.get('key'):
                    for ggrandchild in grandchild:
                        if "0" in ggrandchild.get('key'):
                            value = ggrandchild.get('value')
                    aggregation_method = value

        self = PythonNode.init_wrapper(
            cls,
            groupby_dimensions=groupby_dimensions,
            aggregation_method=aggregation_method,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        # Define constants
        GROUPBY_DIMENSION = self.groupby_dimensions

        # Compute column lists
        self.groups = [col for col in df.select_dtypes(exclude='float64').columns if col in GROUPBY_DIMENSION]
        self.aggregation_variables = [col for col in df.select_dtypes('float64').columns]

        # Group by
        df_groupby = df.groupby(by=self.groups, as_index=False, dropna=False)[self.aggregation_variables]
        if self.aggregation_method == "Sum":
            df = df_groupby.sum(min_count=1)
        elif self.aggregation_method == "Mean":
            df = df_groupby.mean()
        elif self.aggregation_method == "Maximum":
            df = df_groupby.max(numeric_only=True)
        elif self.aggregation_method == "Minimum":
            df = df_groupby.min(numeric_only=True)

        # Define OUTPUT
        return df

    # def update(self):
    #     df = self.in_ports[1]

    #     # Group by
    #     df_groupby = df.groupby(by=self.groups, as_index=False, dropna=False)[self.aggregation_variables]
    #     if self.aggregation_method == "Sum":
    #         df = df_groupby.sum(min_count=1)
    #     elif self.aggregation_method == "Mean":
    #         df = df_groupby.mean()
    #     elif self.aggregation_method == "Maximum":
    #         df = df_groupby.max(numeric_only=True)
    #     elif self.aggregation_method == "Minimum":
    #         df = df_groupby.min(numeric_only=True)

    #     # Define OUTPUT
    #     output_table = df

    #     self.out_ports[1] = output_table


if __name__ == '__main__':
    id = '684'
    relative_path = f'/Users/climact/XCalc/dev/prototype/Workflows/generic_flow/GroupBy dime (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = GroupByDimensions(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_pycharm.csv'), pd.read_csv('/Users/climact/Desktop/out_python.csv'))
