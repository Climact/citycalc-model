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
    MCD (MATCH, CALCULATE & DECOMPOSE)
    ==================================
    Match, Calculate and Decompose input data

    KNIME options implemented:
        -
"""
import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class MCDNode(PythonNode, SubNode):
    def __init__(
        self,
        output_name: str,
        operation_selection: str,
        fill_value_bool: str = "No",
        fill_value: float = 0.0,
    ):
        super().__init__()
        self.output_name = output_name
        self.operation_selection = operation_selection
        self.fill_value_bool = fill_value_bool
        self.fill_value = fill_value
        self.common_dimensions = []  # list of common dimensions
        self.var_xy = []  # list of var names
        self.how_var = ""  # string about how to merge
        self.fun = None  # function to use between var

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}
        
        model = xml_root.find(xmlns + "config[@key='model']")
        # Find values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'operation-selection-' in child.get('key'):
                    for ggrandchild in grandchild:
                        if ggrandchild.get('key') == "0":
                            value = ggrandchild.get('value')
                            kwargs['operation_selection'] = value
                if 'output-name-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['output_name'] = value
                if 'fill-selection-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['fill_value_bool'] = value
                if 'fill-value-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs['fill_value'] = float(value)

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, input_table_1, input_table_2) -> pd.DataFrame:
        # Get common dimensions
        dimensions_1 = input_table_1.select_dtypes(['int64', 'int', 'object'])
        dimensions_2 = input_table_2.select_dtypes(['int64', 'int', 'object'])
        self.common_dimensions = []
        columns_2_only = list(input_table_2.columns)
        for col in dimensions_1:
            if col in dimensions_2:
                self.common_dimensions.append(col)
                columns_2_only.remove(col)
        columns_2_only = [input_table_2.columns.get_loc(col) for col in columns_2_only]

        # Inner or Left Outer Join (optional)
        if self.fill_value_bool == "No":
            self.how_var = "inner"
        elif self.fill_value_bool == "Inner Join":
            self.how_var = "inner"
        elif self.fill_value_bool == "Left [x] Outer Join":
            self.how_var = "left"
        else:
            raise self.exception("Wrong input for flow variable")

        # self.op, input_table = merge(input_table_1, input_table_2, how=self.how_var, on=self.common_dimensions)
        input_table = input_table_1.merge(input_table_2, how=self.how_var, on=self.common_dimensions)

        df = input_table

        # Select x and y
        self.var_xy = df.select_dtypes('float64').columns
        var_x = df[self.var_xy[0]]
        var_y = df[self.var_xy[1]]

        # Fill values (optional)
        if self.fill_value_bool == "Left [x] Outer Join":
            var_x.fillna(self.fill_value, inplace=True)
            var_y.fillna(self.fill_value, inplace=True)

        # Switch on operation
        options = {
            "x + y": fun_1,
            "x - y": fun_2,
            "y - x": fun_3,
            "x * y": fun_4,
            "x / y": fun_5,
            "y / x": fun_6,
            "1 + x * y": fun_7,
            "x * (1-y)": fun_8,
            "(1-x) * y": fun_9,
            "x ^ y": fun_10,
            "y ^ x": fun_11
        }
        self.fun = options[self.operation_selection]

        # Remove input data
        df = df.drop(df[self.var_xy].columns, axis=1)

        # Define OUTPUT
        df[self.output_name] = self.fun(var_x, var_y)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        return df

    # def update(self):
    #     # Copy input to output
    #     # self.op.left = self.in_ports[1]  # No need to check for existence, we can use property directly
    #     # self.op.right = self.in_ports[2]  # No need to check for existence, we can use property directly
    #     # self.op.right = self.op.right.take(self.columns_2_only, axis=1)

    #     # Inner or Left Outer Join (optional)
    #     # FIXME should use this get_results instead of merge but getting segmentation fault
    #     # df = self.op.get_result()
    #     # df = merge(self.in_ports[1], self.in_ports[2], how=self.how_var, on=self.common_dimensions)[1]
    #     df = self.in_ports[1].merge(self.in_ports[2], how=self.how_var, on=self.common_dimensions)

    #     # Select x and y
    #     var_x = df[self.var_xy[0]]
    #     var_y = df[self.var_xy[1]]

    #     # Fill values (optional)
    #     if self.fill_value_bool == "Left [x] Outer Join":
    #         var_x.fillna(self.fill_value, inplace=True)
    #         var_y.fillna(self.fill_value, inplace=True)

    #     # Remove input data
    #     df = df.drop(df[self.var_xy].columns, axis=1)

    #     # Define OUTPUT
    #     df[self.output_name] = self.fun(var_x, var_y)
    #     df.replace([np.inf, -np.inf], np.nan, inplace=True)

    #     output_table = df

    #     self.out_ports[1] = output_table


# Define operations available for MCD
def fun_1(var_x, var_y):
    return var_x + var_y


def fun_2(var_x, var_y):
    return var_x - var_y


def fun_3(var_x, var_y):
    return var_y - var_x


def fun_4(var_x, var_y):
    return var_x * var_y


def fun_5(var_x, var_y):
    return var_x / var_y


def fun_6(var_x, var_y):
    return var_y / var_x


def fun_7(var_x, var_y):
    return 1 + var_x * var_y


def fun_8(var_x, var_y):
    return var_x * (1 - var_y)


def fun_9(var_x, var_y):
    return (1 - var_x) * var_y


def fun_10(var_x, var_y):
    return var_x ** var_y


def fun_11(var_x, var_y):
    return var_y ** var_x


# Redefine the merge for this use case
# Nothing has changed but the fact that we expose the op object
# Source : merge from pandas (pd.core.reshape.merge)
# def merge(
#         left,
#         right,
#         how: str = "inner",
#         on=None,
#         left_on=None,
#         right_on=None,
#         left_index: bool = False,
#         right_index: bool = False,
#         sort: bool = False,
#         suffixes=("_x", "_y"),
#         copy: bool = True,
#         indicator: bool = False,
#         validate=None,
# ) -> "DataFrame":
#     op = _MergeOperation(
#         left,
#         right,
#         how=how,
#         on=on,
#         left_on=left_on,
#         right_on=right_on,
#         left_index=left_index,
#         right_index=right_index,
#         sort=sort,
#         suffixes=suffixes,
#         copy=copy,
#         indicator=indicator,
#         validate=validate,
#     )
#     return op, op.get_result()


if __name__ == '__main__':
    id = '7945'
    relative_path = f'/Users/climact/patex-container/dev/_interactions/workflows/data-processing/2_1 Building (#0)/MCD (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = MCDNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in_1.csv')
    node.in_ports[2] = pd.read_csv('/Users/climact/Desktop/in_2.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_run_pycharm.csv', index=False)
    node.update()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_update_pycharm.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_update_pycharm.csv'), pd.read_csv('/Users/climact/Desktop/out_run_pycharm.csv'), )
