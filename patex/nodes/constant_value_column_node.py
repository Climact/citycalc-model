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
    CONSTANT VALUE COLUMN NODE
    ============================
    KNIME options implemented:
        - Column settings : append, Value settings (string, double, integer)
    KNIME options implemented:
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class ConstantValueColumnNode(PythonNode, NativeNode):
    knime_name = "Constant Value Column"

    def __init__(
        self,
        column_value: str,
        new_column_name: str,
        column_type: str,
    ):
        super().__init__()
        self.column_value = column_value
        self.new_column_name = new_column_name
        self.column_type = column_type

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        replaced_column = model.find(xmlns + "entry[@key='replaced-column']").get('isnull') == 'true'
        column_value = model.find(xmlns + "entry[@key='column-value']").get('value')
        new_column_name = model.find(xmlns + "entry[@key='new-column-name']").get('value')
        column_type = model.find(xmlns + "entry[@key='column-type']").get('value')

        option = ['INT', 'STRING', 'DOUBLE']

        if not replaced_column:
            cls.knime_error(id,
                "The option 'Replace' of the Constant Value Column Knime node is currently "
                "not implemented in the Python converter.")
        elif replaced_column:
            if column_type not in option:
                cls.knime_error(id,
                    "This type of column is currenlty not implemented in the "
                    "Python converter.")
        else:
            cls.knime_error(id,
                "Add this type of constant value is currently not implemented"
                " in the Python converter.")

        self = PythonNode.init_wrapper(cls,
            column_value=column_value,
            new_column_name=new_column_name,
            column_type=column_type,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            self.knime_input_mapping["value"] = self.column_value
        return self

    def apply(self, df, value = None) -> pd.DataFrame:
        # FIXME : there is no check on the data type. If a xml is manually edited, an incoherence between data type
        #  and column value can exist.
        value = value if value is not None else self.column_value

        if self.column_type == 'STRING':
            df[self.new_column_name] = value
        elif self.column_type == "DOUBLE":
            try:
                df[self.new_column_name] = float(value)
            except ValueError:
                # FIXME I don't understand how this can happen?
                raise
                # self.warning("Convert string into flow variable in Constant Value")
                # df[self.new_column_name] = float(self.flow_vars[self.column_value])
        else:
            df[self.new_column_name] = int(value)

        return df
