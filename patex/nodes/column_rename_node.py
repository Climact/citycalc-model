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
    COLUMN RENAME NODE
    ============================
    KNIME options NOT implemented:
        - new column type
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class ColumnRenameNode(PythonNode, NativeNode):
    knime_name = "Column Rename"

    def __init__(self, columns_rename: dict[str, str]):
        super().__init__()
        self.columns_rename = columns_rename

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {"columns_rename": {}}

        model = xml_root.find(xmlns + "config[@key='model']")
        xml_columns = model.find(xmlns + "config[@key='all_columns']").findall(xmlns + "config")

        for xml_column in xml_columns:
            old_name = xml_column.find(xmlns + "entry[@key='old_column_name']").get('value')
            new_name = xml_column.find(xmlns + "entry[@key='new_column_name']").get('value')
            new_type = xml_column.find(xmlns + "entry[@key='new_column_type']").get('value')
            if ' ' in new_name:
                cls.knime_error(id, f'Remove space in column name "{new_name}"')
            if new_type != '0':
                cls.knime_error(id, f"The type of the old column '{old_name}' cannot be changed")
            kwargs["columns_rename"][old_name] = new_name

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        self.debug("This node may cause problem when using Manual Selection afterwards")
        return df.rename(index=str, columns=self.columns_rename)
