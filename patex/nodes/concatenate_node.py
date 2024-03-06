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
    CONCATENATE NODE
    ============================
    KNIME options implemented:
        - Nothing, except for outer join, with no duplicates in row_ids
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class ConcatenateNode(PythonNode, NativeNode):
    knime_name = "Concatenate"

    def __init__(
        self,
        suffix: str,
        fail_on_duplicates: bool,
    ):
        super().__init__()
        self.suffix = suffix
        self.fail_on_duplicates = fail_on_duplicates

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        append_suffix = model.find(xmlns + "entry[@key='append_suffix']").get('value')
        suffix = model.find(xmlns + "entry[@key='suffix']").get('value')
        fail_on_duplicates = model.find(xmlns + "entry[@key='fail_on_duplicates']").get('value') == 'true'
        intersection_of_cols = model.find(xmlns + "entry[@key='intersection_of_columns']").get('value')
        enable_hiliting = model.find(xmlns + "entry[@key='enable_hiliting']").get('value')

        if enable_hiliting != "false" and intersection_of_cols == "false":
            cls.knime_error(id, "Hiliting has not been implemented.")
        if intersection_of_cols != "false" and enable_hiliting == "false":
            cls.knime_error(id, "Intersection of columns has not been implemented.")
        if intersection_of_cols != "false" and enable_hiliting != "false":
            cls.knime_error(id, "Hiliting and intersection of columns has not been implemented.")
        if not fail_on_duplicates and append_suffix != 'true':
            cls.knime_error(id, 
                "Skip Rows option is not implemented in the Python converter. Exception occurend in Concatenate node : ")

        self = PythonNode.init_wrapper(cls, suffix=suffix, fail_on_duplicates=fail_on_duplicates)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df_left, df_right) -> pd.DataFrame:
        if self.fail_on_duplicates:
            cdf = pd.concat([df_left, df_right], join='outer')
            ddf = cdf.duplicated()
            if True in ddf:
                raise self.exception("Concatenation failed in Concatenate Node because duplicate row IDs are encountered.")
            else:
                # this path is not tested in test_concatenate_node.py
                df_out = pd.concat([df_left, df_right], join='outer', sort=False)
        else:
            df_right = df_right.set_index(df_right.index.astype(str) + self.suffix)
            df_out = pd.concat([df_left, df_right], join='outer', sort=False)

        return df_out
