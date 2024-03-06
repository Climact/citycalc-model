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
    ADD MISSING YEARS NODE
    ======================
    Complete data with values for years between 2016 and 2050.
    The fill method can either use data from baseyear or a constant value.

    KNIME options implemented:
        - all
"""
import pandas as pd

from patex.nodes.node import Context, Globals, PythonNode, SubNode


class AddMissingYearsNode(PythonNode, SubNode):
    def __init__(
        self,
        fill_method_index: str = "Last available year",
        fill_value: float = 0.0,
    ):
        super().__init__()
        # FIXME original node has a baseyear, but it's never used, so I removed it
        self.fill_method_index = fill_method_index
        self.fill_value = fill_value

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            match child_key:
                case "fill-selection-766":
                    kwargs["fill_method_index"] = value
                case "fill-value-767":
                    kwargs["fill_value"] = float(value)

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def apply(self, df_data) -> pd.DataFrame:
        # Define constants
        DIMENSIONS_TYPES = ['int16', 'int32', 'int64', 'object']

        # List dimensions
        dimensions = df_data.select_dtypes(include=DIMENSIONS_TYPES).columns
        dimensions_wo_years = [d for d in dimensions if d != "Years"]

        # Add max years as default value column
        df_max_years = df_data.groupby(dimensions_wo_years)[["Years"]].max()
        df_max_years.columns = ["MaxYear"]
        df_data = df_data.merge(df_max_years.reset_index(), on=dimensions_wo_years)
        data_to_duplicate = df_data[df_data["Years"] == df_data["MaxYear"]].copy()
        del data_to_duplicate["MaxYear"]
        del df_data["MaxYear"]

        # Fill method selection (default baseyear)
        if self.fill_method_index == "Constant":
            data_to_duplicate[data_to_duplicate.select_dtypes(float).columns[0]] = self.fill_value

        # Duplicates data for missing years (condition on max year for completion)
        df_data["concat-order"] = 1
        data_to_duplicate["concat-order"] = 2
        years_list = [y for y in Globals.get().missing_years if y <= Globals.get().max_year]
        for year in years_list:
            data_to_duplicate["Years"] = year
            df_data = pd.concat([df_data, data_to_duplicate], ignore_index=True)

        # Sort on concat-order and remove duplicates
        df_data = df_data.sort_values(by=['concat-order'])
        df_data = df_data.drop_duplicates(subset=dimensions, keep='first')
        del df_data['concat-order']

        output_table = df_data.reset_index(drop=True)

        return output_table
