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
    NUMBER TO STRING NODE
    ============================
    KNIME options implemented:
        - Wildcards (but well regex)
"""

import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class NumberToStringNode(PythonNode, NativeNode):
    knime_name = "Number To String"

    def __init__(
        self,
        filter_type: str,
        col_names: list[str] | None = None,
        col_regex: str | None = None,
    ):
        if filter_type == "STANDARD":
            if col_names is None:
                raise ValueError("`col_names` must be set when filter type is STANDARD")
            if col_regex is not None:
                raise ValueError("`col_regex` cannot be set when filter type is STANDARD")
        elif filter_type == "REGEX":
            if col_regex is None:
                raise ValueError("`col_regex` must be set when filter type is REGEX")
            if col_names is not None:
                raise ValueError("`col_names` cannot be set when filter type is REGEX")
        else:
            raise ValueError("`filter_type` must be either 'STANDARD' or 'REGEX'")
            
        super().__init__()
        self.filter_type = filter_type
        self.col_names = col_names
        self.col_regex = re.compile(col_regex) if col_regex is not None else None

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")

        include = model.find(xmlns + "config[@key='include']")
        # Try to find the filter-type. If doesn't exist (deprecated node) : STANDARD
        try:
            filter_type = include.find(xmlns + "entry[@key='filter-type']").get('value')
        except AttributeError:
            filter_type = "STANDARD"

        # Filter type "STANDARD" (columns selection)
        if filter_type == "STANDARD":
            if include.find(xmlns + "config[@key='InclList']") is not None:  # Old version of the node (deprecated)
                inclList = include.find(xmlns + "config[@key='InclList']")
            else:  # New version of the node
                inclList = include.find(xmlns + "config[@key='included_names']")
            totalNrbCol = inclList.find(xmlns + "entry[@key='array-size']").get('value')
            # colNameList = []
            col_names = []
            for i in range(0, int(totalNrbCol)):
                colName = inclList.find(xmlns + "entry[@key='" + str(i) + "']").get('value')
                col_names.append(colName)
            kwargs["col_names"] = col_names

        # Filter type "name_pattern" (columns selection by regex or wildcards)
        elif filter_type == "name_pattern":
            name_pattern = include.find(xmlns + "config[@key='name_pattern']")
            type = name_pattern.find(xmlns + "entry[@key='type']").get('value')

            # Filter type "regex"
            if type == "Regex":
                filter_type = "REGEX"
                kwargs["col_regex"] = name_pattern.find(xmlns + "entry[@key='pattern']").get('value')

        self = PythonNode.init_wrapper(cls,
            filter_type=filter_type,
            **kwargs
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        match self.filter_type:
            case "STANDARD":
                for col_name in self.col_names:
                    df.loc[:, col_name] = df.loc[:, col_name].astype(str)
            case "REGEX":
                columns_list = [i for i in df.columns if self.col_regex.match(i)]
                for col in columns_list:
                    df.loc[:, col] = df.loc[:, col].astype(str)

        return df


if __name__ == "__main__":
    my_id = '5829'
    xml_file = '/Users/climact/XCalc/dev/Test/Number To String (#5829)/settings.xml'
    node_type = 'NativeNode'
    node = NumberToStringNode(my_id, xml_file, node_type)
    node.run()
