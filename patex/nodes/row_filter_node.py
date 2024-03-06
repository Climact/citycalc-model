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
    ROW FILTER NODE
    ============================
    KNIME options implemented:
        - Everything except deep filtering, search with wild cards,
                    filtering by missing values, filtering by RowID
"""

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class RowFilterNode(PythonNode, NativeNode):
    knime_name = "Row Filter"

    def __init__(
        self,
        filter_type: str,
        include: bool,
        that_column: str | None = None,
        pattern: str | None = None,
        case_sensitive: bool | None = None,
        is_reg_exp: bool | None = None,
        lower_bound_bool: bool | None = None,
        lower_bound: float | None = None,
        upper_bound_bool: bool | None = None,
        upper_bound: float | None = None,
        start_index: int | None = None,
        end_index: int | None = None,
    ):
        super().__init__()
        self.filter_type = filter_type
        self.include = include
        self.that_column = that_column
        self.pattern = pattern
        self.case_sensitive = case_sensitive
        self.is_reg_exp = is_reg_exp
        self.lower_bound_bool = lower_bound_bool
        self.lower_bound = lower_bound
        self.upper_bound_bool = upper_bound_bool
        self.upper_bound = upper_bound
        self.start_index = start_index
        self.end_index = end_index

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        config = model.find(xmlns + "config[@key='rowFilter']")
        filter_type = config.find(xmlns + "entry[@key='RowFilter_TypeID']").get('value')

        if filter_type == 'StringComp_RowFilter':
            kwargs["that_column"] = config.find(xmlns + "entry[@key='ColumnName']").get('value')
            kwargs["include"] = config.find(xmlns + "entry[@key='include']").get('value') == 'true'
            kwargs["pattern"] = config.find(xmlns + "entry[@key='Pattern']").get('value')
            kwargs["case_sensitive"] = config.find(xmlns + "entry[@key='CaseSensitive']").get('value') == 'true'
            kwargs["is_reg_exp"] = config.find(xmlns + "entry[@key='isRegExpr']").get('value') == 'true'

            deep_filtering = config.find(xmlns + "entry[@key='deepFiltering']").get('value')
            if deep_filtering == 'true':
                self.logger.error(
                    "Deep Filtering has not been implemented for row filtering! Node's xml file is: {}".format(
                        xml_file))

            has_wild_cards = config.find(xmlns + "entry[@key='hasWildCards']").get('value')
            if has_wild_cards == 'true':
                self.logger.error(
                    "Wild cards search not implemented for row filtering! Node's xml file is: {}".format(xml_file))

        elif filter_type == 'RangeVal_RowFilter':
            kwargs["that_column"] = config.find(xmlns + "entry[@key='ColumnName']").get('value')
            kwargs["include"] = config.find(xmlns + "entry[@key='include']").get('value') == 'true'

            type_lowerbound = config.find(xmlns + "config[@key='lowerBound']").find(xmlns + "entry[@key='datacell']").get('value').split('.')[-1]
            type_upperbound = config.find(xmlns + "config[@key='upperBound']").find(xmlns + "entry[@key='datacell']").get('value').split('.')[-1]

            kwargs["lower_bound_bool"] = True
            kwargs["upper_bound_bool"] = True
            try:
                kwargs["lower_bound"] = config.find(xmlns + "config[@key='lowerBound']").find(
                    xmlns + "config[@key='org.knime.core.data.def." + type_lowerbound + "']").find(
                    xmlns + "entry[@key='" + type_lowerbound + "']").get('value')
            except AttributeError:
                kwargs["lower_bound"] = -10 ^ 10
                kwargs["lower_bound_bool"] = False
            try:
                kwargs["upper_bound"] = config.find(xmlns + "config[@key='upperBound']").find(
                    xmlns + "config[@key='org.knime.core.data.def." + type_upperbound + "']").find(
                    xmlns + "entry[@key='" + type_upperbound + "']").get('value')
            except AttributeError:
                kwargs["upper_bound"] = 10 ^ 10
                kwargs["upper_bound_bool"] = False

            deep_filtering = config.find(xmlns + "entry[@key='deepFiltering']").get('value')
            if deep_filtering == 'true':
                self.logger.error(
                    "Deep Filtering has not been implemented for row filtering! Node's xml file is: " + xml_file)

        elif filter_type == 'MissingVal_RowFilter':
            kwargs["that_column"] = config.find(xmlns + "entry[@key='ColumnName']").get('value')
            kwargs["include"] = config.find(xmlns + "entry[@key='include']").get('value') == 'true'

        elif filter_type == 'RowNumber_RowFilter':
            kwargs["include"] = config.find(xmlns + "entry[@key='include']").get('value') == 'true'
            kwargs["start_index"] = int(config.find(xmlns + "entry[@key='RowRangeStart']").get('value'))
            kwargs["end_index"] = int(config.find(xmlns + "entry[@key='RowRangeEnd']").get('value'))

        elif filter_type == 'RowID_RowFilter':
            self.logger.error(
                "Include or exclude row by ID has not been implemented! Node's xml file is: {}".format(xml_file))

        self = PythonNode.init_wrapper(cls, filter_type=filter_type, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            self.knime_input_mapping["pattern"] = self.flow_vars_params['pattern']
        return self

    def apply(self, df, pattern = None) -> pd.DataFrame:
        if self.filter_type == 'StringComp_RowFilter':
            pattern = pattern if pattern is not None else self.pattern
            that_column_type = df[self.that_column].dtype
            if df[self.that_column].dtype.name != "category": # category type does not exist in numpy dtypes
                pattern = np.dtype(that_column_type).type(pattern)  # Cast pattern into the right type for comparison

            if not self.is_reg_exp:
                if self.case_sensitive:
                    if np.issubdtype(df[self.that_column].dtype, np.number):
                        raise Exception("Numeric filtering is only available with case sensitive match. Node's xml "
                                        "file is: {}".format(self.xml_file))
                    if self.include:
                        df = df[df[self.that_column].str.lower() == pattern.lower()]
                    else:
                        df = df[df[self.that_column].str.lower() != pattern.lower()]
                else:
                    if self.include:
                        df = df[df[self.that_column] == pattern]
                    else:
                        df = df[df[self.that_column] != pattern]
            else:
                df_data_type = df[self.that_column].dtype.name
                if df_data_type == 'category':
                    df_data_type = 'object'
                if np.issubdtype(df_data_type, np.number):
                    raise Exception("Numeric filtering is only available without normal match (no regex). Node's xml "
                                    "file is: {}".format(self.xml_file))
                if self.case_sensitive:
                    if self.include:
                        df = df.loc[df[self.that_column].str.contains("(?i)"+pattern)] # The (?i) in the regex pattern tells the re module to ignore case
                    else:
                        df = df.loc[~df[self.that_column].str.contains("(?i)"+pattern)] # The (?i) in the regex pattern tells the re module to ignore case
                else:
                    if self.include:
                        df = df.loc[df[self.that_column].str.contains(pattern)]
                    else:
                        df = df.loc[~df[self.that_column].str.contains(pattern)]

        elif self.filter_type == 'RangeVal_RowFilter':
            that_column_type = df[self.that_column].dtype

            if that_column_type == 'float64' or that_column_type == 'float16' or that_column_type == 'float32':
                pass
            elif that_column_type == 'int32' or that_column_type == 'int64' or that_column_type == 'int8' or that_column_type == 'int16' or that_column_type == 'uint32' or that_column_type == 'uint64' or that_column_type == 'uint8' or that_column_type == 'uint16':
                pass
            else:
                raise Exception("Column's datatype {} is neither float64, nor int32, nor int64! Node's xml file is: {}".format(that_column_type,self.xml_file))

            self.lower_bound = np.dtype(that_column_type).type(self.lower_bound)  # Cast bound into the right type for comparison
            self.upper_bound = np.dtype(that_column_type).type(self.upper_bound)

            if self.include:
                if not self.lower_bound_bool:
                    df = df[(df[self.that_column] <= self.upper_bound)]
                elif not self.upper_bound_bool:
                    df = df[(df[self.that_column] >= self.lower_bound)]
                else:
                    df = df[(df[self.that_column] >= self.lower_bound) & (df[self.that_column] <= self.upper_bound)]
            else:
                if not self.lower_bound_bool:
                    df = df[(df[self.that_column] > self.upper_bound)]
                elif not self.upper_bound_bool:
                    df = df[(df[self.that_column] < self.lower_bound)]
                else:
                    df = df[(df[self.that_column] > self.upper_bound) | (df[self.that_column] < self.lower_bound)]

        elif self.filter_type == 'MissingVal_RowFilter':
            if self.include:
                df = df[df[self.that_column].isnull()]
            else:
                df = df.dropna(subset=[self.that_column], axis=0)

        elif self.filter_type == 'RowNumber_RowFilter':
            if self.include:
                if self.end_index == -1:
                    df = df.iloc[list(range(self.start_index, len(df.index)))]
                else:
                    df = df.iloc[list(range(self.start_index, self.end_index+1))]
            else:
                if self.end_index >= len(df.index)-1 or self.end_index == -1:

                    if self.start_index == 0:
                        df = None
                    else:
                        df = df.iloc[list(range(0, self.start_index))]
                else:

                    if self.start_index == 0:
                        df = df.iloc[list(range(self.end_index+1, len(df.index)))]
                    else:
                        df = df.iloc[list(range(0, self.start_index))+list(range(self.end_index+1, len(df.index)))]

        elif self.filter_type == 'RowID_RowFilter':
            raise Exception("Include or exclude row by ID has not been implemented! Node's xml file is: {}".format(self.xml_file))

        return df