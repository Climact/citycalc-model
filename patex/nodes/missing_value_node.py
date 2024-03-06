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
    MISSING VALUE NODE
    ============================
    KNIME options implemented:
        - Linear interpolation and Fixed, Previous, Next Value for double and integer. Column settings not implemented.
        Fillna not available for string and replace value can only be 0.
"""

import re

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class MissingValueNode(PythonNode, NativeNode):
    knime_name = "Missing Value"

    def __init__(
        self,
        DTS_DT_O: list[str],
        FixedValue: str,
        missing_values_by_columns: list[str] = [],
        dimension_rx: str | None = None,
    ):
        if any("String" in DTS[0] and "Fixed" in DTS[1] for DTS in DTS_DT_O):
            if dimension_rx is None:
                raise ValueError("`dimension_rx` must be specified for fixed strings")

        super().__init__()
        self.dimension_rx = re.compile(dimension_rx) if dimension_rx is not None else None
        self.DTS_DT_O = DTS_DT_O
        self.FixedValue = FixedValue
        self.missing_values_by_columns = missing_values_by_columns

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        missing_values_by_columns = []

        model = xml_root.find(xmlns + "config[@key='model']")
        columnsettings = model.find(xmlns + "config[@key='columnSettings']")
        temp = columnsettings.findall(xmlns + "config")
        # if temp != []:
        #     raise cls.knime_exception(id, 
        #         "Options have been defined in Column Settings tab of the Missing Value node but this is not implemented in the Python converter")
        for columnsetting in columnsettings:
            colNames = columnsetting.find(xmlns + "config[@key='colNames']").findall(xmlns + "entry")
            cols = []
            for colName in colNames:
                if colName.get('key') != 'array-size':
                    cols.append(colName.get("value"))
            Option = columnsetting.find(xmlns + "config[@key='settings']").find(xmlns + 'entry').get("value")
            if "Previous" in Option:
                missing_values_by_columns.append([cols, "Previous"])
            else:
                cls.knime_error(id, f"Filling missing values with {str(Option)} option hasn't been implemented in the Python converter.")

        DataTypeSettings = model.find(xmlns + "config[@key='dataTypeSettings']").findall(xmlns + "config")

        dimension_rx = None
        DTS_DT_O = []
        FixedValue = None
        FixedValue_to_compare = None
        for DataTypeSetting in DataTypeSettings:
            Setting = DataTypeSetting.find(xmlns + "config")
            DataType = DataTypeSetting.get('key')
            Option = DataTypeSetting.find(xmlns + "entry").get('value')
            d = [DataType, Option]
            DTS_DT_O.append(d)
            # Fixed Value Option
            if "DoNothing" in Option:
                pass
            elif "Double" in DataType and "Fixed" in Option:
                FixedValue = Setting.find(xmlns + "entry").get('value')
            elif "Int" in DataType and "Fixed" in Option:
                FixedValue_to_compare = Setting.find(xmlns + "entry").get('value')
            elif "String" in DataType and "Fixed" in Option:
                dimension_rx = "^.*\[.*•\]$"
                FixedValue_to_compare = Setting.find(xmlns + "entry").get('value')
            # Interpolation Option
            elif "Int" in DataType and "Interpolation" in Option:
                pass
            elif "Double" in DataType and "Interpolation" in Option:
                pass
            # Previous Value Option
            elif "Int" in DataType and "Previous" in Option:
                pass
            elif "Double" in DataType and "Previous" in Option:
                pass
            # Next Value Option
            elif "Int" in DataType and "Next" in Option:
                pass
            elif "Double" in DataType and "Next" in Option:
                pass
            elif "Int" not in DataType and "Double" not in DataType:
                cls.knime_error(id, f"Missing value node options related the data type '{str(DataType)}' haven't been implemented in the Python converter.")
            else:
                cls.knime_error(id, f"Filling missing values with {str(Option)} option hasn't been implemented in the Python converter.")

        if FixedValue is not None and FixedValue_to_compare is not None:
            if FixedValue != str(float(FixedValue_to_compare)):
                raise self.exception("It has not been implemented to specify different values for the fixed value in Missing value node.")
        if FixedValue is None and FixedValue_to_compare is not None:
            FixedValue = FixedValue_to_compare

        self = PythonNode.init_wrapper(cls,
            dimension_rx=dimension_rx,
            DTS_DT_O=DTS_DT_O,
            FixedValue=FixedValue,
            missing_values_by_columns=missing_values_by_columns,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        for col in df:
            if df[col].dtype != np.number:
                if df[col].isna().sum() != 0:
                    self.debug("Some string columns contains Missing value. This case is not implemented in Missing Value node")

        for list in self.missing_values_by_columns:
            cols = list[0]
            option = list[1]
            if option == 'Previous':
                df[cols] = df[cols].fillna(method='ffill')

        for DTS in self.DTS_DT_O:
            if "DoNothing" in DTS[1]:
                pass
            elif "String" in DTS[0] and "Fixed" in DTS[1]:
                na_dimensions = [c for c in df.columns[df.isna().all()].to_list() if not self.dimension_rx.match(c)]
                string_columns = set(df.select_dtypes("object").columns.to_list() + na_dimensions)
                other_columns = [c for c in df.columns if c not in string_columns]
                df = pd.concat([df[other_columns],
                                df[string_columns].fillna((self.FixedValue))], axis=1)
            else:
                if "IntCell" in DTS[0]:
                    numerics = ['int16', 'int32', 'int64']
                else:
                    numerics = ['float16', 'float32', 'float64']
                cols = [col for col in df.columns if df[col].dtype in numerics]
                if "Fixed" in DTS[1]:  # Fixed and other than string
                    self.FixedValue = float(self.FixedValue)
                    df[cols] = df[cols].fillna((self.FixedValue))
                # Interpolation Option
                elif "Interpolation" in DTS[1]:
                    df[cols] = df[cols].interpolate(method='linear')
                # Previous Value Option
                elif "Previous" in DTS[1]:
                    df[cols] = df[cols].fillna(method='ffill')
                # Next Value Option
                elif "Next" in DTS[1]:
                    df[cols] = df[cols].fillna(method='bfill')

        return df
