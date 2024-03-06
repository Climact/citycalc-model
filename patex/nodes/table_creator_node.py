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
    TABLE CREATOR NODE
    ============================
    KNIME options implemented:
        - Everything except column skipping, missing value settings, domain settings, and formatting,
                also need to make sure that the dataframe columns get the correct dtype assigned to them
"""

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class TableCreatorNode(PythonNode, NativeNode):
    knime_name = "Table Creator"

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df

    def init_ports(self):
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        row_indices = model.find(xmlns + "config[@key='rowIndices']")
        col_indices = model.find(xmlns + "config[@key='columnIndices']")
        vals = model.find(xmlns + "config[@key='values']")

        n_elements = row_indices.find(xmlns + "entry[@key='array-size']").get('value')
        n_elements = int(n_elements)
        row_arr = []
        col_arr = []
        val_arr = []

        for i in list(range(0,n_elements)):
            temp_str = str(i)
            i_th_row_entry = row_indices.find(xmlns + "entry[@key='" + temp_str + "']").get('value')
            i_th_col_entry = col_indices.find(xmlns + "entry[@key='" + temp_str + "']").get('value')
            i_th_val_entry = vals.find(xmlns + "entry[@key='" + temp_str + "']").get('value')

            row_arr.append(int(i_th_row_entry))
            col_arr.append(int(i_th_col_entry))
            val_arr.append(i_th_val_entry)

        n_rows = np.max(row_arr) + 1

        row_id_prefix = model.find(xmlns + "entry[@key='rowIdPrefix']").get('value')
        row_id_suffix = model.find(xmlns + "entry[@key='rowIdSuffix']").get('value')
        row_id_start_value = model.find(xmlns + "entry[@key='rowIdStartValue']").get('value')
        row_id_start_value = int(row_id_start_value)

        row_labels = []
        for i in list(range(row_id_start_value,row_id_start_value + n_rows)):
            row_labels.append(row_id_prefix + str(i) + row_id_suffix)

        col_labels = []
        col_types = {}
        column_properties = model.find(xmlns + "config[@key='columnProperties']")

        for child in column_properties:
            i_th_col = child
            col_name = i_th_col.find(xmlns + "entry[@key='ColumnName']").get('value')
            col_labels.append(col_name)

            column_class = i_th_col.find(xmlns + "config[@key='ColumnClass']").find(xmlns + "entry[@key='cell_class']").get('value')
            if "StringCell" in column_class:
                col_types[col_name] = 'str'
            elif "IntCell" in column_class:
                col_types[col_name] = 'int'
            elif "DoubleCell" in column_class:
                col_types[col_name] = 'float'
            else:
                self.logger.error("Error in node:" + str(self.id) + " : column of unknown data type! Node's xml file is: " + xml_file)

            if i_th_col.find(xmlns + "entry[@key='MissValuePattern']").get('value') != "":
                self.logger.debug("Error in node:" + str(self.id) + " : customization of missing values not implemented! Node's xml file is: " + xml_file)
            if i_th_col.find(xmlns + "entry[@key='SkipThisColumn']").get('value') != "false":
                self.logger.error("Error in node " + str(self.id) + " : column skipping not implemented! Node's xml file is: " + xml_file)
            if i_th_col.find(xmlns + "entry[@key='ReadPossValsFromFile']").get('value') != "false":
                self.logger.error("Error in node " + str(self.id) + " : domain settings not implemented! Node's xml file is: " + xml_file)
            if i_th_col.find(xmlns + "entry[@key='FormatParameter']").get('isnull') != "true":
                self.logger.error("Error in node " + str(self.id) + " : formatting not implemented! Node's xml file is: " + xml_file)

        n_cols = len(col_labels)

        # I removed the `row_labels` because they contain no useful information
        #df = pd.DataFrame(np.full((n_rows, n_cols), 'NaN'), columns=col_labels, index=row_labels)
        df = pd.DataFrame(np.full((n_rows, n_cols), 'NaN'), columns=col_labels)

        for i in list(range(0,n_cols)):
            for j in list(range(0,n_elements)):
                if col_arr[j] == i:
                    df[col_labels[i]].iloc[row_arr[j]] = val_arr[j]

        for label in col_labels:
            if col_types[label] == 'str':
                df[label] = df[label].astype(str)
            elif col_types[label] == 'int':
                for elem in df[label]:
                    if elem == 'NaN':
                        self.logger.error("Error in : " + xml_file + " Current implementation doesn't allow missing values in integer columns!")
                        # FIXME : doesn't work with missing vals since NaN can't be converted to int!
                df[label] = df[label].astype(int)
            elif col_types[label] == 'float':
                df[label] = df[label].astype(float)
            else:
                pass

        self = PythonNode.init_wrapper(cls, df=df)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self) -> pd.DataFrame:
        return self.df
