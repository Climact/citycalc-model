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
    EXCEL READER NODE
    ============================
    KNIME options not implemented:
        - Connect timeout
        - Table contains column names in row number different from 1
        - Tables contains row IDs in columns ... + Make row id unique
        - Generate rowIDs (index as per sheet content, skipped row will increment index)
        - If we do not read the entire data sheet and we don't specifiy the column
        - Skip empty columns, skip hidden column, skip empty row
        - Reevalutate formula
        - None of the reevaluation pattern
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class ExcelReaderNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.filename = ""
        self.this_sheetname = ""
        self.this_header = ""
        self.this_name = ""
        self.this_usecol = ""
        self.read_all_data = False
        self.beg = 0
        self.end = 0
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")
        id = str(self.id)
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        xml_filename = model.find(self.xmlns + "entry[@key='XLS_LOCATION']")
        self.filename = xml_filename.get("value")
        self.filename = self.filename.replace('\\', '/')
        self.filename = self.filename.replace("knime://LOCAL", self.local)

        xml_sheetname = model.find(self.xmlns + "entry[@key='SHEET_NAME']")
        self.read_all_data = model.find(self.xmlns + "entry[@key='READ_ALL_DATA']").get('value')
        first_row = model.find(self.xmlns + "entry[@key='FIRST_ROW']").get('value')
        last_row = model.find(self.xmlns + "entry[@key='LAST_ROW']").get('value')
        first_col = model.find(self.xmlns + "entry[@key='FIRST_COL']").get('value')
        last_col = model.find(self.xmlns + "entry[@key='LAST_COL']").get('value')
        has_column_headers = model.find(self.xmlns + "entry[@key='HAS_COL_HDRS']").get('value')
        column_headers_row = model.find(self.xmlns + "entry[@key='COL_HDR_ROW']").get('value')
        reevaluate_formula = model.find(self.xmlns + "entry[@key='REEVALUATE_FORMULAE']").get('value')

        if reevaluate_formula == 'true':
            raise Exception("Reevaluate formula was not implemented (Excel Writer node #" + id + ", xml file : " + str(
                self.xml_file) + ")")

        if has_column_headers == 'true':
            if column_headers_row != '1':
                raise Exception(
                    "The column header must be in the row 1 (Excel Reader _XLS_ node #" + id + ", xml file : " + str(
                        self.xml_file) + ")")
        index_continuous = model.find(self.xmlns + "entry[@key='INDEX_CONTINUOUS']").get('value')
        if index_continuous == 'false':
            raise Exception(
                "The RowIDs are generated incrementally and starting with row0 (Excel Reader _XLS_ node #" + id + ", xml file : " + str(
                    self.xml_file) + ")")

        skip_empty_cols = model.find(self.xmlns + "entry[@key='SKIP_EMPTY_COLS']").get('value')
        skip_empty_row = model.find(self.xmlns + "entry[@key='SKIP_EMPTY_ROWS']").get('value')
        skip_hidden_cols = model.find(self.xmlns + "entry[@key='SKIP_HIDDEN_COLS']").get('value')
        if skip_empty_cols == 'true' or skip_empty_row == 'true' or skip_hidden_cols == 'true':
            self.logger.debug(
                "Skipping empty rows, cols or hidden cols is not implemented in the Python converter. The exception occurred in the Excel Reader Node (Cfr. xml file : " + str(
                    self.xml_file) + ").")

        if xml_sheetname.get('value') == '':  # if the first sheet is chosen
            self.this_sheetname = 0
        else:
            self.this_sheetname = xml_sheetname.get("value")

        self.this_name = []

        self.this_usecol = first_col + ":" + last_col

        column_specs = model.find(self.xmlns + "config[@key='XLS_DataTableSpec']").findall(self.xmlns + "config")
        # dict_type = {}

        if has_column_headers == 'true':
            self.this_header = int(column_headers_row) - 1
            self.this_name = None
        else:
            self.this_header = None
            for this_column in column_specs:
                col_name = this_column.find(self.xmlns + "entry[@key='column_name']").get('value')
                self.this_name.append(col_name)

        if self.read_all_data != 'true':
            if last_col == '' or first_col == '':
                raise Exception(
                    "If we do not read all the date, the last and the first column must be set (Excel Reader _XLS_ node #" + id + ", xml file : " + str(
                        self.xml_file) + ")")
            if last_row == '' or first_row == '':
                raise Exception("If we do not read all the date, the last and the first row must be set (Excel Reader _XLS_ node #"+id+", xml file : "+ str(self.xml_file)+")")

            if has_column_headers == 'true':
                self.beg = int(first_row)-1
                self.end = int(last_row)-1
            else:
                self.beg = int(first_row)-1
                self.end = int(last_row)

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        if self.read_all_data == 'true':
            df = pd.read_excel(io=self.filename, sheet_name=self.this_sheetname, header=self.this_header, names=self.this_name)

        else:
            df = pd.read_excel(io=self.filename, sheet_name=self.this_sheetname, header=self.this_header, names=self.this_name, usecols=self.this_usecol)
            df = df.iloc[self.beg:self.end]

        # if 'Years' in list(df.columns):
        #     df['Years'] = df['Years'].astype(str)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df)
