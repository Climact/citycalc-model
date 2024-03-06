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
    STRING TO NUMBER
    ============================
    KNIME options implemented:
        -  Type : Double or Int, decimal separator : '.' or ',' Thousands separator : '.' or ',' 
    KNIME options NOT implemented:
        - Accept type suffix & Always include all columns
        - Others datatypes and decimal separators.
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class StringToNumberNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.decimalSeparator = ""
        self.thousandsSeparator = ""
        self.colNameList = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        include = model.find(self.xmlns + "config[@key='include']")
        inclList = include.find(self.xmlns + "config[@key='InclList']")
        totalNrbCol = inclList.find(self.xmlns + "entry[@key='array-size']").get('value')
        self.decimalSeparator = model.find(self.xmlns + "entry[@key='decimal_separator']").get('value')
        self.thousandsSeparator = model.find(self.xmlns + "entry[@key='thousands_separator']").get('value')
        dataTypes = model.find(self.xmlns + "config[@key='parse_type']")
        dataType = dataTypes.find(self.xmlns + "entry[@key='cell_class']").get('value')

        for i in range(0, int(totalNrbCol)):
            colName = inclList.find(self.xmlns + "entry[@key='" + str(i) + "']").get('value')
            self.colNameList.append(colName)

        if model.find(self.xmlns + "entry[@key='generic_parse']").get('value') == 'true':
            raise Exception(
                "The option 'Accept the suffix' has been checked in the String to Number node (Cfr. xml file : " + str(
                    self.xml_file) + ") but this is not implemented in the Python converter.")
        elif "Long" in dataType :
            raise Exception(
                "The conversion to data type '"+dataType+"' in the String to Number node (Cfr. xml file : " + str(
                    self.xml_file) + ") is not implemented in the Python converter.")

        elif (self.decimalSeparator != "." and self.decimalSeparator != ",") or (self.thousandsSeparator != "." and self.thousandsSeparator != "," and self.thousandsSeparator != ""):
            raise Exception(
                "Unimplemented decimal and thousands separators (respectively : '" + self.decimalSeparator + ''",'" + self.thousandsSeparator + "') are configured in the String to Number node (Cfr. xml file : " + str(
                    self.xml_file) + ").")

        else:
            if self.decimalSeparator == ',' and self.thousandsSeparator == ',':
                raise Exception(
                    "The decimal and thousands separators are identicals (Cfr. xml file : " + str(
                        self.xml_file) + "). This .xml file must have been manually modified as this setting is incoherent to Knime")
            elif self.decimalSeparator == '.' and self.thousandsSeparator == '.':
                raise Exception(
                    "The decimal and thousands separators are identicals (Cfr. xml file : " + str(
                        self.xml_file) + "). This .xml file must have been manually modified as this setting is incoherent to Knime")

            if "Int" in dataType:
                self.logger.debug(
                "A conversion to Integer was requested in the String to Number node (Cfr. xml file : " + str(
                    self.xml_file) + ") but is not implemented in the Python converter. Instead, a conversion to float is done.")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)

        #"Double" in dataType:
        if self.decimalSeparator == '.' and self.thousandsSeparator == '':
            df[self.colNameList] = df[self.colNameList].apply(pd.to_numeric, errors='coerce')
        elif self.decimalSeparator == ',' and self.thousandsSeparator == '':
            df[self.colNameList] = df[self.colNameList].replace(',', '.', regex=True)
            df[self.colNameList] = df[self.colNameList].apply(pd.to_numeric, errors='coerce')
        elif self.decimalSeparator == '.' and self.thousandsSeparator == ',':
            df[self.colNameList] = df[self.colNameList].replace(",", "", regex=True)
            df[self.colNameList] = df[self.colNameList].apply(pd.to_numeric, errors='coerce')
        elif self.decimalSeparator == ',' and self.thousandsSeparator == '.':
            df[self.colNameList] = df[self.colNameList].replace(".", "").replace(",", ".", regex=True)
            df[self.colNameList] = df[self.colNameList].apply(pd.to_numeric, errors='coerce')

    # If necessary to implement conversion vers int : below, a draft of the code.
    # for double add : downcast="double"
        # elif "Int" in dataType:
        #     if decimalSeparator == '.' and thousandsSeparator == '':
        #         df1[colNameList] = df1[colNameList].apply(pd.to_numeric, errors='coerce',downcast='integer')
        #     elif decimalSeparator == ',' and thousandsSeparator == '':
        #         df1[colNameList] = df[colNameList].replace(',', '.', regex=True)
        #         df1[colNameList] = df1[colNameList].apply(pd.to_numeric, errors='coerce',downcast='integer')
        #     elif decimalSeparator == '.' and thousandsSeparator == ',':
        #         df1[colNameList] = df1[colNameList].replace(",", "")
        #         df1[colNameList] = df1[colNameList].apply(pd.to_numeric, errors='coerce',downcast='integer')
        #     elif decimalSeparator == ',' and thousandsSeparator == '.':
        #         df1[colNameList] = df1[colNameList].replace(".", "").replace(",", ".", regex=True)
        #         df1[colNameList] = df1[colNameList].apply(pd.to_numeric, errors='coerce',downcast='integer')
        #     elif decimalSeparator == ',' and thousandsSeparator == ',':
        #         raise Exception(
        #             "The decimal and thousands separators are identicals (Cfr. xml file : " + str(
        #                 self.xml_file) + "). This .xml file must have been manually modified as this setting is incoherent to Knime")
        #     elif decimalSeparator == '.' and thousandsSeparator == '.':
        #         raise Exception(
        #             "The decimal and thousands separators are identicals (Cfr. xml file : " + str(
        #                 self.xml_file) + "). This .xml file must have been manually modified as this setting is incoherent to Knime")
        #     else:
        #         raise Exception(
        #             "Unimplemented decimal and thousands separators (respectively : '" + decimalSeparator + ''",'" + thousandsSeparator + "') are configured in the String to Number node (Cfr. xml file : " + str(
        #                 self.xml_file) + ").")

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df)
