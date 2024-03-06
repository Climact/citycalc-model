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
    COLUMN SPLITTER NODE
    ============================
    KNIME options NOT implemented:
        - splitting by datatype and wildcards
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class ColumnSplitterNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.type_of_splitter = None
        self.pattern = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None}

    def build_node(self):
        start = timer()
        self.log_timer(None, 'START')

        id = str(self.id) + " " + self.xml_file
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.type_of_splitter = model.find(self.xmlns + "config[@key='Filter Column Settings']").find(
            self.xmlns + "entry[@key='filter-type']").get('value')

        # Regex type of filtering
        if self.type_of_splitter == 'name_pattern':
            self.entry = model.find(self.xmlns + "config[@key='Filter Column Settings']").find(
                self.xmlns + "config[@key='name_pattern']")
            if self.entry.find(self.xmlns + "entry[@key='type']").get('value') == 'Regex':

                case_sensitive = self.entry.find(self.xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = self.entry.find(self.xmlns + "entry[@key='pattern']").get('value')
                self.pattern = Node.patternreshape(self, pattern, case_sensitive)
            else:
                raise Exception(
                    "Wildcard column splitting is not implemented (Column Splitter node #" + id + ", xml file : " + str(
                        self.xml_file) + ")")

        # Manual type of filtering
        elif self.type_of_splitter == 'STANDARD':
            xml_columns_to_filtered = model.find(self.xmlns + "config[@key='Filter Column Settings']").find(
                self.xmlns + "config[@key='excluded_names']").findall(self.xmlns + "entry")
            self.for_top = []
            for column_to_filtered in xml_columns_to_filtered:
                if column_to_filtered.get('key') != 'array-size':
                    self.for_top.append(column_to_filtered.get('value'))

        # Type of filtering trough data_type
        elif self.type_of_splitter == 'datatype':
            raise Exception(
                'Column type splitting not implemented (Column Splitter node #' + id + ', xml file : ' + str(
                    self.xml_file) + ')')
        else:
            raise Exception(
                'Unknown typ splitting (Column Splitter node #' + id + ', xml file : ' + str(
                    self.xml_file) + ')')

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()

        df = self.read_in_port(1)

        # Regex type of filtering
        if self.type_of_splitter == 'name_pattern':
            # FIXME: remove xml reader from run : self.entry shouldn't be red in the run but in the build step
            if self.entry.find(self.xmlns + "entry[@key='type']").get('value') == 'Regex':
                df_bottom = df.copy()
                df_bottom = df_bottom.filter(regex=self.pattern, axis=1)
                df_top = df.drop(df_bottom.columns, axis=1)

        # Manual type of filtering
        elif self.type_of_splitter == 'STANDARD':
            df_top = pd.DataFrame()
            df_bottom = df
            for this_column in self.for_top:
                df_top[this_column] = df[this_column].values
                df_bottom = df_bottom.drop(this_column, axis=1)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(2, df_bottom)
        self.save_out_port(1, df_top)
