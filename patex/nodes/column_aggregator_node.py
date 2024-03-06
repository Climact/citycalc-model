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
    COLUMN AGGREGATOR NODE
    ============================
    KNIME options implemented:
        Columns:
        - Manual filtering
        - Regex (case sensitive and insensitive) filtering
        - Enforce exclusion
        Options:
        - Aggregation methods: Maximum, Minimum, Sum, Variance, Unique Count
        - Remove aggregation columns
        - Remove retained columns
        - Maximum unique value per row = 10000
"""
import re
from timeit import default_timer as timer

from patex.nodes.node import Node


class ColumnAggregatorNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.id_xml = str(id) + " " + xml_file
        self.type_of_pattern = 'Manual'
        self.case_sensitive = 'false'
        self.pattern = None
        self.xml_columns_to_aggregate = None
        self.columns_not_to_aggregate = []
        self.columns_to_aggregate = []
        self.type_of_columns_to_aggregate = []
        self.list_types = []
        self.list_names = []
        self.length = 0
        self.remove_aggregation_columns = None
        self.remove_retained_columns = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        type_of_filtering = model.find(self.xmlns + "config[@key='aggregationColumns']").find(
            self.xmlns + "entry[@key='filter-type']").get('value')
        max_value = model.find(self.xmlns + "entry[@key='maxNoneNumericalVals']").get('value')
        if max_value != '10000':
            raise Exception(
                'The maximum unique value per row is set at 10000 (Column Aggregator node #' + str(self.id) + ', .xml file ' + str(
                    self.xml_file) + ')')

        self.remove_aggregation_columns = model.find(self.xmlns + "entry[@key='removeAggregationColumns']").get('value')
        self.remove_retained_columns = model.find(self.xmlns + "entry[@key='removeRetainedColumns']").get('value')

        # ------------------------------------------------------
        # Columns filtering
        # ------------------------------------------------------
        # Regex type of filtering
        if type_of_filtering == 'name_pattern':
            entry = model.find(self.xmlns + "config[@key='aggregationColumns']").find(
                self.xmlns + "config[@key='name_pattern']")
            if entry.find(self.xmlns + "entry[@key='type']").get('value') == 'Regex':
                self.type_of_pattern = 'Regex'
                self.case_sensitive = entry.find(self.xmlns + "entry[@key='caseSensitive']").get('value')
                self.pattern = entry.find(self.xmlns + "entry[@key='pattern']").get('value')
                # a = pattern.split('.')  # Pattern in xml : 'pattern.*'
                # this_str = '^'  # For the regex to only look at the begining of the word
                if self.pattern.startswith('|'):
                    self.pattern = self.pattern[1:]
                if self.pattern.endswith('|'):
                    self.pattern = self.pattern[:-1]
            else:
                raise Exception("Wildcard column splitting is not implemented (Column Aggregator node #" + str(self.id) + ")")

        # Manual type of filtering
        elif type_of_filtering == 'STANDARD':
            enforce_option = model.find(self.xmlns + "config[@key='aggregationColumns']").find(
                self.xmlns + "entry[@key='enforce_option']").get('value')
            if enforce_option != 'EnforceExclusion':
                self.logger.error("The ENFORCE option must be set on 'Enforce Exclusion' (" + str(self.xml_file) + ")")

            xml_columns_to_aggregate = model.find(self.xmlns + "config[@key='aggregationColumns']").find(
                self.xmlns + "config[@key='included_names']").findall(self.xmlns + "entry")
            xml_columns_not_to_aggregate = model.find(self.xmlns + "config[@key='aggregationColumns']").find(
                self.xmlns + "config[@key='excluded_names']").findall(self.xmlns + "entry")
            for this_column in xml_columns_to_aggregate:
                if this_column.get('key') != 'array-size':
                    self.columns_to_aggregate.append(this_column.get('value'))
            for this_column in xml_columns_not_to_aggregate:
                if this_column.get('key') != 'array-size':
                    self.columns_not_to_aggregate.append(this_column.get('value'))

        # Data_type filtering
        elif type_of_filtering == 'datatype':
            raise Exception(
                "Column type filtering is not implemented for column aggregator (Column Aggregator node #" + str(self.id) + ", .xml file " + str(
                    self.xml_file) + ")")

        names_of_col = model.find(self.xmlns + "config[@key='aggregationMethods']").find(
            self.xmlns + "config[@key='resultColName']").findall(self.xmlns + "entry")
        types_of_filter = model.find(self.xmlns + "config[@key='aggregationMethods']").find(
            self.xmlns + "config[@key='aggregationMethod']").findall(self.xmlns + "entry")

        for ls_name in names_of_col:
            if ls_name.get('key') != 'array-size':
                self.list_names.append(ls_name.get('value'))
            else:
                self.length = int(ls_name.get('value'))

        for ls_type in types_of_filter:
            if ls_type.get('key') != 'array-size':
                self.list_types.append(ls_type.get('value'))
        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD')

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        columns_to_aggregate = self.columns_to_aggregate.copy()
        columns_not_to_aggregate = self.columns_not_to_aggregate.copy()
        type_of_columns_to_aggregate = self.type_of_columns_to_aggregate.copy()

        df = self.read_in_port(1)
        if self.type_of_pattern == 'Regex':
            list_columns = df.columns
            for this_column in list_columns:
                if self.case_sensitive == 'true':
                    # if re.search(this_str + a[0], this_column) == None:
                    if re.search(self.pattern, this_column) == None:
                        columns_not_to_aggregate.append(this_column)
                    else:
                        columns_to_aggregate.append(this_column)
                        type_of_columns_to_aggregate.append(df[this_column].dtype)
                else:
                    # if re.search(this_str + a[0], this_column, re.IGNORECASE) == None:
                    if re.search(self.pattern, this_column, re.IGNORECASE) == None:
                        columns_not_to_aggregate.append(this_column)
                    else:
                        columns_to_aggregate.append(this_column)
                        type_of_columns_to_aggregate.append(df[this_column].dtype)

        else:
            for this_column in columns_to_aggregate:
                type_of_columns_to_aggregate.append(df[this_column].dtype)

        if 'object' in type_of_columns_to_aggregate:
            self.logger.debug('You are aggregating over object (node #' + str(self.id) + ', .xml file ' + str(self.xml_file) + ')')

        df_to_aggregate = df[columns_to_aggregate]

        for i in range(self.length):
            if self.list_names[i] in columns_to_aggregate:
                # FIXME: to be solve, this is a quick fix in order not to remove a column that we are building
                columns_to_aggregate.remove(self.list_names[i])
            if self.list_types[i] == "Maximum":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('max', axis=1)
            elif self.list_types[i] == "Minimum":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('min', axis=1)
            elif self.list_types[i] == "Sum_V2.5.2":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('sum', axis=1)
            elif self.list_types[i] == "Variance":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('var', axis=1)
            elif self.list_types[i] == "List":
                df.loc[:, self.list_names[i]] = df.agg(lambda x: list(tuple(x)), axis=1)
            elif self.list_types[i] == "Unique count":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('nunique', axis=1)
            elif self.list_types[i] == "Product":
                df.loc[:, self.list_names[i]] = df_to_aggregate.agg('prod', axis=1)
            else:
                raise Exception("The aggregation method " + self.list_types[
                    i] + " has not been implemented for ((Column Aggregator node #" + str(self.id) + ", .xml file " + str(
                    self.xml_file) + ")")

        # self.logger.debug("Columns to aggregate in " + self.xml_file + " are : " + str(self.columns_to_aggregate))

        if self.remove_retained_columns == 'true':
            df = df.drop(columns_not_to_aggregate, axis=1)

        if self.remove_aggregation_columns == 'true':
            df = df.drop(columns_to_aggregate, axis=1)

        # logger
        t = timer() - start
        self.log_timer(t, 'END')

        self.save_out_port(1, df)
