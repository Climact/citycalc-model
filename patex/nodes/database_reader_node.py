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
    DATABASE READER NODE
    ============================
    KNIME options NOT implemented:
        - No MySQL connector node before
        - option "Run sql query only during execute, skips configure" not checked
"""

from timeit import default_timer as timer

import numpy as np
import pandas as pd
import pymysql

from patex.nodes.node import Node


class DatabaseReaderNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None, db_host=None, db_port=None, db_user=None,
                 db_password=None, db_schema=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.dataframe = pd.DataFrame()
        self.column = None  # Name of the lever
        self.value = None  # Name of the flow variable containing the value for the lever_selection
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        self.log_timer(None, 'START')

        if self.db_host is not None and self.db_port is not None and self.db_user is not None and \
                self.db_password is not None and self.db_schema is not None:

            connection = pymysql.connect(user=self.db_user, password=self.db_password, host=self.db_host,
                                         database=self.db_schema)

            model = self.xml_root.find(self.xmlns + "config[@key='model']")
            statement = model.find(self.xmlns + "entry[@key='statement']").get('value')
            execute_without_configure = model.find(self.xmlns + "entry[@key='execute_without_configure']").get('value')
            statement = statement.replace('%%00010', ' ')  # remove %%00010 char that represent a 'return to line'

            if 'WHERE' in statement:
                statement_split = statement.split("WHERE")
                new_statement = statement_split[0]
                lever_selection = statement_split[1]
                self.column = lever_selection.split("=")[0]
                self.column = self.column.replace(" ", "")
                self.column = self.column.replace("`", "")
                self.value = lever_selection.split("=")[1]
                self.value = self.value.replace("$", "")
                self.value = self.value.replace("{", "")
                self.value = self.value.replace("}", "")
                self.value = self.value[1:]
            else:
                new_statement = statement

            if execute_without_configure == 'false':
                raise Exception(
                    "SQL query are run during execute, configuration is skipped (Database reader node #" + str(
                        self.id) + ", xml file : " + str(self.xml_file) + ")")

            try:
                new_statement.encode().decode('utf-8')
            except UnicodeDecodeError:
                raise Exception(
                    "Encoding of DB not in UTF-8 (Database reader node #" + str(self.id) + ", xml file : " + str(
                        self.xml_file) + ")")

            try:
                df = pd.read_sql_query(sql=new_statement, con=connection)
            except pymysql.OperationalError as e:
                raise Exception(
                    "Unable to fetch data from table (Database reader node #" + str(self.id) + ", xml file : " + str(
                        self.xml_file) + ")")

            # FIXME: always remove duplicate ?
            df = df.loc[:, ~df.columns.duplicated()]
            # The following line is to remove the None if an entire column is empty
            # (see here: https://github.com/pandas-dev/pandas/issues/14319)
            df.replace([None], np.nan, inplace=True)

            self.dataframe = df

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        # if self.db_host is None or self.db_port is None or self.db_user is None or \
        #         self.db_password is None or self.db_schema is None:
        if self.dataframe.empty:

            connect_dict = self.read_in_port(1)
            if connect_dict is None or connect_dict == {}:
                raise Exception(
                    "The node 'Database Reader' must be connected to a node 'MySQL connector' (Database reader node #{}".format(
                        self.id))

            db_connector = connect_dict['connection']

            model = self.xml_root.find(self.xmlns + "config[@key='model']")
            statement = model.find(self.xmlns + "entry[@key='statement']").get('value')
            execute_without_configure = model.find(self.xmlns + "entry[@key='execute_without_configure']").get('value')
            statement = statement.replace('%%00010', ' ')  # remove %%00010 char that represent a 'return to line'

            # reformat statement if using flow variables in it : $${Iflow_variable}$$
            new_statement = ''
            for item in statement.split('$$'):
                if item.startswith('{'):
                    item = item.replace('{', '')
                    item = item.replace('}', '')
                    item = self.flow_vars[item[1:]][1]
                new_statement = new_statement + str(item)

            if execute_without_configure == 'false':
                raise Exception(
                    "SQL query are run during execute, configuration is skipped (Database reader node #" + id + ", xml file : " + str(
                        self.xml_file) + ")")

            try:
                new_statement.encode().decode('utf-8')
            except UnicodeDecodeError:
                raise Exception(
                    "Encoding of DB not in UTF-8 (Database reader node #" + id + ", xml file : " + str(
                        self.xml_file) + ")")
            try:
                df = pd.read_sql_query(sql=new_statement, con=db_connector)
            except pymysql.OperationalError as e:
                raise Exception("Unable to fetch data from table (Database reader node #" + id + ", xml file : " + str(
                    self.xml_file) + ")")

            # FIXME: always remove duplicate ?
            df = df.loc[:, ~df.columns.duplicated()]
            # The following line is to remove the None if an entire column is empty
            # (see here: https://github.com/pandas-dev/pandas/issues/14319)
            df.replace([None], np.nan, inplace=True)

            self.save_out_port(1, df)

        elif self.value is not None and self.column is not None:
            value_type = self.flow_vars[self.value][0]
            value = self.flow_vars[self.value][1]
            if value_type == "DATAFRAME":  # if we specify the position of the levers for each country
                df_value = value[['Country', 'levers']]
                df_value = df_value.rename(columns={'levers': self.column})
                keys = list(df_value.columns.values)
                i1 = self.dataframe.set_index(keys).index
                i2 = df_value.set_index(keys).index
                self.save_out_port(1, self.dataframe[i1.isin(i2)])
            else:  # if the position of the lever is the same for each country
                # output_table = self.dataframe.copy()
                # lever_column = [col for col in output_table.columns if 'lever' in col]
                # level_min = math.floor(value)
                # level_max = math.ceil(value)
                # columns_fts = [col for col in output_table.columns if 'fts' in col]
                # mask_min = (output_table[lever_column[0]] == int(level_min))
                # mask_max = (output_table[lever_column[0]] == int(level_max))
                # table_min = output_table.loc[mask_min, columns_fts].values
                # table_max = output_table.loc[mask_max, columns_fts].values
                # table = table_min + (table_max - table_min) * (value - level_min)
                # output_table = output_table.loc[mask_min]
                #
                # output_table.loc[:, columns_fts] = table
                # self.save_out_port(1, output_table)

                mask = (self.dataframe[self.column] == value)
                self.save_out_port(1, self.dataframe.loc[mask])
        else:
            self.save_out_port(1, self.dataframe)

        # logger
        t = timer() - start
        self.log_timer(t, "END")
