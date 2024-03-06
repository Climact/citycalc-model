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
    DATABASE WRITER NODE
    ============================
    KNIME options NOT implemented:
        - Fail if an error occurs
        - Insert null for missing columns not checked
        - ALL sql type except for varchar, integer, numeric
"""

from timeit import default_timer as timer

from sqlalchemy.types import INTEGER
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import NVARCHAR

from patex.nodes.node import Node


class DatabaseWriterNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.append = []
        self.insert_null = []
        self.fail_on_error = []
        self.batch_size = []
        self.table_name = []
        self.append_isTrue = []
        self.dict_type = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        id = str(self.id) + " " + self.xml_file

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.append = model.find(self.xmlns + "entry[@key='append_data']").get('value')
        self.insert_null = model.find(self.xmlns + "entry[@key='insert_null_for_missing_cols']").get('value')
        self.fail_on_error = model.find(self.xmlns + "entry[@key='fail_on_error']").get('value')
        if self.fail_on_error == 'false':
            self.logger.error("The option fail if an error occurs must be checked (Database writer node #" + id +
                            ", xml file : " + str(self.xml_file)+")")
        self.batch_size = int(model.find(self.xmlns + "entry[@key='batch_size']").get('value'))
        self.table_name = model.find(self.xmlns + "entry[@key='table']").get('value')
        sql_type = model.find(self.xmlns + "config[@key='sql_types']").findall(self.xmlns + "entry")

        if self.append != 'false':
            self.append_isTrue = False
            if self.insert_null == 'false':
                self.logger.error("Insert null for missing value must be checked (Database writer #"+id+", xml file : "
                                + str(self.xml_file)+")")
        else:
            self.append_isTrue = True

        self.dict_type = {}
        for this_sql_type in sql_type:
            this_type = this_sql_type.get('value')
            if "varchar" in this_type:
                number = this_type.replace('(', ' ').replace(')', ' ').split(' ')[1]
                self.dict_type[this_sql_type.get('key')] = NVARCHAR(number)
            elif "integer" in this_type:
                self.dict_type[this_sql_type.get('key')] = INTEGER
            elif "numeric" in this_type:
                list_number = this_type.replace('(', ',').replace(')', ',').split(',')
                self.dict_type[this_sql_type.get('key')] = NUMERIC(list_number[1], list_number[2])
            else:
                self.logger.error("The type " + this_sql_type.get(
                    'key') + " is not handle (Database writer node #" + id + ", xml file : " + str(self.xml_file) +
                                "). Types must be of the form 'varchar(int)', 'integer', 'numeric(int a, int b)' with a greater than b.")

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        id = str(self.id) + " " + self.xml_file
        df = self.read_in_port(1)
        connect_dict = self.read_in_port(2)

        if connect_dict is None:
            self.logger.error("The node 'Database write' must be connected to an MySQL connector (Database writer node #"
                            + id + ", xml file : " + str(self.xml_file)+")")

        engine = connect_dict['engine']

        if self.append_isTrue:
            df.to_sql(name=self.table_name, con=engine, if_exists='replace', index=False, chunksize=self.batch_size,
                      dtype=self.dict_type)
        else:
            df.to_sql(name=self.table_name, con=engine, if_exists='append', index=False, chunksize=28, dtype=self.dict_type)

        # logger
        t = timer() - start
        self.log_timer(t, "END")
