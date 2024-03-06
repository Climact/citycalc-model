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
    MYSQL CONNECTOR NODE
    ============================
    KNIME options NOT implemented:
        - Credential authentication
        - Time zone correction
        - Retrieve metadata during connection
"""

from timeit import default_timer as timer

import pymysql
from sqlalchemy import create_engine

from patex.nodes.node import Node

pymysql.install_as_MySQLdb()


class MySQLConnectorNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None, db_host=None, db_port=None, db_user=None,
                 db_password=None, db_schema=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.connect_dict = {}
        self.user_name = ""
        self.password_hard = ""
        self.hostname = ""
        self.database_name = ""
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        if self.db_host is None or self.db_port is None or self.db_user is None or \
                self.db_password is None or self.db_schema is None:
            id = str(self.id) + " " + self.xml_file
            model = self.xml_root.find(self.xmlns + "config[@key='model']")
            credential = model.find(self.xmlns + "entry[@key='credential_name']")
            if credential is not None:
                self.logger.error("Credential authentication is not allowed (MySQL Connector node #{}".format(id))
            else:
                driver = model.find(self.xmlns + "entry[@key='driver']").get('value')
                database = model.find(self.xmlns + "entry[@key='database']").get('value')
                user = model.find(self.xmlns + "entry[@key='user']").get('value')
                self.user_name = model.find(self.xmlns + "entry[@key='userName']").get('value')
                timezone = model.find(self.xmlns + "entry[@key='timezone']").get('value')
                space_in_column = model.find(self.xmlns + "entry[@key='allowSpacesInColumnNames']").get('value')
                metadata = model.find(self.xmlns + "entry[@key='retrieveMetadataInConfigure']").get('value')

                if metadata != 'false':
                    self.logger.error(
                        "Metadata are not retrieved during configuration. Uncheck the option in the node configuration dialog (MySQL Connector node #{}".format(
                            id))

                if timezone != "current":
                    self.logger.error("The timezone must be local (MySQL Connector node #{}".format(id))

                if space_in_column != 'true':
                    self.logger.error("Space in column are allowed (MySQL Connector node #{}".format(id))

                self.hostname = model.find(self.xmlns + "config[@key='default-connection']").find(
                    self.xmlns + "entry[@key='hostname']").get('value')
                port = model.find(self.xmlns + "config[@key='default-connection']").find(
                    self.xmlns + "entry[@key='port']").get('value')
                self.database_name = model.find(self.xmlns + "config[@key='default-connection']").find(
                    self.xmlns + "entry[@key='databaseName']").get('value')

                # TODO correct the issue with the password credentials
                self.password_hard = 'admin'

                # Connector to write in a database
                try:
                    engine = create_engine(
                        "mysql://" + self.user_name + ":" + self.password_hard + "@" + self.hostname + "/" + self.database_name)
                except pymysql.OperationalError:
                    self.logger.error(
                        "The connection to the database hasn't been implemented (MySQL Connector node #" + id + ", xml file : " + str(
                            self.xml_file) + ")")

                self.connect_dict['engine'] = engine

        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        if self.db_host is not None and self.db_port is not None and self.db_user is not None and \
                self.db_password is not None and self.db_schema is not None:
            self.save_out_port(1, self.connect_dict)
        else:
                #Connector to read in a database
                connection = pymysql.connect(user=self.user_name,password = self.password_hard, host=self.hostname, database=self.database_name)
                self.connect_dict['connection'] = connection

                self.save_out_port(1,self.connect_dict)
            # logger
        t = timer() - start
        self.log_timer(t, "END")



