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
    VARIABLE TO TABLE COLUMN NODE
    ============================
    KNIME options implemented:
        -
"""

import re
from timeit import default_timer as timer

from patex.nodes.node import Node


class VariableToTableColumnNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.id_xml = str(id) + " " + xml_file
        self.entry = None
        self.type_of_filter = None
        self.pattern = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.type_of_filter = model.find(self.xmlns + "config[@key='variable-filter']").find(
            self.xmlns + "entry[@key='filter-type']").get('value')

        # Regex type of filtering
        if self.type_of_filter == 'name_pattern':
            self.entry = model.find(self.xmlns + "config[@key='variable-filter']").find(
                self.xmlns + "config[@key='name_pattern']")
            if self.entry.find(self.xmlns + "entry[@key='type']").get('value') == 'Regex':
                case_sensitive = self.entry.find(self.xmlns + "entry[@key='caseSensitive']").get('value')
                self.pattern = self.entry.find(self.xmlns + "entry[@key='pattern']").get('value')

                if case_sensitive == 'true':
                    self.pattern = self.patternreshape(self.pattern, case_sensitive=True)
                else:
                    self.pattern = self.patternreshape(self.pattern, case_sensitive=False)
            else:
                self.logger.error("Wildcard column filtering is not implemented (node #" + str(self.id_xml) + ")")
        # Manual type of filtering
        elif self.type_of_filter == 'STANDARD':
            self.logger.error(
                'Manual Column filtering is not implemented (Column filter node #' + str(self.id_xml)  + ")")

        # Type of filtering trough data_type
        elif self.type_of_filter == 'datatype':
            self.logger.error(
                'Column type filtering is not implemented (Column filter node #' + str(self.id_xml)  + ")")

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(2)

        # Regex type of filtering
        if self.type_of_filter == 'name_pattern':
             for key, value in self.flow_vars.items():
                if re.match(self.pattern, key):
                    if value[0] == 'STRING':
                        df[key] = value[1]
                    elif value[0] == "DOUBLE":
                        df[key] = float(value[1])
                    elif value[0] == "INTEGER":
                        df[key] = int(value[1])
                    elif value[0] == "UNKNOWN_CLASS":
                        try:
                            df[key] = float(value[1])
                        except ValueError as e:
                            self.logger.error(e)
                            df[key] = value[1]
                    elif value[0] == "DATAFRAME":
                        # TODO: Convert dataframe into single value for lever position (mutli-country)
                        df[key] = int(value[1]['levers'][0])
                    elif value[0] == "DICTIONARY":
                        df[key] = int(value[1]['default'])
                    else:
                        raise Exception("Unknown variable type")
    # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df)
