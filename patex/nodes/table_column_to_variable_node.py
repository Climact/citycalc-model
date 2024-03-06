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
    TABLE COLUMN TO VARIABLE NODE
    ============================
    KNIME options implemented:
        - everything, except that it always ignores missing values (or the missing value 'NaN' at least)
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class TableColumnToVariableNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.column_name = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        column = model.find(self.xmlns + "config[@key='column']")
        self.column_name = column.find(self.xmlns + "entry[@key='columnName']").get('value')
        ignore_missing = model.find(self.xmlns + "entry[@key='ignore missing']").get('value')

        if ignore_missing != "true":
            raise Exception("Error in node " + str(
                self.id) + " : not ignore missing values not implemented! Node's xml file is: " + self.xml_file)

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)
        df = df[self.column_name]
        index = df.index

        i = 0
        for entry in df:
            if entry != 'NaN':
                if type(entry).__name__ == 'str':
                    self.flow_vars[index[i]] = ("STRING", entry)
                elif type(entry).__name__ == 'int':
                    self.flow_vars[index[i]] = ("INTEGER", entry)
                elif type(entry).__name__ == 'float':
                    self.flow_vars[index[i]] = ("DOUBLE", entry)
                else:
                    raise Exception("Error in node " + str(self.id) +
                                    " : entry datatype not recognized! Node's xml file is: " + self.xml_file)
            i = i + 1

        # logger
        t = timer() - start
        self.log_timer(t, "END")
