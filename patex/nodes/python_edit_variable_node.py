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
    PYTHON EDIT VARIABLE NODE
    ============================
    KNIME options implemented:
        - All
"""

import collections
from timeit import default_timer as timer

from patex.nodes.node import Node


class PythonEditVariableNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.source_code = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.source_code = model.find(self.xmlns + "entry[@key='sourceCode']").get('value')
        python_version = model.find(self.xmlns + "entry[@key='pythonVersionOption']").get('value')
        convert_miss_to_python = model.find(self.xmlns + "entry[@key='convertMissingToPython']").get('value')
        convert_miss_from_python = model.find(self.xmlns + "entry[@key='convertMissingFromPython']").get('value')
        sentinel_option = model.find(self.xmlns + "entry[@key='sentinelOption']").get('value')

        if python_version != "PYTHON3" and python_version != "python3":
            self.logger.error("Error in node " + str(
                self.id) + " : execution of Python 2 code not implemented! Node's xml file is: " + self.xml_file)

        if convert_miss_to_python != "false":
            self.logger.error("Error in node " + str(
                self.id) + " : conversion of missing values to sentinel values not implemented! Node's xml file is: " + self.xml_file)

        if convert_miss_from_python != "false":
            self.logger.error("Error in node " + str(
                self.id) + " : conversion of sentinel values to missing values not implemented! Node's xml file is: " + self.xml_file)

        if sentinel_option != "MIN_VAL":
            self.logger.error("Error in node " + str(
                self.id) + " : sentinel value other than MIN_VAL not implemented! Node's xml file is: " + self.xml_file)

        self.source_code = self.source_code.replace('%%00010', '\n')
        self.source_code = self.source_code.replace('%%00009', '\t')
        self.source_code = self.source_code.replace('&quot;', '"')
        self.source_code = self.source_code + '\n'  # whitespace padding in case the code ends on a loop that requires a final "enter" to be run

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        self.logger.debug("Python Edit Variable doesn's support input port and variables should be red from xml instead of running python code.")

        flow_variables = collections.OrderedDict()

        for var in self.flow_vars:
            flow_variables[var] = self.flow_vars[var][1]

        ns = {'flow_variables': flow_variables}
        exec(self.source_code, ns)

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        for var in flow_variables:
            self.flow_vars[var] = ('UNKNOWN_CLASS', flow_variables[var])
