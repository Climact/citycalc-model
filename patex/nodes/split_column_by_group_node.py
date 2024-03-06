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
    SPLIT COLUMN BY GROUP METANODE
    ===================
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class SplitColumnByGroup(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.pattern = None
        self.local_flow_vars = {}
        self.column = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        # Check code version
        # workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        # if workflow_template_information is None:
        #     self.logger.error(
        #         "The tree merge node (" + self.xml_file + ") is not connected to the EUCalc - Split column metanode template.")
        # else:
        #     timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
        #     if timestamp != '2019-04-06 23:27:04':
        #         self.logger.error(
        #             "The template of the EUCalc - Split column metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['pattern_search'] = ('STRING', "(.*)")
        self.local_flow_vars['pattern_replacement'] = ('STRING', "$1")
        self.local_flow_vars['new_name'] = ('STRING', "new_name")
        self.local_flow_vars['bool_append'] = ('STRING', "0")
        self.local_flow_vars['target_column'] = ('STRING', "RowIDs")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-1596": "pattern_search",
                          "string-input-1597": "pattern_replacement",
                          "string-input-1599": "new_name",
                          "boolean-input-1598": "bool_append",
                          "column-selection-1595": "target_column"}

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        self.pattern = {r''+'^'+self.local_flow_vars['pattern_search'][1]+'$' : r''+self.local_flow_vars['pattern_replacement'][1].replace('$','\\')}

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "SPLIT column by group")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "SPLIT column by group")

        df = self.read_in_port(1)
        if self.local_flow_vars["bool_append"][1] == 'true':
            self.column = df[self.local_flow_vars["target_column"][1]].replace(self.pattern, regex=True).values
            df[self.local_flow_vars["new_name"][1]] = df[self.local_flow_vars["target_column"][1]].replace(self.pattern, regex=True)
        else:
            self.column = df[self.local_flow_vars["target_column"][1]].replace(self.pattern, regex=True).values
            df[self.local_flow_vars["target_column"][1]] = df[self.local_flow_vars["target_column"][1]].replace(self.pattern,
                                                                                                     regex=True)

        self.save_out_port(1, df)
        # logger
        t = timer() - start
        self.log_timer(t, "END", "SPLIT column by group")

    def update(self):
        start = timer()
        self.log_timer(None, 'UPDATE', "SPLIT column by group")

        df = self.in_ports[1]
        if self.local_flow_vars["bool_append"][1] == 'true':
            df[self.local_flow_vars["new_name"][1]] = self.column
        else:
            df[self.local_flow_vars["target_column"][1]] = self.column
        self.out_ports[1] = df

        # logger
        t = timer() - start
        self.log_timer(t, "END", "SPLIT column by group")