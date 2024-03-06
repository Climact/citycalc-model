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
    TREE MERGE METANODE
    ===================
    KNIME options implemented:
        - Everything
    Exceptions:
       - Returns an exception if the user tries to create a column with a name that's already taken
       - Returns an exception if the timestamp of the metanode is not aligned
"""

import re
from timeit import default_timer as timer

from patex.nodes.node import Node


class TreeMergeNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.values = []
        self.local_flow_vars = {}
        self.pattern_a = None
        self.columns_copied_from_input = []
        self.columns_copied_from_input_int = []
        self.columns_used_int = []
        self.updated_column = None
        self.columns_used = []
        self.full_name = None
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        # Check code version
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The tree merge node (" + self.xml_file + ") is not connected to the EUCalc - Tree merge metanode template.")
        # else:
        #     timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
        #     if timestamp != '2018-09-05 16:41:42' and timestamp != "2019-04-01 17:02:42" and timestamp != "2020-02-21 18:02:07":
        #         self.logger.error(
        #             "The template of the EUCalc - Tree merge metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['unit'] = ('STRING', "unit")
        self.local_flow_vars['aggregation-method'] = ('STRING', "Product")
        self.local_flow_vars['aggregation-remove'] = ('STRING', "true")
        self.local_flow_vars['aggregation-pattern'] = ('STRING', ".*")
        self.local_flow_vars['new-column-name'] = ('STRING', "new-column")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-349": "new-column-name",
                          "string-input-354": "unit",
                          "string-input-357": "aggregation-pattern",
                          "boolean-input-359": "aggregation-remove",
                          "single-selection-350": "aggregation-method"}

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        self.pattern_a = self.patternreshape(self.local_flow_vars['aggregation-pattern'][1],  case_sensitive='true')

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Tree Merge")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Tree Merge")

        df = self.read_in_port(1).copy()

        is_matching = False

        self.full_name = self.local_flow_vars['new-column-name'][1] + '[' + self.local_flow_vars['unit'][1] + ']'
        if self.full_name in df.columns:
            self.logger.debug("A column with name " + self.full_name + " already exists and will be overwritten!")

        self.columns_used = [col for col in df.columns if re.match(self.pattern_a, col)]
        if self.local_flow_vars['aggregation-method'][1] == "Product":
            self.f = lambda x: x.prod(axis=1)
        elif self.local_flow_vars['aggregation-method'][1] == "Sum":
            self.f = lambda x: x.sum(axis=1)

        df['COL_TEMP'] = self.f(df[self.columns_used])

        if not self.columns_used:
            raise Exception(
                "No match found in input columns for pattern \"" + self.local_flow_vars['aggregation-pattern'][
                    1] + "\"")


        if self.local_flow_vars['aggregation-remove'][1] == "true":
            df = df.drop(self.columns_used, axis=1)

        # Variables for the update
        self.columns_copied_from_input = df.columns.intersection(self.read_in_port(1).columns)
        self.columns_copied_from_input_int = [self.read_in_port(1).columns.get_loc(c) for c in self.columns_copied_from_input]
        self.columns_used_int = [self.read_in_port(1).columns.get_loc(c) for c in self.columns_used]

        df = df.rename(columns={"COL_TEMP": self.full_name})
        self.updated_column = df.columns.get_loc(self.full_name)

        self.save_out_port(1, df)
        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Tree Merge")

    def update(self):
        start = timer()
        self.log_timer(None, 'UPDATE', "EUCALC Tree Merge")
        benchmark = False
        df = self.in_ports[1] # No need to check for existence, we can use property directly

        # Order of performance for indexing:
        #
        # Method:
        # - take
        # - iloc
        # - loc
        #
        # Indexor (to verify):
        # - List of indices (integers)
        # - List of booleans
        # - List of names
        #
        # Typical times:
        # Split time  2:     0.576 ms (Indexing: take)
        # Split time  3:     1.066 ms (Indexing: iloc)
        # Split time  4:     2.480 ms (Indexing: [])
        # Split time  5:     1.449 ms (Indexing: loc)

        # if benchmark:
        #     t = SplitTimer()
        #
        #     a = df.take(self.columns_copied_from_input_int, axis=1)
        #     t.print_timer('Indexing: take')
        #     b = df.iloc[:, self.columns_copied_from_input_int]
        #     t.print_timer('Indexing: iloc')
        #     c = df[self.columns_copied_from_input]
        #     t.print_timer('Indexing: []')
        #     d = df.loc[:, self.columns_copied_from_input]
        #     t.print_timer('Indexing: loc')
        #     self.out_ports[1] = a
        #     t.print_timer()
        # else:

        self.out_ports[1] = df.take(self.columns_copied_from_input_int, axis=1)
            #t.print_timer('Indexing: take')

        # Benchmarking getting a single column
        # if benchmark:
        #     i=27
        #     block = df._data.blocks[df._data._blknos[i]]
        #     values = block.iget(df._data._blklocs[i])
        #     t.print_timer('Direct access to block')
        #
        #     values=df._data.iget(i)
        #     t.print_timer('Access block manager')
        #
        #     values = df.iloc[:,i]
        #     t.print_timer('iloc')

        # Order of performance for adding a new column:
        #
        # Method:
        # - insert
        # - []
        # - loc[]
        #
        # Typical times:
        # Split time 14:     0.655 ms (Inserting: insert)
        # Split time 11:     0.746 ms (Inserting: [])
        # Split time 13:     1.451 ms (Inserting: loc)

        # if benchmark:
        #     a= self.out_ports[1].copy()
        #     t.print_timer()
        #     a[self.full_name] = self.f(self.in_ports[1].take(self.columns_used_int, axis=1))
        #     t.print_timer('Inserting: []')
        #
        #     a = self.out_ports[1].copy()
        #     t.print_timer()
        #     a.loc[:, self.full_name] = self.f(self.in_ports[1].take(self.columns_used_int, axis=1))
        #     t.print_timer('Inserting: loc')


        self.out_ports[1].insert(loc=len(self.out_ports[1].columns),
                                 column=self.full_name,
                                 value=self.f(self.in_ports[1].take(self.columns_used_int, axis=1)))
        #t.print_timer('Inserting: insert')

        #t.print_timer(last=True)

        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Tree Merge")

