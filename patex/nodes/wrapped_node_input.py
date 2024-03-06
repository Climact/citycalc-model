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

   WRAPPED NODE INPUT
    ============================
    KNIME options implemented:
        - Manual filtering
        - Enforce exclusion/inclusion
"""

from patex.nodes.node import Context, PythonNode, NativeNode


class WrappedNodeInput(PythonNode, NativeNode):
    knime_name = "WrappedNode Input"

    def __init__(
        self,
        port_num: int,
        var_to_add_List: list = [],
        var_to_drop_List: list = [],
        enforce_option_isTrue: bool = False,
        keep_all_variables: bool = False,
    ):
        # `port_num` is needed for `super().__init__`, because it calls `self.init_ports`
        self.port_num = port_num
        super().__init__()
        self.var_to_add_List = var_to_add_List
        self.var_to_drop_List = var_to_drop_List
        self.enforce_option_isTrue = enforce_option_isTrue
        self.keep_all_variables = keep_all_variables

    def init_ports(self):
        for i in range(0, self.port_num):
            self.out_ports[i+1] = None

    def n_outputs(self):
        return self.port_num + 1

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}
        var_to_add_List = []
        var_to_drop_List = []

        port_num = int(xml_root.find(xmlns + "config[@key='model']" + xmlns + "config[@key='port-names']" + xmlns + "entry[@key='array-size']").get('value'))
        
        model = xml_root.find(xmlns + "config[@key='model']")
        type_of_filter = model.find(xmlns + "config[@key='variable-filter']").find(
            xmlns + "entry[@key='filter-type']").get('value')

        variable_prefix_is_null = model.find(xmlns + "entry[@key='variable-prefix']").get('isnull')
        if variable_prefix_is_null != "true":
            self.logger.error(
                "Variable prefix not implemented (node #" + str(id) + "). Node's xml file is: " + xml_file)

        if type_of_filter == 'STANDARD':
            enforce_option = model.find(xmlns + "config[@key='variable-filter']").find(
                xmlns + "entry[@key='enforce_option']").get('value')
            if enforce_option == 'EnforceExclusion':
                kwargs["enforce_option_isTrue"] = True
                vars_to_be_filtered = model.find(xmlns + "config[@key='variable-filter']").find(
                    xmlns + "config[@key='excluded_names']")
                vars_to_be_filtered_findall = vars_to_be_filtered.findall(xmlns + "entry")
                for var_to_be_filtered in vars_to_be_filtered_findall:
                    if var_to_be_filtered.get('key') != 'array-size':
                        var_to_drop_List.append(var_to_be_filtered.get('value'))

            else:
                kwargs["enforce_option_isTrue"] = False
                vars_to_keep = model.find(xmlns + "config[@key='variable-filter']").find(
                    xmlns + "config[@key='included_names']")
                vars_to_keep_findall = vars_to_keep.findall(xmlns + "entry")
                for var_to_keep in vars_to_keep_findall:
                    if var_to_keep.get('key') != 'array-size':
                        var_to_add_List.append(var_to_keep.get('value'))
        else:
            # FIXME uncomment this
            # self.logger.warning("Regex/wildcard variable filtering is not implemented. All variable were kept. (node #" + str(self.id)
            #                     + "). Node's xml file is: " + xml_file)
            kwargs["keep_all_variables"] = True

        # flow_stack = xml_root.find(xmlns + "config[@key='flow_stack']")
        # for var in flow_stack.findall(xmlns + "config"):
        #     var_val = var.find(xmlns + "entry[@key='value']").get("value")
        #     var_name = var.find(xmlns + "entry[@key='name']").get("value")
        #     var_class = var.find(xmlns + "entry[@key='class']").get('value')
        #     var_type = var.find(xmlns + "entry[@key='type']").get('value')
        #     if var_type == 'variable':
        #         if var_class == "DOUBLE":
        #             self.flow_vars[var_name] = ("DOUBLE", float(var_val))
        #         elif var_class == "INTEGER":
        #             self.flow_vars[var_name] = ("INTEGER", int(var_val))
        #         elif var_class == "STRING":
        #             self.flow_vars[var_name] = ("STRING", var_val)
        #         else:
        #             self.logger.error(
        #                 "This datatype is not implemented in Wrapped node Input. Exception occurred in :" + str(
        #                     xml_file) + ".")

        self = PythonNode.init_wrapper(cls,
            port_num=port_num,
            var_to_add_List=var_to_add_List,
            var_to_drop_List=var_to_drop_List,
            **kwargs
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, flow_vars, **ports):
        if not self.keep_all_variables:
            # Manual type of filtering
            # if the type of filter is 'STANDARD':
            if self.enforce_option_isTrue:
                for var_to_drop in self.var_to_drop_List:
                    if var_to_drop in flow_vars:
                        del flow_vars[var_to_drop]
                    #else:
                    #    self.logger.debug(
                    #                'The variable ' + var_to_drop + ' does not exist and cannot be excluded (node #' + str(
                    #                    self.id) + "). Node's xml file is: " + self.xml_file)
            else:
                # enforce inclusion
                new_flow_vars = {}
                for var_to_add in self.var_to_add_List:
                    if var_to_add in flow_vars:
                        new_flow_vars[var_to_add] = flow_vars[var_to_add]
                    #else:
                        #self.logger.debug(
                        #    'The variable ' + var_to_add + ' does not exist and cannot be included (node #' + str(
                        #        self.id) + "). Node's xml file is: " + self.xml_file)

                flow_vars = new_flow_vars

        return (flow_vars,) + tuple(ports[key] for key in sorted(ports.keys()))

    def run(self):
        for out_port in self.out_ports:
            self.save_out_port(out_port, self.supernode.read_in_port(out_port))

        self.flow_vars = self.supernode.flow_vars

        if not self.keep_all_variables:
            # Manual type of filtering
            # if the type of filter is 'STANDARD':
            if self.enforce_option_isTrue:
                for var_to_drop in self.var_to_drop_List:
                    if var_to_drop in self.flow_vars:
                        del self.flow_vars[var_to_drop]
                    #else:
                    #    self.logger.debug(
                    #                'The variable ' + var_to_drop + ' does not exist and cannot be excluded (node #' + str(
                    #                    self.id) + "). Node's xml file is: " + self.xml_file)
            else:
                # enforce inclusion
                new_flow_vars = {}
                for var_to_add in self.var_to_add_List:
                    if var_to_add in self.flow_vars:
                        new_flow_vars[var_to_add] = self.flow_vars[var_to_add]
                    #else:
                        #self.logger.debug(
                        #    'The variable ' + var_to_add + ' does not exist and cannot be included (node #' + str(
                        #        self.id) + "). Node's xml file is: " + self.xml_file)

                self.flow_vars = new_flow_vars

        # Regex type of filtering
        """if type_of_filter == 'name_pattern':
            entry = model.find(self.xmlns + "config[@key='column-filter']").find(
                self.xmlns + "config[@key='name_pattern']")
            if entry.find(self.xmlns + "entry[@key='type']").get('value') == 'Regex':
                case_sensitive = entry.find(self.xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = entry.find(self.xmlns + "entry[@key='pattern']").get('value')
                a = pattern.split('.')  # Pattern in xml : 'pattern.*'
                this_str = '^'  # For the regex to only look at the begining of the word
                if case_sensitive == 'true':
                    df = df.filter(regex=this_str + a[0])
                else:
                    df = df.filter(regex=re.compile(this_str + a[0], re.IGNORECASE))
            else:
                raise Exception("Wildcard column filtering is not implemented (node #" + str(id) + ")")"""




""" NEW VERSION OF THE CODE THAT WASN'T TESTED
from patex.nodes.node import Node
from timeit import default_timer as timer


class WrappedNodeInput(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.type_of_filter = []
        self.enforce_option = []
        self.vars_to_be_filtered = []
        self.dic_var_to_be_filtered_key = {}
        self.dic_var_to_drop = {}
        self.vars_to_keep = []
        self.dic_var_to_keep_key = {}
        self.dic_var_to_add = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        port_num = self.xml_root.find(self.xmlns + "config[@key='model']" + self.xmlns + "config[@key='port-names']" + self.xmlns + "entry[@key='array-size']").get('value')
        for i in range(0, int(port_num)):
            self.out_ports[i+1] = None

    def build_node(self):
        start = timer()
        # logger
        id = self.id

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.type_of_filter = model.find(self.xmlns + "config[@key='variable-filter']").find(
            self.xmlns + "entry[@key='filter-type']").get('value')

        variable_prefix_is_null = model.find(self.xmlns + "entry[@key='variable-prefix']").get('isnull')
        if variable_prefix_is_null != "true":
            raise Exception(
                "Variable prefix not implemented (node #" + str(id) + "). Node's xml file is: " + self.xml_file)

        # Manual type of filtering
        if self.type_of_filter == 'STANDARD':
            self.enforce_option = model.find(self.xmlns + "config[@key='variable-filter']").find(
                self.xmlns + "entry[@key='enforce_option']").get('value')
            if self.enforce_option == 'EnforceExclusion':
                self.vars_to_be_filtered = model.find(self.xmlns + "config[@key='variable-filter']").find(
                    self.xmlns + "config[@key='excluded_names']").findall(self.xmlns + "entry")
                for var_to_be_filtered in self.vars_to_be_filtered:
                    self.dic_var_to_be_filtered_key[var_to_be_filtered] = var_to_be_filtered.get('key')
                    if self.dic_var_to_be_filtered_key[var_to_be_filtered] != 'array-size':
                        self.dic_var_to_drop[var_to_be_filtered] = var_to_be_filtered.get('value')
            else:
                # enforce inclusion
                self.vars_to_keep = model.find(self.xmlns + "config[@key='variable-filter']").find(
                    self.xmlns + "config[@key='included_names']").findall(self.xmlns + "entry")
                for var_to_keep in self.vars_to_keep:
                    self.dic_var_to_keep_key[var_to_keep] = var_to_keep.get('key')
                    if self.dic_var_to_keep_key[var_to_keep] != 'array-size':
                        self.dic_var_to_add[var_to_keep] = var_to_keep.get('value')
                        
        else:
            raise Exception("Regex/wildcard variable filtering is not implemented (node #" + str(
                id) + "). Node's xml file is: " + self.xml_file)

        t = timer() - start
        self.log_timer(t, "BUILD")
        # flow_stack = self.xml_root.find(self.xmlns + "config[@key='flow_stack']")
        # for var in flow_stack.findall(self.xmlns + "config"):
        #     var_val = var.find(self.xmlns + "entry[@key='value']").get("value")
        #     var_name = var.find(self.xmlns + "entry[@key='name']").get("value")
        #     var_class = var.find(self.xmlns + "entry[@key='class']").get('value')
        #     var_type = var.find(self.xmlns + "entry[@key='type']").get('value')
        #     if var_type == 'variable':
        #         if var_class == "DOUBLE":
        #             self.flow_vars[var_name] = ("DOUBLE", float(var_val))
        #         elif var_class == "INTEGER":
        #             self.flow_vars[var_name] = ("INTEGER", int(var_val))
        #         elif var_class == "STRING":
        #             self.flow_vars[var_name] = ("STRING", var_val)
        #         else:
        #             raise Exception(
        #                 "This datatype is not implemented in Wrapped node Input. Exception occurred in :" + str(
        #                     self.xml_file) + ".")
        pass

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        for out_port in self.out_ports:
            self.save_out_port(out_port, self.supernode.read_in_port(out_port))

        self.flow_vars = self.supernode.flow_vars

        id = self.id

        if self.type_of_filter == 'STANDARD':
            if self.enforce_option == 'EnforceExclusion':
                for var_to_be_filtered in self.vars_to_be_filtered:
                    if self.dic_var_to_be_filtered_key[var_to_be_filtered] != 'array-size':
                        if self.dic_var_to_drop[var_to_be_filtered] in self.flow_vars:
                            del self.flow_vars[self.dic_var_to_drop[var_to_be_filtered]]
                        else:
                            self.logger.warning(
                                'The variable ' + self.dic_var_to_drop[var_to_be_filtered] + ' does not exist and cannot be excluded (node #' + str(
                                    id) + ") Node's xml file is: " + self.xml_file)

            else:
                # enforce inclusion
                new_flow_vars = {}
                for var_to_keep in self.vars_to_keep:
                    if self.dic_var_to_keep_key[var_to_keep] != 'array-size':
                        if self.dic_var_to_add[var_to_keep] in self.flow_vars:
                            new_flow_vars[self.dic_var_to_add[var_to_keep]] = self.flow_vars[self.dic_var_to_add[var_to_keep]]
                        else:
                            self.logger.warning(
                                'The variable ' + self.dic_var_to_add[var_to_keep] + ' does not exist and cannot be included (node #' + str(
                                    id) + ") Node's xml file is: " + self.xml_file)
                self.flow_vars = new_flow_vars
        # logger
        t = timer() - start
        self.log_timer(t, "END")

        # Regex type of filtering
        """