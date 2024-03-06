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
    WRAPPED NODE OUTPUT
    ============================
    KNIME options implemented:
        - Manual filtering
        - Enforce exclusion/inclusion
"""

from patex.nodes.node import Context, PythonNode, NativeNode


class WrappedNodeOutput(PythonNode, NativeNode):
    knime_name = "WrappedNode Output"

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
            self.in_ports[i+1] = None

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
            raise Exception(
                "Variable prefix not implemented (node #" + str(self.id) + "). Node's xml file is: " + xml_file)
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
            raise Exception("Regex/wildcard variable filtering is not implemented. All variables were kept (node #" + str(self.id) + "). Node's xml file is: " + xml_file)
            kwargs["keep_all_variables"] = True

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
                new_flow_vars = {}
                for var_to_add in self.var_to_add_List:
                    if var_to_add in flow_vars:
                        new_flow_vars[var_to_add] = flow_vars[var_to_add]
                    #else:
                    #    self.logger.debug(
                    #                'The variable ' + var_to_add + ' does not exist and cannot be included (node #' + str(
                    #                    self.id) + "). Node's xml file is: " + self.xml_file)

                flow_vars = new_flow_vars
                # raise Exception("The enforce option must be set on 'Enforce Exclusion' (node #" + str(self.id) + ")")

        return (flow_vars,) + tuple(ports[key] for key in sorted(ports.keys()))

    def run(self):
        for in_port in self.in_ports:
            self.supernode.save_out_port(in_port, self.read_in_port(in_port))

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
                new_flow_vars = {}
                for var_to_add in self.var_to_add_List:
                    if var_to_add in self.flow_vars:
                        new_flow_vars[var_to_add] = self.flow_vars[var_to_add]
                    #else:
                    #    self.logger.debug(
                    #                'The variable ' + var_to_add + ' does not exist and cannot be included (node #' + str(
                    #                    self.id) + "). Node's xml file is: " + self.xml_file)

                self.flow_vars = new_flow_vars
                # raise Exception("The enforce option must be set on 'Enforce Exclusion' (node #" + str(self.id) + ")")

        self.supernode.flow_vars = self.flow_vars
