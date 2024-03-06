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
    TABLE ROW TO VARIABLE NODE
    ============================
    KNIME options implemented:
        - None
"""

from patex.nodes.node import Context, PythonNode, NativeNode, FlowVars


class TableRowToVariableNode(PythonNode, NativeNode):
    knime_name = "Table Row to Variable"

    def __init__(self, var_classes: dict):
        for c in var_classes.values():
            if c not in ["DOUBLE", "INTEGER", "STRING"]:
                raise ValueError(f"datatype {c} is not implemented in Table Row to Variable node.")

        super().__init__()
        self.var_classes = var_classes

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        flow_stack = xml_root.find(xmlns + "config[@key='flow_stack']")
        variables = flow_stack.findall(xmlns + "config")
        if variables is None:
            raise Exception("The Table Row to Variable node has been saved without its flow stack. Exception occurred in :" + str(xml_file) + ". Please run the node, save and retry.")
        var_classes = {}
        for var in variables:
            var_name = var.find(xmlns + "entry[@key='name']").get('value')
            var_class = var.find(xmlns + "entry[@key='class']").get('value')
            var_classes[var_name] = var_class

        self = PythonNode.init_wrapper(cls, var_classes=var_classes)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> "FlowVars":
        flow_vars = {}
        for column in df.columns:
            flow_variable_name = column
            flow_variable_value = df[column].iloc[0]

            var_class = self.var_classes.get(flow_variable_name)
            if var_class is not None:
                flow_vars[flow_variable_name] = flow_variable_value
        rowNumber = df.count()

        # FIXME uncomment this
        # if rowNumber[0] >= 1:
        #     self.logger.debug("Table row to variable #"+str(self.id)+": "+str(rowNumber[0]+1) + " row(s).  All rows were ignored, except the first one - XML : " + str(self.xml_file))

        return flow_vars
