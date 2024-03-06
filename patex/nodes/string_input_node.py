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
    STRING INPUT NODE
    ============================
    KNIME options implemented:
        - Nothing, the run() method only creates a flow variable with the right name
                    and value: default_value
"""

from patex.nodes.node import Context, PythonNode, NativeNode, FlowVars


class StringInputNode(PythonNode, NativeNode):
    knime_name = "String Input"

    def __init__(
        self,
        flow_variable_name: str,
        default_value: str,
    ):
        super().__init__()
        self.flow_variable_name = flow_variable_name
        self.default_value = default_value

    def init_ports(self):
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        flow_variable_name = model.find(xmlns + "entry[@key='flowvariablename']").get('value')
        default_value = model.find(xmlns + "config[@key='defaultValue']").find(xmlns + "entry[@key='string']").get('value')

        self = PythonNode.init_wrapper(cls,
            flow_variable_name=flow_variable_name,
            default_value=default_value,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, flow_vars: "FlowVars") -> "FlowVars":
        flow_vars[self.flow_variable_name] = self.default_value
        return flow_vars
