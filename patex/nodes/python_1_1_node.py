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
    PYTHON 1 TO 1 NODE
    ============================
    KNIME options implemented:
        - Everything except execution of Python 2 code, conversion of missing values/sentinel values and sentinel values other than MIN_VAL.
                Row chunking, and row limits not implemented either.
"""

import collections

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode, FlowVars


class Python11Node(PythonNode, NativeNode):
    knime_name = "Python Script (1⇒1)"

    # TODO: See what row chunking does, and check if any exceptions need to be raised concerning it.
    def __init__(self, code: str):
        super().__init__()
        self.program = compile(code, 'code_String', 'exec', optimize=-1)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        source_code = model.find(xmlns + "entry[@key='sourceCode']").get('value')
        python_version = model.find(xmlns + "entry[@key='pythonVersionOption']").get('value')
        convert_miss_to_python = model.find(xmlns + "entry[@key='convertMissingToPython']").get('value')
        convert_miss_from_python = model.find(xmlns + "entry[@key='convertMissingFromPython']").get('value')
        sentinel_option = model.find(xmlns + "entry[@key='sentinelOption']").get('value')

        if python_version != "PYTHON3" and python_version != "python3":
            cls.knime_error(id, "execution of Python 2 code not implemented!")

        if convert_miss_to_python != "false":
            cls.knime_error(id, "conversion of missing values to sentinel values not implemented")

        if convert_miss_from_python != "false":
            cls.knime_error(id, "conversion of sentinel values to missing values not implemented")

        if sentinel_option != "MIN_VAL":
            cls.knime_error(id, "sentinel value other than MIN_VAL not implemented")

        source_code = source_code.replace('%%00010', '\n')
        source_code = source_code.replace('%%00009', '\t')
        source_code = source_code.replace('&quot;', '"')
        source_code = source_code + '\n'  # whitespace padding in case the code ends on a loop that requires a final "enter" to be run

        self = PythonNode.init_wrapper(cls, code=source_code)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, input_table) -> pd.DataFrame:
        ns = {'output_table': None, 'input_table': input_table}
        object_columns = [col for col in input_table.select_dtypes(include='object').columns if
                          col not in ["Country", "Years"]]
        for col in object_columns:
            input_table[col] = input_table[col].astype(dtype=float, errors='ignore')

        exec(self.program, ns)

        return ns['output_table']
