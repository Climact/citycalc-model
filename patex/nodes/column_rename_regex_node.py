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
    COLUMN RENAME REGEX NODE
    ============================
    KNIME options implemented:
        - All
"""

import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class ColumnRenameRegexNode(PythonNode, NativeNode):
    knime_name = "Column Rename (Regex)"

    def __init__(self, search_string: str, replace_string: str):
        super().__init__()
        self.search_string = search_string
        self.replace_string = replace_string

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        search_string = model.find(xmlns + "entry[@key='searchString']").get('value')
        replace_string = model.find(xmlns + "entry[@key='replaceString']").get('value')
        is_case_insensitive = model.find(xmlns + "entry[@key='isCaseInsensitive']").get('value')

        if '|' in search_string:
            cls.knime_error(id, "Rename Regex is not supporting multiple searchstring at the same time using '|'")
        if '#' in search_string:
            cls.knime_error(id, "Renaming columns containing # is not a good practice. Please remove this before creating multiple columns with the same same")

        if is_case_insensitive == 'true':
            cls.knime_error(id, "Case insensitive not implemented")
            # for i in range(0, len(df.columns.values)):
            #     df.columns.values[i] = re.sub(searchString, replaceString, df.columns.values[i], flags=re.IGNORECASE)
            #     if df.columns.values[i] in df.columns.values[0:i]:
            #         raise cls.knime_exception(id, 
            #             "A column has already been named '" + df.columns.values[
            #                 i] + "'")
        is_literal = model.find(xmlns + "entry[@key='isLiteral']").get('value')
        if is_literal == "true":
            cls.knime_error(id, 'Literal option is not implemented')

        # if is_literal == 'false':
        # raise cls.knime_exception(id, "The node column rename regex can only accept literal expression")

        if '\\' in replace_string:
            cls.knime_debug(id, "Removing backslash in renaming string")
            replace_string = replace_string.replace('\\', '')

        if search_string.startswith("|") or search_string.endswith("|"):
            cls.knime_error(id, "Incoherent '|' placement in searchstring")

        if '$9' in replace_string:
            cls.knime_error(id, 
                'You reach the limit of groups for REGEX renaming (9) Consider changing the '
                'limit in the converter.')

        self = PythonNode.init_wrapper(cls, search_string=search_string, replace_string=replace_string)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            for key, var in self.flow_vars_params.items():
                if key == "searchString":
                    self.knime_input_mapping["searchString"] = var
                elif key == "replaceString":
                    self.knime_input_mapping["replaceString"] = var
                else:
                    raise self.exception(f"The parameter '{key}' cannot be handled by a flow variable")
        return self

    def apply(self, df, search_string = None, replace_string = None) -> pd.DataFrame:
        search_string = search_string if search_string is not None else self.search_string
        replace_string = replace_string if replace_string is not None else self.replace_string
        d = {}
        for i in range(0, len(df.columns.values)):
            if search_string.startswith("("):
                matcher = re.match(search_string+".*", df.columns.values[i])
            else:
                matcher = re.match(".*"+search_string+".*", df.columns.values[i])
            if matcher is not None:
                new_replace_string = replace_string
                if '$' in replace_string:
                    for j in range(1, 9):
                        pattern_str = '$' + str(j)
                        if pattern_str in replace_string:
                            new_replace_string = re.sub("\\" + pattern_str, matcher.group(j), new_replace_string)
                            #if j == 9:
                            #    raise self.exception(
                            #       'You reach the limit of groups for REGEX renaming (9) Consider changing the '
                            #      'limit in the converter.')

                # df.columns.values[i] = replaceString
                d[df.columns.values[i]] = re.sub(search_string, new_replace_string, df.columns.values[i])
                if df.columns.values[i] in df.columns.values[0:i]:
                    raise self.exception(f"A column has already been named '{df.columns.values[i]}'")
        return df.rename(columns=d)
