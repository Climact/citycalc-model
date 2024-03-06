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
    JOINER NODE
    ============================
    KNIME options NOT implemented:
        - Joining column on any of the value that matches from the top and bottom input
        - Hiliting
        - Duplicate column handling with 'filter duplicates', 'don't execute' and 'append custom suffix'
        - Joining columns handling with 'remove joining columns from top input'
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class JoinerNode(PythonNode, NativeNode):
    knime_name = "Joiner"

    def __init__(
        self,
        joiner: str,
        left_input: list[str],
        right_input: list[str],
    ):
        super().__init__()
        self.joiner = joiner
        self.left_input = left_input
        self.right_input = right_input
        self.dico = {}
        self.new_column_names_right_input = []

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        left_input = []
        right_input = []

        model = xml_root.find(xmlns + "config[@key='model']")
        composition_mode = model.find(xmlns + "entry[@key='compositionMode']").get('value')
        join_mode = model.find(xmlns + "entry[@key='joinMode']").get('value')
        duplicate_handling = model.find(xmlns + "entry[@key='duplicateHandling']").get('value')
        left_include_all = model.find(xmlns + "entry[@key='leftIncludeAll']").get('value')
        right_include_all = model.find(xmlns + "entry[@key='rightIncludeAll']").get('value')
        rm_left_join_cols = model.find(xmlns + "entry[@key='rmLeftJoinCols']").get('value')
        rm_right_join_cols = model.find(xmlns + "entry[@key='rmRightJoinCols']").get('value')
        enable_hiliting = model.find(xmlns + "entry[@key='enableHiLite']").get('value')

        if enable_hiliting != 'false':
            cls.knime_error(id, "Hiliting has not been implement for the joiner")

        if duplicate_handling != 'AppendSuffixAutomatic':
            cls.knime_error(id, 
                f'The duplicate column handling {duplicate_handling} has not been implemented yet for the joiner')

        if left_include_all != 'true':
            cls.knime_error(id, "All column from top input must be included")

        if right_include_all != 'true':
            cls.knime_error(id, "All column from bottom input must be included")

        if rm_left_join_cols == 'true':
            cls.knime_error(id, 'Joining column from top input must not be removed')

        if rm_right_join_cols == 'false':
            cls.knime_error(id, 'Joining column from bottom input must be removed')

        if composition_mode == 'any':
            raise cls.knime_exception(id, "Composition node 'any' has not been implemented for joiner")

        if join_mode == 'InnerJoin':
            joiner = 'inner'
        elif join_mode == 'LeftOuterJoin':
            joiner = 'left'
        elif join_mode == 'RightOuterJoin':
            joiner = 'right'
        elif join_mode == 'FullOuterJoin':
            joiner = 'outer'

        xml_left_inputs = model.find(xmlns + "config[@key='leftTableJoinPredicate']").findall(xmlns + "entry")
        xml_right_inputs = model.find(xmlns + "config[@key='rightTableJoinPredicate']").findall(
            xmlns + "entry")

        for xml_left_input in xml_left_inputs:
            if xml_left_input.get('key') != 'array-size':
                left_input.append(xml_left_input.get('value'))

        for xml_right_input in xml_right_inputs:
            if xml_right_input.get('key') != 'array-size':
                right_input.append(xml_right_input.get('value'))

        if len(left_input) != len(right_input):
            cls.knime_error(id, left_input)
            cls.knime_error(id, right_input)
            raise cls.knime_exception(id, "Top input and bottom input must be the same for the joiner")

        self = PythonNode.init_wrapper(cls,
            joiner=joiner,
            left_input=left_input,
            right_input=right_input,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df_left, df_right) -> pd.DataFrame:
        if df_left.empty:
            raise self.exception("First input of Joiner is Empty")
        if df_right.empty:
            if '_interactions/workflows/data-processing/DB _ Macro_E (#3889)/Joiner (#1422)/settings.xml' \
                    not in self.xml_file:
                raise self.exception("Second input of Joiner is Empty")

        left_column = df_left.columns.tolist()
        right_column = df_right.columns.tolist()
        length = len(right_column)
        for i in range(length):
            this_str = right_column[i]
            old_name = right_column[i]
            if this_str not in self.right_input:
                while this_str in (list(set().union(left_column, right_column[0:i]))):
                    # FIXME: this may cause error if there is a (# not linked to a column number
                    if '(#' in this_str:
                        ls = this_str.replace('(#', ' ').replace(')', ' ').split()
                        ls[-1] = "(#" + str(int(ls[-1]) + 1) + ")"
                        this_str = " ".join(ls)

                    else:
                        this_str = this_str + " (#1)"
                right_column[i] = this_str
                self.dico[old_name] = this_str
        input_dico = dict(zip(self.right_input, self.left_input))
        if self.dico is not None:
            self.dico.update(input_dico)
        self.new_column_names_right_input = [self.dico.get(c, c) for c in df_right.columns]
        # if self.benchmark:
        #     t.print_timer('setup')
        df_right = df_right.rename(index=str, columns=self.dico)
        # if self.benchmark:
        #     t.print_timer('rename')
        df_merged = df_left.merge(df_right, how=self.joiner, on=self.left_input)
        # if self.benchmark:
        #     t.print_timer('merge', last=True)

        return df_merged

    # def update(self):
    #     df_left = self.in_ports[1]
    #     df_right = self.in_ports[2]
    #     df_right.columns = self.new_column_names_right_input
    #     df_merged = df_left.merge(df_right, how=self.joiner, on=self.left_input)
    #     self.out_ports[1] = df_merged
