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
    TREE MERGE GROUP METANODE
    ===================
    KNIME options implemented:
        - Everything
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class TreeMergeGroupsNode(PythonNode, SubNode):
    def __init__(
        self,
        unit: str = "unit",
        aggregation_method: str = "Product",
        aggregation_remove: str = "true",
        aggregation_pattern: str = ".*",
        new_column_name: str = "new-column",
    ):
        super().__init__()
        self.unit = unit
        self.aggregation_method = aggregation_method
        self.aggregation_remove = aggregation_remove
        self.aggregation_pattern = aggregation_pattern
        self.new_column_name = new_column_name

        self.columns_copied_from_input_int = []
        self.columns_used_dict = {}

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        # Check code version
        workflow_template_information = xml_root.find(xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The tree merge node (" + xml_file + ") is not connected to the EUCalc - Tree merge metanode template.")
        # else:
        #     timestamp = workflow_template_information.find(xmlns + "entry[@key='timestamp']").get('value')
        #     if timestamp != '2019-03-04 15:57:16' and timestamp != '2019-08-23 15:38:21' and timestamp != "2020-02-21 19:59:14":
        #         self.logger.error(
        #             "The template of the EUCalc - Tree merge metanode (Groups) was updated. Please check the version that you're using in " + xml_file)

        model = xml_root.find(xmlns + "config[@key='model']")

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            match child_key:
                case "string-input-349":
                    kwargs["new_column_name"] = value
                case "string-input-354":
                    kwargs["unit"] = value
                case "string-input-357":
                    kwargs["aggregation_pattern"] = value
                case "boolean-input-359":
                    kwargs["aggregation_remove"] = value
                case "single-selection-350":
                    kwargs["aggregation_method"] = value

        # self.pattern_a = self.patternreshape(self.local_flow_vars['aggregation-pattern'][1],  case_sensitive='true')
        # self.pattern_a = self.local_flow_vars['aggregation-pattern'][1]

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        output_table = df
        pat = self.aggregation_pattern

        name_components = self.new_column_name.split(',')

        grouped = output_table.groupby(output_table.columns.str.extract(pat, expand=False), axis=1)
        for name, group in grouped:
            if len(name_components) == 1:
                column_name = self.new_column_name + "_" + name + "[" + self.unit + "]"
            elif len(name_components) == 2:
                column_name = name_components[0] + "_" + name + "_" + name_components[1] + "[" + self.unit + "]"
            if self.aggregation_method == "Sum":
                output_table[column_name] = output_table[group.columns].sum(axis=1)
            elif self.aggregation_method == "Product":
                output_table[column_name] = output_table[group.columns].prod(axis=1)

            if self.aggregation_remove == 'true':
                output_table = output_table.drop(group.columns, axis=1)

            self.columns_used_dict[column_name] = [df.columns.get_loc(c) for c in group.columns]

        # Variables for the update
        columns_copied_from_input = output_table.columns.intersection(df.columns)
        self.columns_copied_from_input_int = [df.columns.get_loc(c) for c in
                                              columns_copied_from_input]

        if max([len(i) for i in self.columns_used_dict.values()])==1:
            # Speed up things if resulting columns are all 1-1 with their input columns
            self.f = lambda x: x
        elif self.aggregation_method == "Sum":
            self.f = lambda x: x.sum(axis=1)
        elif self.aggregation_method == "Product":
            self.f = lambda x: x.prod(axis=1)

        return output_table

    # def update(self):
    #      df = self.in_ports[1]
    #      self.out_ports[1] = df.take(self.columns_copied_from_input_int, axis=1)

    #      for column_name, columns in self.columns_used_dict.items():
    #          self.out_ports[1].insert(loc=len(self.out_ports[1].columns),
    #                                   column=column_name,
    #                                   value=self.f(self.in_ports[1].take(columns, axis=1)))
