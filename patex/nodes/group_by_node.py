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
    GROUP BY NODE
    ============================
    KNIME options implemented:
        - All
    CAVEATS
        - Doesn't always work correctly with missing values!
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class GroupByNode(PythonNode, NativeNode):
    knime_name = "GroupBy"
    
    def __init__(
        self,
        to_group: list[str],
        pattern: list[str],
        pattern_aggregation_method: list[str],
        method_list_manual: list[str],
        aggregation_list_manual: list[str],
        to_aggregate_manual: list[str],
        name_policy: str,
    ):
        super().__init__()
        self.to_group = to_group
        self.pattern = pattern
        self.pattern_aggregation_method = pattern_aggregation_method
        self.method_list_manual = method_list_manual
        self.aggregation_list_manual = aggregation_list_manual
        self.to_aggregate_manual = to_aggregate_manual
        self.name_policy = name_policy

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        group_by_cols = model.find(xmlns + "config[@key='grouByColumns']")
        aggregation_column = model.find(xmlns + "config[@key='aggregationColumn']")
        aggr_tree = aggregation_column.find(xmlns + "config[@key='columnNames']")
        aggr_method_tree = aggregation_column.find(xmlns + "config[@key='aggregationMethod']")
        incl_missing_vals = aggregation_column.find(xmlns + "config[@key='inclMissingVals']")

        incl_tree = group_by_cols.find(xmlns + "config[@key='InclList']")
        to_group = [c.get('value') for c in incl_tree if c.get('key') != 'array-size']

        enable_hilite = model.find(xmlns + "entry[@key='enableHilite']").get('value')
        if enable_hilite != "false":
            raise cls.knime_exception(id, "Hilite not implemented!")

        pattern = []
        pattern_aggregation_method = []

        pattern_aggregators = model.find(xmlns + "config[@key='patternAggregators']")
        for child in pattern_aggregators:
            if child.find(xmlns + "entry[@key='isRegularExpression']").get('value') == 'true':
                pattern.append(child.find(xmlns + "entry[@key='inputPattern']").get('value'))
                pattern_aggregation_method.append(child.find(xmlns + "entry[@key='aggregationMethod']").get('value'))

            else:
                raise cls.knime_exception(id, "Only regex pattern are accepted in GroupBy nodes")

        # MANUAL Aggregation columns
        method_list_manual = []
        aggregation_list_manual = []
        to_aggregate_manual = []

        for child in aggr_tree:
            if child.get('key') != "array-size":
                to_aggregate_manual.append(child.get('value'))
                aggregation_list_manual.append(child.get('value'))

        for child in aggr_method_tree:
            if child.get('key') != "array-size":
                method_list_manual.append(child.get('value'))

        # FIXME not used so I commented it
        # for child in incl_missing_vals:
        #     if child.get('key') != "array-size":
        #         missing_val_policy.append(child.get('value'))

        # DATATYPE Aggregation : not implemented
        data_type_aggregators = model.find(xmlns + "config[@key='dataTypeAggregators']")
        raise_datatype_exception = False
        for child in data_type_aggregators:
            raise_datatype_exception = True
        if raise_datatype_exception:
            cls.knime_error(id, "DataType Aggregators not implemented!")

        name_policy = model.find(xmlns + "entry[@key='columnNamePolicy']").get('value')

        self = PythonNode.init_wrapper(cls,
            to_group=to_group,
            pattern=pattern,
            pattern_aggregation_method=pattern_aggregation_method,
            method_list_manual=method_list_manual,
            aggregation_list_manual=aggregation_list_manual,
            to_aggregate_manual=to_aggregate_manual,
            name_policy=name_policy,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self


    def apply(self, df) -> pd.DataFrame:
        # ------------------------------------------------------
        # Grouping Dataframe
        # ------------------------------------------------------
        grouped = df.groupby(self.to_group)

        # ------------------------------------------------------
        # Selecting Aggregation column selection type
        # ------------------------------------------------------

        # Implemented functions
        aggr_rename_dict = {'Mean': 'mean', 'Sum_V2.5.2': 'sum', 'First': 'first', 'Count': 'count', "Maximum": 'max', "Minimum": 'min'}
        aggr_dict_pattern = {}
        aggregation_list_pattern = []
        method_list_pattern = []

        # PATTERN based Aggregation columns
        for i,pat in enumerate(self.pattern):
            grouped_columns = df.filter(regex=pat)
            to_aggregate_pattern = list(grouped_columns.columns)
            for col in to_aggregate_pattern:
                if col not in self.to_group:
                    aggr_dict_pattern[col] = aggr_rename_dict[self.pattern_aggregation_method[i]]
                    aggregation_list_pattern.append(col)
                    method_list_pattern.append(self.pattern_aggregation_method[i])


        # Compute the above value to create a dictionary
        aggr_methods_renamed = [aggr_rename_dict.get(item, item) for item in
                                self.method_list_manual]  # rename the list of aggregation method using the above dictionary
        aggr_dict_manual = dict(zip(self.to_aggregate_manual, aggr_methods_renamed))



        # ------------------------------------------------------
        # Grouping dictionary of columns : {'column' : 'method'}
        # ------------------------------------------------------
        aggr_dict = {**aggr_dict_manual,**aggr_dict_pattern}
        method_list = self.method_list_manual + method_list_pattern
        aggregation_list = self.aggregation_list_manual + aggregation_list_pattern

        try:
            df_out = grouped.agg(aggr_dict).reset_index()
        except AttributeError as error:
            raise error

        # ------------------------------------------------------
        # Renaming columns : {'column' : 'new name'}
        # ------------------------------------------------------
        if self.name_policy == "Aggregation method (column name)":
            aggr_rename_col_dict = {'Mean': 'Mean', 'Sum_V2.5.2': 'Sum', 'First': 'First*', 'Count': 'Count*'}
            aggr_methods_col = [aggr_rename_col_dict.get(item, item) for item in
                                method_list]  # rename the list of aggregation method using the above dictonary
            aggr_col_dict = dict(zip(aggregation_list, aggr_methods_col))

            aggr_col_dict = {x: y + '(' + x + ')' for x, y in aggr_col_dict.items()}

            df_out.rename(columns=aggr_col_dict, inplace=True)
            aggregation_list = [aggr_col_dict.get(item, item) for item in
                                aggregation_list]  # rename the list of aggregation method using the above dictonary
        elif self.name_policy == "Column name (aggregation method)":
            aggr_rename_col_dict = {'Mean': 'Mean', 'Sum_V2.5.2': 'Sum', 'First': 'First', 'Count': 'Count*'}
            aggr_methods_col = [aggr_rename_col_dict.get(item, item) for item in
                                method_list]  # rename the list of aggregation method using the above dictonary
            aggr_col_dict = dict(zip(aggregation_list, aggr_methods_col))
            aggr_col_dict = {x: x + ' (' + y + ')' for x, y in aggr_col_dict.items()}

            df_out.rename(columns=aggr_col_dict, inplace=True)
            aggregation_list = [aggr_col_dict.get(item, item) for item in
                                aggregation_list]  # rename the list of aggregation method using the above dictonary

        # df_out = df_out.sort_values(self.to_group)  # Make sure you reorder so output corresponds to knime

        if df_out.empty:
            self.error("Output of GroupBy node is Empty. Check the parameter of the node and chose REGEX or MANUAL selection, not both !")

        return df_out[self.to_group+aggregation_list]

# Available functions
# mean()	Compute mean of groups
# sum()	Compute sum of group values
# size()	Compute group sizes
# count()	Compute count of group
# std()	Standard deviation of groups
# var()	Compute variance of groups
# sem()	Standard error of the mean of groups
# describe()	Generates descriptive statistics
# first()	Compute first of group values
# last()	Compute last of group values
# nth()	Take nth value, or a subset if n is a list
# min()	Compute min of group values
# max()	Compute max of group values
