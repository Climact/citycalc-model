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
    UNPIVOTING NODE
    ============================
    KNIME options NOT implemented:
        - Value Columns : selection by type and wildcard and 'skip rows containing missing cells'
        - Retaining column: selection by type and wildcard
        - Hiliting
"""

import operator
import re
from functools import reduce

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


class UnpivotingNode(PythonNode, NativeNode):
    knime_name = "Unpivoting"

    def __init__(
        self,
        filter_type: str,
        retained_type: str,
        id_variables: list = [],
        id_values: list = [],
        to_keep: list = [],
        not_to_keep: list = [],
        a_filter: str | None = None,
        a_retained: str | None = None,
        regex_or_wildcard: str | None = None,
        entry_type: str | None = None,
        case_sensitive_filter: str | None = None,
        this_str: str = '^',
    ):
        super().__init__()
        self.a_filter = a_filter
        self.a_retained = a_retained
        self.case_sensitive_filter = case_sensitive_filter
        self.entry_type = entry_type
        self.filter_type = filter_type
        self.id_values = id_values
        self.id_variables = id_variables
        self.not_to_keep = not_to_keep
        self.regex_or_wildcard = regex_or_wildcard
        self.retained_type = retained_type
        self.this_str = this_str
        self.to_keep = to_keep
        
    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}
        id_values = []
        id_variables = []
        to_keep = [] # or
        not_to_keep = [] # that is the question
        
        model = xml_root.find(xmlns + "config[@key='model']")
        filter_type = model.find(xmlns + "config[@key='value_columns']").find(xmlns + "entry[@key='filter-type']").get('value')
        missing_val = model.find(xmlns + "entry[@key='missing-values']").get('value')
        hiliting = model.find(xmlns + "entry[@key='enable-hiliting']").get('value')

        # FIXME uncomment this
        # if hiliting == 'true':
        #     self.logger.debug("Hiliting has not been enabled for unpivoting node (node #"+ str(self.id_xml) +")")
        if missing_val == 'true':
            self.logger.error("Skip row containing missing value is not handled by unpivoting node (node #"+str(self.id_xml)+")")

        if filter_type == 'STANDARD':
            xml_to_include = model.find(xmlns + "config[@key='value_columns']").find(xmlns + "config[@key='included_names']").findall(xmlns + "entry")
            xml_to_exclude = model.find(xmlns + "config[@key='value_columns']").find(
                xmlns + "config[@key='excluded_names']").findall(xmlns + "entry")

            for column_to_include in xml_to_include:
                if column_to_include.get('key') != 'array-size':
                    id_values.append(column_to_include.get('value'))

            for column_to_exclude in xml_to_exclude:
                if column_to_exclude.get('key') != 'array-size':
                    id_variables.append(column_to_exclude.get('value'))

        elif filter_type == 'datatype':
            raise Exception('Type selection has not been implemented for unpivoting (node #'+str(self.id_xml)+')')

        elif filter_type == 'name_pattern':
            entry = model.find(xmlns + "config[@key='value_columns']").find(
                xmlns + "config[@key='name_pattern']")
            kwargs["entry_type"] = entry.find(xmlns + "entry[@key='type']").get('value')
            if kwargs["entry_type"] == 'Regex':
                kwargs["case_sensitive_filter"] = entry.find(xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = entry.find(xmlns + "entry[@key='pattern']").get('value')
                if pattern.startswith(" "):
                    pattern = pattern[1:]
                if pattern.startswith('|'):
                    pattern = pattern[1:]
                if pattern.endswith('|'):
                    pattern = pattern[:-1]
                kwargs["a_filter"] = pattern#.split('.')  # Pattern in xml : 'pattern.*'
            else:
                raise Exception("Wildcard column filtering is not implemented (node"+str(self.id_xml)+")")

        retained_type = model.find(xmlns + "config[@key='retained_columns']").find(
            xmlns + "entry[@key='filter-type']").get('value')

        if retained_type == 'STANDARD':
            xml_to_retain = model.find(xmlns + "config[@key='retained_columns']").find(
                xmlns + "config[@key='included_names']").findall(xmlns + "entry")
            xml_not_to_retain = model.find(xmlns + "config[@key='retained_columns']").find(
                xmlns + "config[@key='excluded_names']").findall(xmlns + "entry")

            for column_to_include in xml_to_retain:
                if column_to_include.get('key') != 'array-size':
                    to_keep.append(column_to_include.get('value'))

            for column_to_exclude in xml_not_to_retain:
                if column_to_exclude.get('key') != 'array-size':
                    not_to_keep.append(column_to_exclude.get('value'))
        elif retained_type == "name_pattern":
            kwargs["regex_or_wildcard"]  = model.find(xmlns + "config[@key='retained_columns']").find(xmlns + "config[@key='name_pattern']").find(xmlns + "entry[@key='type']").get('value')
            if kwargs["regex_or_wildcard"] == 'Regex':
                # case_sensitive_retained = model.find(xmlns + "config[@key='retained_columns']").find(xmlns + "config[@key='name_pattern']").find(xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = model.find(xmlns + "config[@key='retained_columns']").find(xmlns + "config[@key='name_pattern']").find(xmlns + "entry[@key='pattern']").get('value')
                if pattern.startswith(" "):
                    pattern = pattern[1:]
                if pattern.startswith('|'):
                    pattern = pattern[1:]
                if pattern.endswith('|'):
                    pattern = pattern[:-1]
                kwargs["a_retained"] = pattern#.split('.')  # Pattern in xml : 'pattern.*'
            else:
                raise Exception('Retaining column by wildcard has not been implement for pivoting node (node #'+str(self.id_xml)+')')
        else:
            raise Exception('Retaining column by type has not been implemented for pivoting node (node #'+str(self.id_xml)+')')

        self = PythonNode.init_wrapper(cls,
            filter_type=filter_type,
            id_values=id_values,
            id_variables=id_variables,
            not_to_keep=not_to_keep,
            retained_type=retained_type,
            to_keep=to_keep,
            **kwargs
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        id_variables = self.id_variables.copy()
        id_values = self.id_values.copy()
        cols_to_keep = self.to_keep.copy()
        cols_not_to_keep = self.not_to_keep.copy()
        column_to_check = df.columns

        # rowIDS = pd.Series([])
        # nb_row, nb_column = df.shape
        # for m in range(nb_row):
        #     rowIDS[m] = "Row" + str(m)
        #
        # df.insert(0, 'RowIDs', rowIDS)

        # FIXME uncomment this
        # self.logger.debug('Row IDs column desactivated in Unpivotting node.')

        if self.filter_type == 'name_pattern':
            if self.entry_type == 'Regex':
                if self.case_sensitive_filter == 'true':
                    id_values = [col for col in column_to_check if re.match(r''+self.this_str+self.a_filter,col)]
                    id_variables = [col for col in column_to_check if not re.match(r'' + self.this_str + self.a_filter, col)]
                else:
                    id_values = [col for col in column_to_check if re.match(r''+self.this_str+self.a_filter,col,re.IGNORECASE)]
                    id_variables = [col for col in column_to_check if not re.match(r'' + self.this_str + self.a_filter, col,re.IGNORECASE)]


        # id_variables.append('RowIDs')
        df = df.melt(id_vars=id_variables, value_vars=id_values, var_name='ColumnNames', value_name='ColumnValues')

        if self.retained_type == "name_pattern":
            if self.regex_or_wildcard == 'Regex':
                if self.case_sensitive_filter == 'true':
                    cols_to_keep = [col for col in column_to_check if re.match(r''+self.this_str+self.a_retained,col)]
                    cols_not_to_keep = [col for col in column_to_check if not re.match(r'' + self.this_str + self.a_retained, col)]
                else:
                    cols_to_keep = [col for col in column_to_check if re.match(r''+self.this_str+self.a_retained,col,re.IGNORECASE)]
                    cols_not_to_keep = [col for col in column_to_check if not re.match(r'' + self.this_str + self.a_retained, col,re.IGNORECASE)]

        length = len(id_values)
        # df.sort_values('RowIDs', inplace = True)
        for column_to_keep in cols_to_keep:
            if column_to_keep in id_values:  # We have to create a column here because melt didn't create it
                new_df = df[df['ColumnNames']==column_to_keep]
                this_list = new_df['ColumnValues'].values
                new_val = []
                for i in range(len(this_list)):
                    new_val.append([this_list[i]]*length)

                new_val = reduce(operator.concat, new_val)
                df[column_to_keep] = new_val

        for column_to_drop in cols_not_to_keep:
            if column_to_drop in id_variables:
                df = df.drop(column_to_drop, axis=1)

        return df