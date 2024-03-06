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
    PIVOTING NODE
    ============================
    KNIME options implemented:
        - Advanced settings: hiliting, process in memory, retain row ordeor, maximum unique value per group, all column naming except for 'keep original name'
        - Pivot settings: append overal totals, not ignore domain, not ignore missing values
        - Manual aggregation: Aggregation on several columns and all methods except for median, mean and sum
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


AGGR_RENAME_COL_DICT = {
    "mean": "Mean",
    "sum": "Sum",
    "first": "First*",
    "count": "Count*",
    "max": "Maximum",
}


class PivotingNode(PythonNode, NativeNode):
    knime_name = "Pivoting"

    def __init__(
        self,
        agg_dict: dict,
        column_name_option: str,
        column_name_policy: str,
        list_group_columns: list[str],
        list_pivots: list[str],
    ):
        super().__init__()
        self.agg_dict = agg_dict
        self.column_name_option = column_name_option
        self.column_name_policy = column_name_policy
        self.list_group_columns = list_group_columns
        self.list_pivots = list_pivots

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None, 2: None, 3: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        column_name_policy = model.find(xmlns + "entry[@key='columnNamePolicy']").get('value')
        column_name_option = model.find(xmlns + "entry[@key='column_name_option']").get('value')
        list_pivots = []
        list_group_columns = []
        agg_dict = {}
        if column_name_option != "Pivot name+Aggregation name":
            cls.knime_error(id, "The pivot node is not using the Pivot Name + Aggregation Name option in the column name which may produce errors in the converter")
        max_unique_value = model.find(xmlns + "entry[@key='maxNoneNumericalVals']").get('value')
        enable_hiliting = model.find(xmlns + "entry[@key='enableHilite']").get('value')
        missing_values = model.find(xmlns + "entry[@key='missing_values']").get('value')
        append_overall_totals = model.find(xmlns + "entry[@key='total_aggregation']").get('value')
        ignore_domain = model.find(xmlns + "entry[@key='ignore_domain']").get('value')

        if max_unique_value != '10000':
            cls.knime_errorid, (
                'The maximum unique value  per group option has not been implemented. The value must be kept at 10000')

        if enable_hiliting == 'true':
            cls.knime_error(id, "Hiliting has not been enabled for pivoting node")

        if missing_values == 'false':
            cls.knime_warning(id, "Missing value are ignored for pivoting")

        if append_overall_totals == 'true':
            cls.knime_error(id, "Append overall totals has not been implemented yet for pivoting")

        if ignore_domain == 'false':
            cls.knime_error(id, "Domain is ignored for pivoting")
        xml_pivots = model.find(xmlns + "config[@key='pivotColumns']").find(
            xmlns + "config[@key='InclList']").findall(xmlns + "entry")

        for xml_pivot in xml_pivots:
            if xml_pivot.get('key') != 'array-size':
                list_pivots.append(xml_pivot.get('value'))
        xml_group_columns = model.find(xmlns + "config[@key='grouByColumns']").find(
            xmlns + "config[@key='InclList']").findall(xmlns + "entry")
        for xml_group_column in xml_group_columns:
            if xml_group_column.get('key') != 'array-size':
                list_group_columns.append(xml_group_column.get('value'))

        aggr_rename_dict = {'Mean': 'mean', 'Sum_V2.5.2': 'sum', 'First': 'first', 'Count': 'count', 'Maximum':'max'}

        xml_aggregations = model.find(xmlns + "config[@key='aggregationColumn']").find(
            xmlns + "config[@key='columnNames']").findall(xmlns + "entry")
        try:
            for xml_aggregation in xml_aggregations:
                if xml_aggregation.get('key') != 'array-size':
                    key = xml_aggregation.get('key')
                    column_name = xml_aggregation.get('value')
                    aggregation_method = model.find(xmlns + "config[@key='aggregationColumn']").find(
                        xmlns + "config[@key='aggregationMethod']").find(
                        xmlns + "entry[@key='" + key + "']").get('value')
                    if column_name in agg_dict:
                        if isinstance(agg_dict[column_name], list):
                            agg_dict[column_name] = agg_dict[column_name] + [
                                aggr_rename_dict[aggregation_method]]
                        else:
                            agg_dict[column_name] = [agg_dict[column_name],
                                                          aggr_rename_dict[aggregation_method]]
                    else:
                        agg_dict[column_name] = aggr_rename_dict[aggregation_method]
        except KeyError as e:
            cls.knime_errorid, (f'The {str(e)} method is not implemented in the Pivot node')

        self = PythonNode.init_wrapper(cls,
            agg_dict=agg_dict,
            column_name_option=column_name_option,
            column_name_policy=column_name_policy,
            list_group_columns=list_group_columns,
            list_pivots=list_pivots,
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> tuple[pd.DataFrame, None, None]:
        try:
            df = df.pivot_table(columns=self.list_pivots, index=self.list_group_columns, aggfunc=self.agg_dict, dropna=True, fill_value=None)
        except KeyError as e:
            self.error(e)
            self.error("Check your pivoting node")
            raise e


        # if len(self.list_pivots) > 2:
        #     self.warning('Renaming of Pivot table may not works properly if number of pivot is <=2")
        if self.column_name_policy == 'Keep original name(s)':
            if len(df.columns[0]) == 5:
                df.columns = [f'{k}_{l}_{m}_{n}+{i}' for i, k , l, m, n in df.columns]
            elif len(df.columns[0]) == 4:
                df.columns = [f'{k}_{l}_{m}+{i}' for i, k , l, m in df.columns]
            elif len(df.columns[0]) == 3:
                df.columns = [f'{k}_{l}+{i}' for i, k , l in df.columns]
            elif len(df.columns[0]) == 2:
                if self.column_name_option == "Pivot name":
                    df.columns = [f'{k}' for i, k in df.columns]
                else:
                    df.columns = [f'{k}+{i}' for i, k in df.columns]
        elif self.column_name_policy == 'Aggregation method (column name)':
            if len(df.columns[0]) == 4:
                if len(self.list_pivots) == 3:
                    df.columns = [f'{j}_{k}_{l}+{AGGR_RENAME_COL_DICT[next(iter(self.agg_dict.values()))]}({i})' for
                                  i, j, k, l in df.columns]
                elif len(self.list_pivots) == 2:
                    df.columns = [f'{k}_{l}+{AGGR_RENAME_COL_DICT[j]}({i})' for i, j, k, l in df.columns]
                else:
                    self.error('Pivot node with "Aggregation method (column name)" only support 3 or 2 pivots.')
                    raise Exception
            elif len(df.columns[0]) == 3:
                if len(self.list_pivots) == 2:
                    df.columns = [f'{k}_{l}+{AGGR_RENAME_COL_DICT[next(iter(self.agg_dict.values()))]}({i})' for
                                  i, k, l in df.columns]
                elif len(self.list_pivots) == 1:
                    df.columns = [f'{k}+{AGGR_RENAME_COL_DICT[j]}({i})' for i, j, k in df.columns]
                else:
                    self.error('Pivot node with "Aggregation method (column name)" only support 2 or 1 pivots.')
                    raise Exception
            elif len(df.columns[0]) == 2:
                if len(self.list_pivots) == 1:
                    df.columns = [f'{k}+{AGGR_RENAME_COL_DICT[next(iter(self.agg_dict.values()))]}({i})' for i, k in
                              df.columns]
                else:
                    self.error('Pivot node with "Aggregation method (column name)" only support 1 pivots.')
                    raise Exception
            else:
                raise Exception
        elif self.column_name_policy == 'Column name (aggregation method)':
            if len(self.list_pivots) == 6:
                self.warning("PATCH for release v2.0 in Buildings in Pivot node")
                df.columns = [f'{j}_{k}_{l}_{m}_{n}_{o}+{i} ({AGGR_RENAME_COL_DICT[self.agg_dict[i]]})' for i, j, k, l, m, n, o in df.columns]
            elif len(self.list_pivots) == 5:
                self.warning("PATCH for release v2.0 in Buildings in Pivot node")
                df.columns = [f'{j}_{k}_{l}_{m}_{n}+{i} ({AGGR_RENAME_COL_DICT[self.agg_dict[i]]})' for i, j, k, l, m, n
                 in df.columns]
            elif len(self.list_pivots) == 2:
                df.columns = [f'{k}+{i} ({AGGR_RENAME_COL_DICT[j]})' for i, j, k in df.columns]
            else:
                self.error('Pivot node with "Column name (aggregation method)" only support 1 pivots.')
                raise Exception

        df = df.reset_index(drop=False)

        self.debug('The Pivot Node is not deserving output to the port 2 and 3')

        return (df, None, None)