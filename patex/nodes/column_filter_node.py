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
    COLUMN FILTER NODE
    ==================
    KNIME options implemented:
        Columns:
        - Manual filtering
        - Regex (case sensitive and insensitive) filtering
        - Enforce exclusion
"""

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode, patternreshape


class ColumnFilterNode(PythonNode, NativeNode):
    knime_name = "Column Filter"

    def __init__(
        self,
        pattern: str | None = None,
        columns_to_drop: list[str] | None = None,
    ):
        if (pattern is not None and columns_to_drop is not None) or (
            pattern is None and columns_to_drop is None
        ):
            raise ValueError(
                "Exactly one of `pattern` and `columns_to_drop` should be specified"
            )

        super().__init__()
        self.pattern = pattern
        self.columns_to_drop = columns_to_drop
        self.columns_copied_from_input_int = []

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        try:
            type_of_filter = model.find(xmlns + "config[@key='column-filter']").find(
            xmlns + "entry[@key='filter-type']").get('value')
        except AttributeError as e:
            cls.knime_error(id, "Filter node is empty")
            raise Exception(e)

        # Regex type of filtering
        if type_of_filter == 'name_pattern':
            entry = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "config[@key='name_pattern']")
            if entry.find(xmlns + "entry[@key='type']").get('value') == 'Regex':
                case_sensitive = entry.find(xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = entry.find(xmlns + "entry[@key='pattern']").get('value')
                try:
                    kwargs["pattern"] = patternreshape(pattern, case_sensitive)
                except ValueError:
                    cls.knime_error(id, f'Remove the bracket and the OR char in the pattern ("{pattern}")')
            else:
                cls.knime_error(id, f"Wildcard column filtering is not implemented")

        # Manual type of filtering
        elif type_of_filter == 'STANDARD':
            kwargs["columns_to_drop"] = []
            enforce_option = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "entry[@key='enforce_option']").get('value')
            if enforce_option != 'EnforceExclusion':
                cls.knime_error(id, "The enforce option must be set on 'Enforce Exclusion'")
            xml_columns_to_filtered = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "config[@key='excluded_names']")
            for xml_column_to_filtered in xml_columns_to_filtered.findall(xmlns + "entry"):
                if xml_column_to_filtered.get('key') != 'array-size':
                    kwargs["columns_to_drop"].append(xml_column_to_filtered.get('value'))

        # Type of filtering trough data_type
        elif type_of_filter == 'datatype':
            cls.knime_error(id, 'Column type filtering is not implemented')

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            self.knime_input_mapping["pattern"] = self.flow_vars_params['pattern']
        return self

    def apply(self, df, pattern = None) -> pd.DataFrame:
        try:
            pattern = patternreshape(pattern, True) if pattern is not None else self.pattern
        except ValueError:
            self.error(f'Remove the bracket and the OR char in the pattern')
        if pattern is not None:
            df_filtered = df.filter(regex=pattern, axis=1)
        else:
            df_filtered = df
            for col in self.columns_to_drop:
                if col in df_filtered.columns:
                    df_filtered = df_filtered.drop(col, axis=1)
                #else:
                #    self.debug(
                #        'The columns ' + col + ' does not exist and cannot be filtered')

        # Variables for the update
        columns_copied_from_input = df_filtered.columns.intersection(df.columns)
        self.columns_copied_from_input_int = [df.columns.get_loc(c) for c in columns_copied_from_input]

        return df_filtered

    # def update(self):
    #     df = self.in_ports[1] # No need to check for existence, we can use property directly

    #     self.out_ports[1] = df.take(self.columns_copied_from_input_int, axis=1)
