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
    Missing Value Filter Node
    =========================
    KNIME options implemented:
        - Manual filtering
        - Regex (case sensitive and insensitive) filtering
        - Enforce exclusion
        - Filter based on missing values
"""
from patex.nodes.node import Context, PythonNode, NativeNode, patternreshape

import pandas as pd


class MissingValueColumnFilterNode(PythonNode, NativeNode):
    knime_name = "Missing Value Column"

    def __init__(
        self,
        missing_threshold: float,
        type_of_pattern: str,
        columns_to_drop: list[str] = [],
        pattern: str | None = None,
    ):
        super().__init__()
        self.columns_to_drop = columns_to_drop
        self.missing_threshold = missing_threshold
        self.pattern = pattern
        self.type_of_pattern = type_of_pattern

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}
        columns_to_drop = []
        
        model = xml_root.find(xmlns + "config[@key='model']")
        try:
            type_of_filter = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "entry[@key='filter-type']").get('value')
        except AttributeError as e:
            cls.knime_error(id, "Filter Node is empty")
            raise Exception(e)

        # Regex type of filtering
        if type_of_filter == 'name_pattern':
            entry = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "config[@key='name_pattern']")
            if entry.find(xmlns + "entry[@key='type']").get('value') == 'Regex':
                type_of_pattern = 'Regex'
                case_sensitive = entry.find(xmlns + "entry[@key='caseSensitive']").get('value')
                pattern = entry.find(xmlns + "entry[@key='pattern']").get('value')
                kwargs["pattern"] = patternreshape(pattern, case_sensitive)
            else:
                cls.knime_error(id, "Wildcard column filtering is not implemented")

        # Manual type of filtering
        elif type_of_filter == 'STANDARD':
            type_of_pattern = 'Manual'
            enforce_option = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "entry[@key='enforce_option']").get('value')
            if enforce_option != 'EnforceExclusion':
                cls.knime_error(id, "The enforce option must be set on 'Enforce Exclusion'")
            xml_columns_to_filtered = model.find(xmlns + "config[@key='column-filter']").find(
                xmlns + "config[@key='excluded_names']")
            for xml_column_to_filtered in xml_columns_to_filtered.findall(xmlns + "entry"):
                if xml_column_to_filtered.get('key') != 'array-size':
                    columns_to_drop.append(xml_column_to_filtered.get('value'))

        # Type of filtering trough data_type
        elif type_of_filter == 'datatype':
            cls.knime_error(id, 'Column type filtering is not implemented')

        # Missing value threshold
        missing_threshold = float(model.find(xmlns + "entry[@key='missing_value_percentage']").get('value')) / 100

        self = PythonNode.init_wrapper(cls,
            columns_to_drop=columns_to_drop,
            missing_threshold=missing_threshold,
            type_of_pattern=type_of_pattern,
            **kwargs
        )
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            self.knime_input_mapping["pattern"] = self.flow_vars_params['pattern']
        return self

    def apply(self, df, pattern = None) -> pd.DataFrame:
        if self.type_of_pattern == 'Regex':
            pattern = patternreshape(pattern, True) if pattern is not None else self.pattern
            df = df.filter(regex=pattern, axis=1)
        else:
            for col in self.columns_to_drop:
                if col in df.columns:
                    df = df.drop(col, axis=1)
                #else:
                #    self.debug(
                #        f'The columns {col} does not exist and cannot be filtered')

        # Drop columns based on missing threshold
        return df.dropna(axis=1, thresh=self.missing_threshold)


if __name__ == '__main__':
    import pandas as pd

    id = '4087'
    xml_file = '/Users/climact/XCalc/dev/buildings/workflows/buildings/2_1 Building (#3780)/Missing Value Column Filter (#4087)/settings.xml'
    node_type = 'NativeNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = MissingValueColumnFilterNode(id, xml_file, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)
