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
    MATH FORMULA MULTI COLUMN NODE
    ============================
    KNIME options NOT implemented:
        - Selection of column by wildcards
        - Use of row index and row count
        - All special mathematical function except for COL_MIN, COL_MAX, COL_MEAN, COL_MEDIAN, pi, e ,COL_SUM, COL_STDDEV, COL_VAR, ln
"""

import re
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from patex.nodes.node import Node


class MathFormulaMultiColumnNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.id_xml = str(id) + " " + xml_file
        self.append_column = None
        self.suffix_column = None
        self.expression = None
        self.convert_to_int = None
        self.type_of_filter = None
        self.filter_regex = None
        self.selected_columns = []
        self.case_sensitive = None
        self.pattern = None
        self.specific_expressions = []
        self.fun_dic = {}
        self.raise_error_list = []
        self.col_rename_dico = {}
        self.exp_rename_dico = {}
        self.flow_var_dico = {}
        self.col_id = 0
        self.splitted = None
        self.known_expression = None
        self.name_pattern_and_regex = None
        self.selected_columns_int = []
        self.df_hist = pd.DataFrame()
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        self.append_column = model.find(self.xmlns + "entry[@key='append_column']").get('value')
        self.suffix_column = model.find(self.xmlns + "entry[@key='replaced_column']").get('value')
        self.expression = model.find(self.xmlns + "entry[@key='expression']").get('value')
        self.convert_to_int = model.find(self.xmlns + "entry[@key='convert_to_int']").get('value')
        self.expression = self.expression.replace('%%00010', '')

        self.type_of_filter = model.find(self.xmlns + "config[@key='column_selection']").find(
            self.xmlns + "entry[@key='filter-type']").get('value')
        entry = model.find(self.xmlns + "config[@key='column_selection']").find(
            self.xmlns + "config[@key='name_pattern']")
        self.filter_regex = entry.find(self.xmlns + "entry[@key='type']").get('value')

        # ------------------------------------------------------
        # Columns filtering
        # ------------------------------------------------------
        # Regex type of filtering
        if self.type_of_filter == 'name_pattern':
            if self.filter_regex == 'Regex':
                self.case_sensitive = entry.find(self.xmlns + "entry[@key='caseSensitive']").get('value')
                self.pattern = entry.find(self.xmlns + "entry[@key='pattern']").get('value')
                self.pattern_reshape = self.patternreshape(self.pattern, self.case_sensitive)
            else:
                raise Exception(
                    "Wildcard column selection is not implemented (math formula multi column node #" + self.id_xml + ")")

        # Manual type of filtering
        elif self.type_of_filter == 'STANDARD':
            xml_columns_to_keep = model.find(self.xmlns + "config[@key='column_selection']").find(
                self.xmlns + "config[@key='included_names']")
            for xml_column_to_keep in xml_columns_to_keep.findall(self.xmlns + "entry"):
                if xml_column_to_keep.get('key') != 'array-size':
                    self.selected_columns.append(xml_column_to_keep.get('value'))

        # ------------------------------------------------------
        # Preparing the evaluation of the expression by renaming columns and validate math operations
        # ------------------------------------------------------
        self.specific_expressions = ["$$CURRENT_COLUMN$$", # To replace NaN by ?
                                     "if($$CURRENT_COLUMN$$<0,0,$$CURRENT_COLUMN$$)",
                                     "if($$CURRENT_COLUMN$$>0,$$CURRENT_COLUMN$$,0)",
                                     "1-if($$CURRENT_COLUMN$$>1,1,$$CURRENT_COLUMN$$)",
                                     "if(isNaN($$CURRENT_COLUMN$$), 0, if(isInfinite($$CURRENT_COLUMN$$),0,1-$$CURRENT_COLUMN$$))",
                                     "1-$$CURRENT_COLUMN$$", "$$CURRENT_COLUMN$$", "$$CURRENT_COLUMN$$*1000",
                                     "$$CURRENT_COLUMN$$*1e-12", "$$CURRENT_COLUMN$$*1e6/3600",
                                     "$$CURRENT_COLUMN$$*1e9", "$$CURRENT_COLUMN$$*1e-9", "$$CURRENT_COLUMN$$/1000",
                                     "if($$CURRENT_COLUMN$$>1,1,$$CURRENT_COLUMN$$)", "1-(1-$$CURRENT_COLUMN$$)^5",
                                     "(1-$$CURRENT_COLUMN$$)^5", "$$CURRENT_COLUMN$$/5", "$$CURRENT_COLUMN$$*1e3",
                                     "$$CURRENT_COLUMN$$*$cal_rate_road[%]$",
                                     "$$CURRENT_COLUMN$$*$lfs_pop_population[inhabitants]$*365",
                                     "$$CURRENT_COLUMN$$*365*$lfs_pop_population[inhabitants]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_emissions_total[%]$",
                                     "if(isNaN($$CURRENT_COLUMN$$)==1,0,if(isInfinite($$CURRENT_COLUMN$$)==1,100,$$CURRENT_COLUMN$$))",
                                     "$$CURRENT_COLUMN$$ * 8.766",
                                     "if(isNaN($$CURRENT_COLUMN$$)==1,0,$$CURRENT_COLUMN$$)",
                                     "0",
                                     "$$CURRENT_COLUMN$$*$cal_rate_CO2[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_CH4[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_N2O[%]$",
                                     "$$CURRENT_COLUMN$$*$tra_fuel-mix_efuel[%]$",
                                     "$$CURRENT_COLUMN$$/3.6e9",
                                     "$$CURRENT_COLUMN$$*$cal_rate_diesel[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_gasoline[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_biodiesel[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_bioethanol[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_biogas[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_jetfuel[%]$",
                                     "$$CURRENT_COLUMN$$*$cal_rate_electricity[%]$",
                                     "$CURRENT_COLUMN$$*$tec_emission-factor_CH4_diesel[Mt/TWh]$",
                                     "$$CURRENT_COLUMN$$*$tec_emission-factor_N2O_gas[Mt/TWh]$",
                                     "196",
                                     "$$CURRENT_COLUMN$$*$tra_fuel-mix_biofuel-road[%]$",
                                     "abs($$CURRENT_COLUMN$$-1)",
                                     "exp((ln(1-$$CURRENT_COLUMN$$)))",
                                     "exp((ln($$CURRENT_COLUMN$$-1)))",
                                     "ceil(sin(-1*$$CURRENT_COLUMN$$*pi/1800000))",
                                     "if($$CURRENT_COLUMN$$>=0,0,-$$CURRENT_COLUMN$$)",
                                     "if($$CURRENT_COLUMN$$<=0,0,$$CURRENT_COLUMN$$)",
                                     "if($elc_link-costs-to-activity[-]$==1,$$CURRENT_COLUMN$$,$tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]$)",
                                     "if($elc_link-costs-to-activity_efuels[-]$==1,$$CURRENT_COLUMN$$,$tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]$)",
                                     "if($$CURRENT_COLUMN$$ > 0, 0, abs($$CURRENT_COLUMN$$))",
                                     "if($$CURRENT_COLUMN$$ < 0, 0, $$CURRENT_COLUMN$$)",
                                     "if($$CURRENT_COLUMN$$==101,1,$$CURRENT_COLUMN$$)",
                                     "if(isInfinite($$CURRENT_COLUMN$$), 0, $$CURRENT_COLUMN$$)",
                                     "if(isNaN($$CURRENT_COLUMN$$), 0, $$CURRENT_COLUMN$$)"
                                     ]

        self.raise_error_list = ['ROW_COUNT', 'ROW_INDEX', 'log', 'log1p', 'exp', 'pow', 'abs', 'sqrt', 'rand', 'mod',
                                 'if',
                                 'round', 'round', 'roundHalfUp', 'ceil', 'floor', 'binom', 'sin', 'cos', 'tan', 'asin',
                                 'acos', 'atan', 'atan', 'atan2', 'sinh', 'cosh', 'max_in_args', 'min_in_args',
                                 'argmin',
                                 'armax', 'colMin', 'colMax', 'average', 'median', 'signum', 'between', 'isNaN',
                                 'isInfinite', '||', '&&', '==', '!=', '>', '<', '>=', '<=']

        self.fun_dic = dict(
            {'COL_MIN': np.min, 'COL_MAX': np.max, 'COL_MEAN': np.mean, 'COL_MEDIAN': np.median, 'pi': np.pi
                , 'COL_SUM': np.sum, 'COL_STDDEV': np.std, 'COL_VAR': np.var, 'ln': np.log})

        replace_dic = dict({'^': "**"})

        self.splitted = self.expression.split('$')

        for i, op in enumerate(self.splitted):
            if op in ['ROWINDEX', 'ROWCOUNT']:
                raise Exception(
                    "ROWINDEX and ROWCOUNT are not handled (math formula multicolumn node #" + self.id_xml + ")")

        if self.expression in self.specific_expressions:
            self.unknown_expression = False
        else:
            self.unknown_expression = True
            for i, op in enumerate(self.splitted):
                for unsafe in self.raise_error_list:
                    if unsafe in op:
                        if "habitant" not in op and "technology" not in op:
                            self.logger.error("Unsafe operator (" + unsafe + ") in the expression: " + self.expression)
                for key in replace_dic:
                    if key in op:
                        self.splitted[i] = op.replace(key, replace_dic[key])

        # ------------------------------------------------------
        # Columns filtering
        # ------------------------------------------------------
        # Regex type of filtering
        if self.type_of_filter == 'name_pattern':
            if self.filter_regex == 'Regex':
                # a = pattern.split('.')  # Pattern in xml : 'pattern.*'
                # this_str = '^'  # For the regex to only look at the begining of the word
                self.name_pattern_and_regex = True
                # This commented bloc was replaced by the unique line above
                # if case_sensitive == 'true':
                #     self.selected_columns = df.filter(regex=pattern).columns
                # else:
                #     self.selected_columns = df.filter(regex=re.compile(pattern, re.IGNORECASE)).columns

        if '[' in self.suffix_column or '-' in self.suffix_column:
            self.logger.warning(
                "Avoid using '[' or '-' in the new colum name ('" + self.suffix_column + "') of the new column in the Math Formula Multi Column #" + self.id + ". This may cause errors in the converter (" + self.xml_file + ")")

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)
        df = self.math_multi_columns(df)

        # if 'Years' in df.columns:
        #     self.df_hist = self.historical_table(df)

        self.save_out_port(1, df)
        # logger
        t = timer() - start
        self.log_timer(t, "END")

    # def update(self):
    #     start = timer()
    #     self.log_timer(None, 'START')
    #
    #     df_proj = self.projections_table(self.read_in_port(1))
    #     df_proj = self.math_multi_columns(df_proj)
    #
    #     if self.df_hist.empty:
    #         self.save_out_port(1,df_proj)
    #     else:
    #         self.save_out_port(1, self.merge_hist_proj_table(self.df_hist, df_proj))
    #     # logger
    #     t = timer() - start
    #     self.log_timer(t, "END")

    def math_multi_columns(self, df):
        # ------------------------------------------------------
        # Columns filtering
        # ------------------------------------------------------
        if self.name_pattern_and_regex:
            self.selected_columns = df.filter(regex=self.pattern_reshape).columns
        # ------------------------------------------------------
        # Preparing the evaluation of the expression by renaming columns and validate math operations
        # ------------------------------------------------------
        if self.unknown_expression:
            for i, op in enumerate(self.splitted):
                if op in df.columns:
                    col_name = 'col' + str(self.col_id)
                    self.col_rename_dico[op] = col_name
                    self.exp_rename_dico[col_name] = op
                    self.col_id += 1
                elif op.startswith('{'):
                    self.splitted[i] = self.splitted[i].replace('{', '')
                    self.splitted[i] = self.splitted[i].replace('}', '')
                    self.splitted[i] = '@' + self.splitted[i][
                                             1:]  # variables should be mentionned by @ in the expression
                    self.flow_var_dico[self.splitted[i][1:]] = float(
                        self.flow_vars[self.splitted[i][1:]][1])  # TODO : possible to move in build ?
                else:
                    for unsafe in self.raise_error_list:
                        if unsafe in op:
                            self.logger.error("Unsafe operator (" + unsafe + ") in the expression: " + self.expression)
                            raise Exception(
                                "The operation " + unsafe + " has not been implemented yet (Math formula multi-column node #" + self.id_xml + ")")
                    for safe in self.fun_dic:
                        if safe in op:
                            self.splitted[i] = self.splitted[i].replace(safe,
                                                                        "@" + safe)  # variables should be mentionned by @ in the expression

            splitted = [self.col_rename_dico.get(item, item) for item in
                        self.splitted]  # rename the column name of the splitted expression with the new col names
            new_join_expression = ''.join(splitted)

            # renaming the columns of the dataframe to avoid errors in the eval() function (see below)
            df = df.rename(columns=self.col_rename_dico)

            # renaming the expression to specify which column to replace (or not)
            if self.append_column == 'false':
                new_col_name = "CURRENT_COLUMN" + self.suffix_column
                new_join_expression = new_col_name + "=" + new_join_expression
            else:
                new_col_name = "CURRENT_COLUMN"
                new_join_expression = new_col_name + "=" + new_join_expression

            # ------------------------------------------------------
            # Evaluation of the expression using pandas.DataFrame.eval() function
            # ------------------------------------------------------
            # NOTE: pandas.DataFrame.eval() doesn't allow to access numeric column names or columns names with brackets
            # (e.g. '2011', 'col_var_[unit]'). This is the reason why we are renaming all the columns before the evaluation
            rx = re.compile('^col[0-9]+$')
            columns_order = list(df.columns)
            col_used = [col for col in df.columns if (col in self.selected_columns) or (rx.match(col))]
            col_unused = [col for col in df.columns if col not in col_used]
            df_unused = df[col_unused]
            df = df[col_used]
            df_store = pd.DataFrame()
            for value, col in enumerate(self.selected_columns):
                if col in df.columns:
                    try:
                        if df[col].dtypes == 'object':
                            df[col] = pd.to_numeric(df[col])
                            self.logger.debug(
                                'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                        df.rename(columns={col: "CURRENT_COLUMN"}, inplace=True)
                        df.eval(new_join_expression, inplace=True, local_dict={**self.fun_dic, **self.flow_var_dico})
                        if self.convert_to_int == 'true':
                            df[new_col_name] = df[new_col_name].round(0)
                        df = df.rename(
                            columns={"CURRENT_COLUMN": col,
                                     "CURRENT_COLUMN" + self.suffix_column: col + self.suffix_column})
                        if self.append_column == 'false':
                            new_column_name = col + self.suffix_column
                            columns_order.append(new_column_name)
                        else:
                            new_column_name = col
                        df_store = pd.concat([df_store, df[new_column_name]], axis=1)
                        df = df[col_used]
                    except Exception:
                        self.logger.error(
                            'Error converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                elif col in self.col_rename_dico:
                    if df[self.col_rename_dico[col]].dtypes == 'object':
                        df[self.col_rename_dico[col]] = pd.to_numeric(df[self.col_rename_dico[col]])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    new_expression = new_join_expression.replace("CURRENT_COLUMN", self.col_rename_dico[col])
                    new_col_name_it = new_col_name.replace("CURRENT_COLUMN", self.col_rename_dico[col])
                    df.eval(new_expression, inplace=True, local_dict={**self.fun_dic, **self.flow_var_dico})
                    if self.append_column == 'false':
                        self.exp_rename_dico[self.col_rename_dico[col] + self.suffix_column] = col + self.suffix_column
                        columns_order.append(new_col_name_it)
                    if self.convert_to_int == 'true':
                        df[new_col_name_it] = df[new_col_name_it].round(0)
                    df_store = pd.concat([df_store, df[new_col_name_it]], axis=1)
                    df = df[col_used]
                else:
                    raise Exception("Column " + col + " not in DataFrame (Math MultiColumn node #" + self.id_xml + ")")
            df = pd.concat([df, df_unused], axis=1)
            df = pd.concat([df, df_store], axis=1)
            df = df.loc[:, ~df.columns.duplicated(keep='last')]
            df = df[columns_order]

            df = df.rename(columns=self.exp_rename_dico)

        else:
            cols = self.selected_columns
            # cols_in_df = df.columns.intersection(self.selected_columns)
            # self.selected_columns_int = [df.columns.get_loc(c) for c in cols_in_df]
            # renaming the expression to specify which column to replace (or not)
            if self.append_column == 'false':
                cols = [col + self.suffix_column for col in cols]
                df[cols] = df[self.selected_columns]

            if self.expression == "$$CURRENT_COLUMN$$":
                for col in cols:
                    if df[col].dtypes == 'object':
                        mask = (df[col].isna())
                        df.loc[mask, col] = np.nan()
                        self.logger.debug(
                            'Converting NaN for ' + col + ' to ? (' + self.id_xml + ')')
            elif self.expression == "if($$CURRENT_COLUMN$$<0,0,$$CURRENT_COLUMN$$)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    df.loc[df[col] < 0, col] = 0
            elif self.expression == "if($$CURRENT_COLUMN$$>0,$$CURRENT_COLUMN$$,0)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                        df.loc[df[col] < 0, col] = 0
            elif self.expression == "if($$CURRENT_COLUMN$$>1,1,$$CURRENT_COLUMN$$)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    df.loc[df[col] > 1, col] = 1
            elif self.expression == "1-if($$CURRENT_COLUMN$$>1,1,$$CURRENT_COLUMN$$)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    mask1 = df[col] >= 1
                    mask2 = df[col] < 1
                    df.loc[mask2, col] = 1 - df.loc[mask2, col]
                    df.loc[mask1, col] = 0
            elif self.expression == "if(isNaN($$CURRENT_COLUMN$$), 0, if(isInfinite($$CURRENT_COLUMN$$),0,1-$$CURRENT_COLUMN$$))":
                df.loc[:, cols] = 1 - df.loc[:, cols]
                df.loc[:, cols] = df.loc[:, cols].fillna(0)
            elif self.expression == "1-$$CURRENT_COLUMN$$":
                df.loc[:, cols] = 1 - df.loc[:, cols]
            elif self.expression == "$$CURRENT_COLUMN$$":
                pass
            elif self.expression == "$$CURRENT_COLUMN$$*1000":
                df.loc[:, cols] = df.loc[:, cols] * 1000
            elif self.expression == "$$CURRENT_COLUMN$$*1e-12":
                df.loc[:, cols] = df.loc[:, cols] * 1e-12
            elif self.expression == "$$CURRENT_COLUMN$$*1e6/3600":
                df.loc[:, cols] = df.loc[:, cols] * 1e6 / 3600
            elif self.expression == "$$CURRENT_COLUMN$$*1e9":
                df.loc[:, cols] = df.loc[:, cols] * 1e9
            elif self.expression == "$$CURRENT_COLUMN$$*1e-9":
                df.loc[:, cols] = df.loc[:, cols] * 1e-9
            elif self.expression == "$$CURRENT_COLUMN$$/1000":
                df.loc[:, cols] = df.loc[:, cols] / 1000
            elif self.expression == "1-(1-$$CURRENT_COLUMN$$)^5":
                df.loc[:, cols] = 1 - (1 - df.loc[:, cols]) ** 5
            elif self.expression == "(1 -$$CURRENT_COLUMN$$) ^ 5" or self.expression == "(1-$$CURRENT_COLUMN$$)^5":
                df.loc[:, cols] = (1 - df.loc[:, cols]) ** 5
            elif self.expression == "$$CURRENT_COLUMN$$/5":
                df.loc[:, cols] = df.loc[:, cols] / 5
            elif self.expression == "$$CURRENT_COLUMN$$*1e3":
                df[cols] = df[cols] * 1e3
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_road[%]$":
                df[cols] = df[cols].mul(df["cal_rate_road[%]"], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*365*$lfs_pop_population[inhabitants]$" or self.expression == "$$CURRENT_COLUMN$$*$lfs_pop_population[inhabitants]$*365":
                df[cols] = df[cols].mul(df["lfs_pop_population[inhabitants]"], axis="index") * 365
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_emissions_total[%]$":
                df[cols] = df[cols].mul(df["cal_rate_emissions_total[%]"], axis="index")
            elif self.expression == "if(isNaN($$CURRENT_COLUMN$$)==1,0,if(isInfinite($$CURRENT_COLUMN$$)==1,100,$$CURRENT_COLUMN$$))":
                df[cols] = df[cols].fillna(0)
                df[cols] = df[cols].replace(np.inf, 100)
            elif self.expression == "$$CURRENT_COLUMN$$ * 8.766":
                df[cols] = df[cols] * 8.766
            elif self.expression == "if(isNaN($$CURRENT_COLUMN$$)==1,0,$$CURRENT_COLUMN$$)":
                df[cols] = df[cols].fillna(0)
            elif self.expression == "0":
                df[cols] = 0
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_CO2[%]$":
                df[cols] = df[cols].mul(df['cal_rate_CO2[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_CH4[%]$":
                df[cols] = df[cols].mul(df['cal_rate_CH4[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_N2O[%]$":
                df[cols] = df[cols].mul(df['cal_rate_N2O[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$tra_fuel-mix_efuel[%]$":
                df[cols] = df[cols].mul(df['tra_fuel-mix_efuel[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$/3.6e9":
                df[cols] = df[cols] / 3.6e9
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_diesel[%]$":
                df[cols] = df[cols].mul(df['cal_rate_diesel[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_gasoline[%]$":
                df[cols] = df[cols].mul(df['cal_rate_gasoline[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_biodiesel[%]$":
                df[cols] = df[cols].mul(df['cal_rate_biodiesel[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_bioethanol[%]$":
                df[cols] = df[cols].mul(df['cal_rate_bioethanol[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_biogas[%]$":
                df[cols] = df[cols].mul(df['cal_rate_biogas[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_jetfuel[%]$":
                df[cols] = df[cols].mul(df['cal_rate_jetfuel[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$cal_rate_electricity[%]$":
                df[cols] = df[cols].mul(df['cal_rate_electricity[%]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$tec_emission-factor_CH4_diesel[Mt/TWh]$":
                df[cols] = df[cols].mul(df['tec_emission-factor_CH4_diesel[Mt/TWh]'], axis="index")
            elif self.expression == "$$CURRENT_COLUMN$$*$tec_emission-factor_N2O_gas[Mt/TWh]$":
                df[cols] = df[cols].mul(df['tec_emission-factor_N2O_gas[Mt/TWh]'], axis="index")
            elif self.expression == "196":
                df[cols] = 196
            elif self.expression == "$$CURRENT_COLUMN$$*$tra_fuel-mix_biofuel-road[%]$":
                df[cols] = df[cols].mul(df['tra_fuel-mix_biofuel-road[%]'], axis="index")
            elif self.expression == "abs($$CURRENT_COLUMN$$-1)":
                df[cols] = df[cols] - 1
                df[cols] = df[cols].abs()
            elif self.expression == "exp((ln(1-$$CURRENT_COLUMN$$)))":
                df[cols] = np.exp(np.log(1 - df[cols]))
            elif self.expression == "exp((ln($$CURRENT_COLUMN$$-1)))":
                df[cols] = np.exp(np.log(df[cols] - 1))
            elif self.expression == "ceil(sin(-1*$$CURRENT_COLUMN$$*pi/1800000))":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    df.loc[:, col] = -1 * df.loc[:, col] * np.pi / 1800000
                    df[col] = np.sin(df[col])
                    df[col] = df[col].apply(np.ceil)
            elif self.expression == "if($$CURRENT_COLUMN$$>=0,0,-$$CURRENT_COLUMN$$)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[col])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    df.loc[df[col] >= 0, col] = 0
                    df.loc[df[col] < 0, col] = -df.loc[df[col] < 0, col]
            elif self.expression == "if($$CURRENT_COLUMN$$<=0,0,$$CURRENT_COLUMN$$)":
                for col in cols:
                    if df[col].dtypes == 'object':
                        df[col] = pd.to_numeric(df[prcol])
                        self.logger.debug(
                            'Converting String column ' + col + ' into Numerical column (' + self.id_xml + ')')
                    df.loc[df[col] <= 0, col] = 0
            elif self.expression == "if($elc_link-costs-to-activity[-]$==1,$$CURRENT_COLUMN$$,$tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]$)":
                mask = (df['elc_link-costs-to-activity[-]'] == 1)
                # df["elc_link-costs-to-activity[-]"] = pd.to_numeric(df["elc_link-costs-to-activity[-]"])
                # self.logger.debug('Converting String column elc_link-costs-to-activity[-] into Numerical column (' + self.id_xml + ')')
                for col in cols:
                    df.loc[~mask, col] = df.loc[~mask, "tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]"]
            elif self.expression == "if($elc_link-costs-to-activity_efuels[-]$==1,$$CURRENT_COLUMN$$,$tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]$)":
                mask = (df['elc_link-costs-to-activity_efuels[-]'] == 1)
                for col in cols:
                    df.loc[~mask, col] = df.loc[~mask, "tec_exogenous-energy-costs_ef_synfuel-tbd[EUR/MWh]"]
            elif self.expression == "if($$CURRENT_COLUMN$$ > 0, 0, abs($$CURRENT_COLUMN$$))":
                for col in cols:
                    mask = (df[col] > 0)
                    df.loc[mask, col] = 0
                    df.loc[~mask, col] = abs(df.loc[~mask, col])
            elif self.expression == "if($$CURRENT_COLUMN$$ < 0, 0, $$CURRENT_COLUMN$$)":
                for col in cols:
                    mask = (df[col] < 0)
                    df.loc[mask, col] = 0
            elif self.expression == "if(isNaN($$CURRENT_COLUMN$$), 0, $$CURRENT_COLUMN$$)":
                df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
            elif self.expression == "if(isInfinite($$CURRENT_COLUMN$$), 0, $$CURRENT_COLUMN$$)":
                df[cols] = df[cols].fillna(0)
            elif self.expression == "if($$CURRENT_COLUMN$$==101,1,$$CURRENT_COLUMN$$)":
                for col in cols:
                    mask = (df[col] == 101)
                    df.loc[mask, col] = 1

            # else:
            # raise Exception("Expression (" + self.expression + ") not implemented (Math MultiColumn node #" + self.id_xml + ")")
        return df


if __name__ == '__main__':
    import pyinstrument
    id = '3840'
    relative_path = f'/Users/climact/XCalc/dev/_interactions/workflows_test_modules/test_IND/3_1a Industr (#0)/Math Formula _Multi Column_ (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = MathFormulaMultiColumnNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.build_node()
    profiler = pyinstrument.Profiler()
    profiler.start()
    node.run()
    profiler.stop()
    print(profiler.output_text(unicode=True, color=True))
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)
