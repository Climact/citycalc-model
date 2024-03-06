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
    CALIBRATION METANODE
    ===================
    KNIME options implemented:
        - Everything
    Exceptions:
       - Returns an exception if the timestamp of the metanode is not aligned
"""
import re
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.aggregator import aggregate
from patex.nodes.node import Node


class CalibrationNodeEu(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.model = []
        self.paths = []
        self.values = []
        self.calibration_pattern = []
        self.unit_pattern = []
        self.output_table_2 = None
        self.output_table_1 = None
        self.colUnit = None
        self.columns_copied_from_input_int = []
        self.columns_copied_from_input = []
        self.local_flow_vars = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None, 2: None}

    def build_node(self):
        start = timer()
        # Check code timestamp
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The calibration node (" + self.xml_file + ") is not connected to the EUCalc - Calibration metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-07-10 13:50:48' and timestamp != "2019-07-21 20:33:04" and timestamp != "2019-08-22 17:49:40" and timestamp != "2019-09-26 14:29:19" and timestamp != "2020-05-27 13:09:30":
                self.logger.error(
                    "The template of the EUCalc - Calibration metanode was updated. Please check the version that you're using in " + self.xml_file)

        self.model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['new_name_calibration'] = ('STRING', "fts_new-column")
        self.local_flow_vars['original_pattern'] = ('STRING', "identifier")
        self.local_flow_vars['calibration_pattern'] = ('STRING', "var")
        self.local_flow_vars['start_year'] = ('STRING', "1990")
        self.local_flow_vars['end_year'] = ('STRING', "2019")
        self.local_flow_vars['disabled'] = ('SRING', "false")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-444": "new_name_calibration",
                          "string-input-445": "calibration_pattern",
                          "string-input-446": "original_pattern",
                          "string-input-1163": "start_year",
                          "string-input-1164": "end_year",
                          "disabled-1171": "disabled"}

        for child in self.model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        for child in self.model:
            path = child.get('key')
            self.paths.append(path)
            for grandchild in child:
                value = grandchild.get('value')
                self.values.append(value)

        self.calibration_pattern = re.compile("cal_rate.*")
        self.unit_pattern = re.compile(".*\\[(.*)].*")
        # logger
        t = timer() - start
        self.log_timer(t, 'BUILD', 'Calibration')

    def run(self):
        start = timer()

        self.log_timer(None, 'START', 'Calibration')

        if self.local_flow_vars['disabled'][1] == 'false':
            self.calibration_method(start)
        else:
            # Note: this works only if the "disabled" checkbox is linked to a flow variable
            if self.flow_vars[self.local_flow_vars['disabled'][1]][1] != 0 and self.flow_vars[self.local_flow_vars['disabled'][1]][1] != 'false' :
                self.calibration_bypass()
            else:
                self.calibration_method(start)

    def calibration_bypass(self):
        """

        :return:
        """

        output_table_1 = self.read_in_port(1)
        output_table_2 = output_table_1.copy()
        columns_pattern = self.local_flow_vars['original_pattern'][1] + "|Years|Country"
        output_table_2 = output_table_2.filter(regex=columns_pattern, axis=1)
        cols = [col for col in output_table_2.columns if "Years" not in col and "Country" not in col]
        output_table_2.loc[:, cols] = 1

        d = {}
        original_pattern_with_unit = re.sub(re.compile("\\[.*\\]"), "[(.*)\\]",
                                            self.local_flow_vars["original_pattern"][1])
        renaming_pattern = "cal_rate_$1"
        for col in cols:
            matcher = re.match(original_pattern_with_unit, output_table_2[col].name)
            if matcher is not None:
                new_replace_string = renaming_pattern
                if '$' in renaming_pattern:
                    for j in range(1, 3):
                        pattern_str = '$' + str(j)
                        if pattern_str in renaming_pattern:
                            new_replace_string = re.sub("\\" + pattern_str, matcher.group(j),
                                                        new_replace_string)

                d[output_table_2[col].name] = re.sub(original_pattern_with_unit, new_replace_string,
                                                 output_table_2[col].name)

        output_table_2 = output_table_2.rename(columns=d)

        #output_table_2.columns = output_table_2.columns.str.replace(cal_rate_replace, "cal_rate_")
        self.save_out_port(1, output_table_1)
        self.save_out_port(2, output_table_2)

    def calibration_method(self, start):
        columns_pattern = self.local_flow_vars['calibration_pattern'][1] + "|Years|Country"
        original_pattern_with_unit = re.sub(re.compile("\\[.*\\]"), "[(.*)\\]",
                                            self.local_flow_vars["original_pattern"][1])
        renaming_pattern = self.local_flow_vars["new_name_calibration"][1] + "_$1[$2]"
        # Join calibration table with input table
        df_cal_temp = self.read_in_port(2).filter(regex=columns_pattern, axis=1)
        df_cal = df_cal_temp.loc[(df_cal_temp.Years.astype(int) >= int(self.local_flow_vars["start_year"][1])) & (
                df_cal_temp.Years.astype(int) <= int(self.local_flow_vars["end_year"][1])), :]
        df_temp = self.read_in_port(1)
        df = df_temp[(df_temp.Years.astype(int) >= int(self.local_flow_vars["start_year"][1]))]
        df_b = df_temp[(df_temp.Years.astype(int) < int(self.local_flow_vars["start_year"][1]))]
        self.previous_years = df_b.copy()
        output_table_1 = df.merge(df_cal, how='outer', on=['Country', 'Years'])
        if len(df_b.index) != 0:
            if "\\]" not in self.local_flow_vars["original_pattern"][1]:
                self.logger.error(
                    "Use the regex pattern \[.*\] ad the end of your regex in the following Calibration node " + self.xml_file)
                if "\\[.*" in self.local_flow_vars["original_pattern"][1]:
                    self.local_flow_vars["original_pattern"][1] = self.local_flow_vars["original_pattern"][
                                                                      1] + "\\]"
                    self.logger.error(
                        "Adding \] at the end of the pattern : " + self.local_flow_vars["original_pattern"][1])

            # Rename non-changed historical values with correct name
            d = {}
            for i in range(0, len(df_b.columns.values)):
                matcher = re.match(original_pattern_with_unit, df.columns.values[i])
                if matcher is not None:
                    new_replace_string = renaming_pattern
                    if '$' in renaming_pattern:
                        for j in range(1, 3):
                            pattern_str = '$' + str(j)
                            if pattern_str in renaming_pattern:
                                new_replace_string = re.sub("\\" + pattern_str, matcher.group(j),
                                                            new_replace_string)
                                # if j == 9:
                                #    raise Exception(
                                #       'You reach the limit of groups for REGEX renaming (9) Consider changing the '
                                #      'limit in the converter. (Column Rename Regex node #' + id + ", .xml file " + str(
                                #         self.xml_file) + ")")

                    # df.columns.values[i] = replaceString
                    d[df.columns.values[i]] = re.sub(original_pattern_with_unit, new_replace_string,
                                                     df.columns.values[i])
                    if df.columns.values[i] in df.columns.values[0:i]:
                        raise Exception(
                            "A column has already been named '" + df.columns.values[
                                i] + "' (Column Rename Regex node #" + str(self.id) + ", .xml file " + str(
                                self.xml_file) + ")")

            df_b = df_b.rename(columns=d)
        # Operations to create a table with calibration rates
        output_table_1 = aggregate(output_table_1, 'unit', "Subtraction 1-2",
                                   self.local_flow_vars['calibration_pattern'][1],
                                   self.local_flow_vars['original_pattern'][1], 'cal_delta', 0, 'PAROPP')
        output_table_1 = aggregate(output_table_1, '%', "Division 1/2", 'cal_delta_(.*)\[.*',
                                   self.local_flow_vars['original_pattern'][1], 'cal_rate', 0, 'CAL')
        # Add 1 to calibration rate columns
        calibration_columns = [col for col in output_table_1 if re.match(self.calibration_pattern, col)]
        output_table_1[calibration_columns] += 1


        self.calibration_rates_saved=output_table_1.filter(regex="cal_rate.*|Country|Years", axis=1).copy()


        # sort by country and Years before completing missing values
        output_table_1.sort_values(by=['Country', 'Years'], inplace=True)
        # completing missing values
        output_table_1.fillna(method="ffill", inplace=True)
        # remove unwanted columns
        output_table_1.drop(
            output_table_1.filter(regex=self.local_flow_vars['calibration_pattern'][1], axis=1).columns,
            axis=1,
            inplace=True)
        output_table_1.drop(output_table_1.filter(regex='cal_delta.*', axis=1).columns, axis=1, inplace=True)
        # save calibration rate in second output_table
        output_table_2 = output_table_1.filter(regex="cal_rate.*|Country|Years", axis=1)
        # save dataframes for the update()
        # self.output_table_1 = output_table_1.copy()
        # self.output_table_2 = output_table_2.copy()
        # Variables for the update
        self.columns_copied_from_input = output_table_1.columns.intersection(self.read_in_port(1).columns)
        self.columns_copied_from_input_int = [self.read_in_port(1).columns.get_loc(c) for c in
                                              self.columns_copied_from_input]
        # multiplication of cal_rate by original values
        original_pattern = self.local_flow_vars['original_pattern'][1]
        original_columns = [col for col in output_table_1 if re.match(original_pattern, col)]
        # matcher_unit = re.match(self.unit_pattern, original_columns[0])
        # self.colUnit = matcher_unit.group(1)
        output_table_1 = aggregate(output_table_1, 'unit', "Product", 'cal_rate_(.*)\[%\]',
                                   self.local_flow_vars['original_pattern'][1],
                                   self.local_flow_vars['new_name_calibration'][1], 1, 'PAROPP',
                                   calibration_bool=True)
        # logger
        t = timer() - start
        self.log_timer(t, 'END', 'Calibration')
        if len(df_b.index) == 0:
            self.save_out_port(1, output_table_1)
        else:
            if self.id == '1245':
                self.logger.warning(
                    "PATCH in transport to avoid error in first output of Calibration 1245 outpout (multiple names with same value)")
                self.save_out_port(1, output_table_1)
            else:
                self.save_out_port(1, pd.concat([output_table_1, df_b], join='outer', sort=False))
        self.save_out_port(2, output_table_2)

    # def update(self):
    #     start = timer()
    #     self.log_timer(None, 'UPDATE', "Calibration")
    #
    #     if self.local_flow_vars['disabled'][1] == 'false':
    #         self.calibration_method_update(start)
    #     else:
    #         # Note: this works only if the "disabled" checkbox is linked to a flow variable
    #         if self.flow_vars[self.local_flow_vars['disabled'][1]][1] != 0 and self.flow_vars[self.local_flow_vars['disabled'][1]][1] != 'false' :
    #             self.calibration_bypass()
    #         else:
    #             self.calibration_method_update(start)
    #
    # def calibration_method_update(self, start):
    #     df = self.in_ports[1] # No need to check for existence, we can use property directly
    #     output_table_1 = df.take(self.columns_copied_from_input_int, axis=1)
    #     output_table_2 = self.calibration_rates_saved
    #     output_table_1 = pd.concat([output_table_1, output_table_2.filter(regex="cal_rate.*")], axis=1)
    #
    #     # sort by country and Years before completing missing values
    #     output_table_1.sort_values(by=['Country', 'Years'], inplace=True)
    #     output_table_2.sort_values(by=['Country', 'Years'], inplace=True)
    #     # completing missing values
    #     output_table_1.fillna(method="ffill", inplace=True)
    #     # remove unwanted columns
    #     output_table_1.drop(
    #         output_table_1.filter(regex=self.local_flow_vars['calibration_pattern'][1], axis=1).columns,
    #         axis=1,
    #         inplace=True)
    #
    #
    #     output_table_1 = aggregate(output_table_1, 'unit', "Product", 'cal_rate_(.*)\[%\]',
    #                                self.local_flow_vars['original_pattern'][1],
    #                                self.local_flow_vars['new_name_calibration'][1], 1, 'PAROPP',
    #                                calibration_bool=True)
    #     # logger
    #     t = timer() - start
    #     self.log_timer(t, 'END', 'Calibration')
    #     if len(self.previous_years.index) == 0:
    #         self.out_ports[1] = output_table_1
    #     else:
    #         if self.id == '1245':
    #             self.logger.warning(
    #                 "PATCH in transport to avoid error in first output of Calibration 1245 outpout (multiple names with same value)")
    #             self.out_ports[1] = output_table_1
    #         else:
    #             self.out_ports[1]= pd.concat([output_table_1, self.previous_years], join='outer', sort=False)
    #
    #     self.out_ports[2] = output_table_2
