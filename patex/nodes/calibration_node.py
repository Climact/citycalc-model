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
    This node will calibrate your data on official once.

    KNIME options implemented:
        - All
"""

import re
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode


class CalibrationNode(PythonNode, SubNode):
    def __init__(
        self,
        data_to_be_cal,
        data_cal,
        col_to_be_cal="transport-demand[pkm]",
        col_cal="cal_activity[pkm]",
        apply_calib=True,
    ):
        super().__init__()
        self.data_to_be_cal = data_to_be_cal
        self.data_cal = data_cal
        self.col_to_be_cal = col_to_be_cal
        self.col_cal = col_cal
        self.apply_calib = apply_calib

        # Those are populated during the first run, and reused at update
        self.dimensions_1 = []
        self.dimensions_2 = []
        self.common_dimensions = []
        self.common_dimensions_left = []
        self.common_dimensions_excl_years = []

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None, 2: None, 3: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        kwargs = {}

        model = xml_root.find(xmlns + "config[@key='model']")
        # Search values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'data-to-be-cal-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs["data_to_be_cal"] = value
                if 'data-cal-' in child.get('key'):
                    value = grandchild.get('value')
                    kwargs["data_cal"] = value
                if 'apply-calib-' in child.get('key'):
                    for ggrandchild in grandchild:
                        if ggrandchild.get('key') == "0":
                            value = ggrandchild.get('value')
                            kwargs["apply_calib"] = value == "Yes"

        self = PythonNode.init_wrapper(cls, **kwargs)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        if self.uses_flow_vars:
            self.knime_input_mapping["apply_calib"] = self.flow_vars_params['variable_value']
        return self

    def apply(self, input_table, cal_table, apply_calib = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # Rename calibrated column
        cal_table = cal_table.rename(columns={element: re.sub(r'(.*\[.*)', r'\1_cal', element) for element in cal_table.columns.tolist()})

        # Change value for year parameter dynamically if linked to a flow variable
        apply_calib = True if apply_calib == "Yes" else self.apply_calib

        # Parameters
        col_calibrated = "calibrated_value"
        col_cal = self.data_cal + "_cal"
        col_to_be_cal = self.data_to_be_cal
        col_cal_rate = "cal_rate_" + col_to_be_cal

        # Get common dimensions (with and without years)
        self.dimensions_1 = input_table.select_dtypes(['int32', 'int64', 'int', 'object']).columns
        dimensions_1 = input_table[self.dimensions_1]
        self.dimensions_2 = cal_table.select_dtypes(['int32', 'int64', 'int', 'object']).columns
        dimensions_2 = cal_table[self.dimensions_2]
        self.common_dimensions = []
        self.common_dimensions_left = list(input_table.select_dtypes(['int32', 'int64', 'int', 'object']).columns)
        self.common_dimensions_excl_years = []
        for col in dimensions_1:
            if col in dimensions_2:
                self.common_dimensions.append(col)
                if col != 'Years':
                    self.common_dimensions_excl_years.append(col)

        # Add calibration data to dataframe to calibrate
        output_table = input_table.merge(cal_table, how='left', on=self.common_dimensions)

        # Get max available year to calibrate (by common dimensions (except years)) from calibration table
        max_years_table = output_table.copy()
        max_years_table = max_years_table[~max_years_table[[col_cal, col_to_be_cal]].isna().any(axis=1)]
        max_years_table = max_years_table.groupby(self.common_dimensions_excl_years, as_index=False)["Years"].max()
        max_years_table.rename(columns={'Years': 'max_year'}, inplace=True)

        # Add max year available to calibrate the dataframe
        output_table = output_table.merge(max_years_table, on=self.common_dimensions_excl_years, how="left")

        # Add flag for data without calibration data in max year (missing_cal = 1)
        output_table["missing_cal"] = 0
        mask_na_missing = (output_table["max_year"].isna())
        output_table.loc[mask_na_missing, "missing_cal"] = 1

        # ---------------------------------------------------------- #
        # Split flow based on missing mask
        # Nothing is applied to missing
        # For non missing, apply calibration
        mask_missing = (output_table["missing_cal"] == 1)
        df_missing = output_table.loc[mask_missing].copy()
        df_not_missing = output_table.loc[~mask_missing].copy()

        # Assume that cal rate is 1 for missing values
        df_missing[col_cal_rate] = 1.0
        df_missing["flag"] = "issue (missing official)"

        # ---------------------------------------------------------- #
        # Proceed to calibration if calibration data available
        if not df_not_missing.empty:
            # ---------------------------------------------------------- #
            #  Split sub flow based on years (<= max year and > max year)
            #  Compute cal rates and apply them if required by user
            mask_year = (df_not_missing["Years"] <= df_not_missing["max_year"])
            df_lower = df_not_missing.loc[mask_year].copy()
            df_upper = df_not_missing.loc[~mask_year].copy()

            #   <= max year : new value is the calibration value (unless calibration value != 0 AND calibrated value = 0)
            #                 compute cal rate = calibration data / data to calibrate
            df_lower[col_calibrated] = df_lower[col_cal]
            mask_new_val_except_1 = (df_lower[col_cal] != 0) & (df_lower[col_to_be_cal] == 0)
            df_lower.loc[mask_new_val_except_1, col_calibrated] = df_lower.loc[mask_new_val_except_1, col_to_be_cal]
            df_lower[col_cal_rate] = df_lower[col_cal] / df_lower[col_to_be_cal]
            # Add related flags
            df_lower.loc[mask_new_val_except_1, "flag"] = "issue (model at 0 and non zero official)"
            mask_new_val_except_2 = (df_lower[col_cal] == 0) & (df_lower[col_to_be_cal] == 0)
            df_lower.loc[mask_new_val_except_2, "flag"] = "ok"
            mask_new_val_except_3 = (df_lower[col_to_be_cal] != 0)
            df_lower.loc[mask_new_val_except_3, "flag"] = "ok"
            # Assume that missing values (inf cal rates 0/0 or value/0) have a cal rate of 1
            df_lower.replace([np.inf, -np.inf], np.nan, inplace=True)
            mask_no_cal_rate = (df_lower[col_cal_rate].isna())
            df_lower.loc[mask_no_cal_rate, col_cal_rate] = 1.0

            #   Find cal rate for max year
            df_cal_rate_max_year = df_lower
            mask_max_year = (df_cal_rate_max_year["Years"] == df_cal_rate_max_year["max_year"])
            df_cal_rate_max_year = df_cal_rate_max_year.loc[
                mask_max_year, self.common_dimensions_excl_years + [col_cal_rate, "flag"]]

            #   > max year year : apply cal rate from max year
            #                     new value = cal rate * data to calibrate
            df_upper = df_upper.merge(df_cal_rate_max_year, on=self.common_dimensions_excl_years, how="left")
            df_upper[col_calibrated] = df_upper[col_to_be_cal] * df_upper[col_cal_rate]

            #   Merge sub flow <= max year and > max year
            output_table = pd.concat([df_lower, df_upper], ignore_index=True)

            # ---------------------------------------------------------- #
            # Apply calibration (if requested by the user)
            if apply_calib:
                output_table[col_to_be_cal] = output_table[col_calibrated]

            # Merge flow with missing and non missing
            output_table = pd.concat([output_table, df_missing], ignore_index=True)
        else:
            output_table = df_missing

        # Filter unused columns
        for col in ["missing_cal", "max_year", col_calibrated, col_cal]:
            if col in output_table.columns:
                del output_table[col]

        output_table = output_table[self.common_dimensions_left + ["flag", col_to_be_cal, col_cal_rate]]

        # ---------------------------------------------------------- #
        # RETURN RESULTS
        # Filter the data for the three outputs

        # PORT 1 : Calibrated data (if required)
        # If apply calibration = No => output_table (out_port = 1) = input_table (in_port = 1)
        # Else (apply calibration = Yes) => output_table (out_port = 1) = input_table (in_port = 1) with values calibrated
        output_table_1 = pd.DataFrame()
        for col in output_table.columns:
            if ('cal_rate' not in col) and (col != "flag"):
                output_table_1[col] = output_table[col]

        # PORT 2 : Cal rates
        # Cal rate => output_table (out_port = 2) => cal_rate by dimensions
        output_table_2 = pd.DataFrame()
        for col in output_table.columns:
            if (col in self.common_dimensions_left) or (col == col_cal_rate):
                output_table_2[col] = output_table[col]

        # PORT 3 : Flags with all related data
        # Calibrated data with calibration rates and flags.
        # Possible flags :
        # - ok
        # - issue (missing official)
        # - issue (model at 0 and non zero official)
        output_table_3 = output_table
        output_table_3 = output_table_3[self.common_dimensions + ["flag", col_to_be_cal, col_cal_rate]]

        return (
            output_table_1,
            output_table_2,
            output_table_3,
        )

    # def update(self):
    #     # This is a copy paste of the run function without the search for dimensions

    #     # Import table
    #     input_table = self.in_ports[1]
    #     cal_table = self.in_ports[2]

    #     # Rename calibrated column
    #     cal_table = cal_table.rename(
    #         columns={element: re.sub(r'(.*\[.*)', r'\1_cal', element) for element in cal_table.columns.tolist()})

    #     # Change value for year parameter dynamically if linked to a flow variable
    #     apply_calib = True if apply_calib == "Yes" else self.apply_calib

    #     # Parameters
    #     col_calibrated = "calibrated_value"
    #     col_cal = self.data_cal + "_cal"
    #     col_to_be_cal = self.data_to_be_cal
    #     col_cal_rate = "cal_rate_" + col_to_be_cal

    #     # Add calibration data to dataframe to calibrate
    #     output_table = input_table.merge(cal_table, how='left', on=self.common_dimensions)

    #     # Get max available year to calibrate (by common dimensions (except years)) from calibration table
    #     max_years_table = output_table.copy()
    #     max_years_table = max_years_table[~max_years_table[[col_cal, col_to_be_cal]].isna().any(axis=1)]
    #     max_years_table = max_years_table.groupby(self.common_dimensions_excl_years, as_index=False)["Years"].max()
    #     max_years_table.rename(columns={'Years': 'max_year'}, inplace=True)

    #     # Add max year available to calibrate the dataframe
    #     output_table = output_table.merge(max_years_table, on=self.common_dimensions_excl_years, how="left")

    #     # Add flag for data without calibration data in max year (missing_cal = 1)
    #     output_table["missing_cal"] = 0
    #     mask_na_missing = (output_table["max_year"].isna())
    #     output_table.loc[mask_na_missing, "missing_cal"] = 1

    #     # ---------------------------------------------------------- #
    #     # Split flow based on missing mask
    #     # Nothing is applied to missing
    #     # For non missing, apply calibration
    #     mask_missing = (output_table["missing_cal"] == 1)
    #     df_missing = output_table.loc[mask_missing].copy()
    #     df_not_missing = output_table.loc[~mask_missing].copy()

    #     # Assume that cal rate is 1 for missing values
    #     df_missing[col_cal_rate] = 1.0
    #     df_missing["flag"] = "issue (missing official)"

    #     # ---------------------------------------------------------- #
    #     # Proceed to calibration if calibration data available
    #     if not df_not_missing.empty:
    #         # ---------------------------------------------------------- #
    #         #  Split sub flow based on years (<= max year and > max year)
    #         #  Compute cal rates and apply them if required by user
    #         mask_year = (df_not_missing["Years"] <= df_not_missing["max_year"])
    #         df_lower = df_not_missing.loc[mask_year].copy()
    #         df_upper = df_not_missing.loc[~mask_year].copy()

    #         #   <= max year : new value is the calibration value (unless calibration value != 0 AND calibrated value = 0)
    #         #                 compute cal rate = calibration data / data to calibrate
    #         df_lower[col_calibrated] = df_lower[col_cal]
    #         mask_new_val_except_1 = (df_lower[col_cal] != 0) & (df_lower[col_to_be_cal] == 0)
    #         df_lower.loc[mask_new_val_except_1, col_calibrated] = df_lower.loc[mask_new_val_except_1, col_to_be_cal]
    #         df_lower[col_cal_rate] = df_lower[col_cal] / df_lower[col_to_be_cal]
    #         # Add related flags
    #         df_lower.loc[mask_new_val_except_1, "flag"] = "issue (model at 0 and non zero official)"
    #         mask_new_val_except_2 = (df_lower[col_cal] == 0) & (df_lower[col_to_be_cal] == 0)
    #         df_lower.loc[mask_new_val_except_2, "flag"] = "ok"
    #         mask_new_val_except_3 = (df_lower[col_to_be_cal] != 0)
    #         df_lower.loc[mask_new_val_except_3, "flag"] = "ok"
    #         # Assume that missing values (inf cal rates 0/0 or value/0) have a cal rate of 1
    #         df_lower.replace([np.inf, -np.inf], np.nan, inplace=True)
    #         mask_no_cal_rate = (df_lower[col_cal_rate].isna())
    #         df_lower.loc[mask_no_cal_rate, col_cal_rate] = 1.0

    #         #   Find cal rate for max year
    #         df_cal_rate_max_year = df_lower
    #         mask_max_year = (df_cal_rate_max_year["Years"] == df_cal_rate_max_year["max_year"])
    #         df_cal_rate_max_year = df_cal_rate_max_year.loc[
    #             mask_max_year, self.common_dimensions_excl_years + [col_cal_rate, "flag"]]

    #         #   > max year year : apply cal rate from max year
    #         #                     new value = cal rate * data to calibrate
    #         df_upper = df_upper.merge(df_cal_rate_max_year, on=self.common_dimensions_excl_years, how="left")
    #         df_upper[col_calibrated] = df_upper[col_to_be_cal] * df_upper[col_cal_rate]

    #         #   Merge sub flow <= max year and > max year
    #         output_table = pd.concat([df_lower, df_upper], ignore_index=True)

    #         # ---------------------------------------------------------- #
    #         # Apply calibration (if requested by the user)
    #         if self.apply_calib:
    #             output_table[col_to_be_cal] = output_table[col_calibrated]

    #         # Merge flow with missing and non missing
    #         output_table = pd.concat([output_table, df_missing], ignore_index=True)
    #     else:
    #         output_table = df_missing

    #     # Filter unused columns
    #     for col in ["missing_cal", "max_year", col_calibrated, col_cal]:
    #         if col in output_table.columns:
    #             del output_table[col]

    #     output_table = output_table[self.common_dimensions_left + ["flag", col_to_be_cal, col_cal_rate]]

    #     # ---------------------------------------------------------- #
    #     # RETURN RESULTS
    #     # Filter the data for the three outputs

    #     # PORT 1 : Calibrated data (if required)
    #     # If apply calibration = No => output_table (out_port = 1) = input_table (in_port = 1)
    #     # Else (apply calibration = Yes) => output_table (out_port = 1) = input_table (in_port = 1) with values calibrated
    #     output_table_1 = pd.DataFrame()
    #     for col in output_table.columns:
    #         if ('cal_rate' not in col) and (col != "flag"):
    #             output_table_1[col] = output_table[col]

    #     # PORT 2 : Cal rates
    #     # Cal rate => output_table (out_port = 2) => cal_rate by dimensions
    #     output_table_2 = pd.DataFrame()
    #     for col in output_table.columns:
    #         if (col in self.common_dimensions_left) or (col == col_cal_rate):
    #             output_table_2[col] = output_table[col]

    #     # PORT 3 : Flags with all related data
    #     # Calibrated data with calibration rates and flags.
    #     # Possible flags :
    #     # - ok
    #     # - issue (missing official)
    #     # - issue (model at 0 and non zero official)
    #     output_table_3 = output_table
    #     output_table_3 = output_table_3[self.common_dimensions + ["flag", col_to_be_cal, col_cal_rate]]

    #     self.out_ports[1] = output_table_1
    #     self.out_ports[2] = output_table_2
    #     self.out_ports[3] = output_table_3


if __name__ == '__main__':
    id = '7172'
    relative_path = f'/Users/climact/patex-container/dev/prototype/Workflows/generic_flow/Calibrate (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = CalibrationNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in_1.csv')
    node.in_ports[2] = pd.read_csv('/Users/climact/Desktop/in_2.csv')
    node.run()
    list = ["Country", "Years", "transport-user", "vehicule-type"]
    node.out_ports[1].sort_values(list).to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)
    pd.read_csv('/Users/climact/Desktop/out_knime.csv').sort_values(list).to_csv('/Users/climact/Desktop/out_knime.csv',
                                                                                 index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_pycharm.csv'),
                                  pd.read_csv('/Users/climact/Desktop/out_knime.csv'), check_dtype=False)
