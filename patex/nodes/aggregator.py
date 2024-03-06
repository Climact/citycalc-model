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
    AGGREGATOR
    ============================
    Auxiliary function called by the EUCalc split and parallel op nodes
"""

import logging
import re

import numpy as np

logger = logging.getLogger(__name__)


def aggregate(df, unit_var, aggregation_method, index_pattern, aggregation_pattern, new_name_start, aggregation_remove,
              caller, calibration_bool = False, new_name_suffix = '' ):
    # t_start = timer()
    list_of_columns_1 = []
    list_of_columns_2 = []
    new_columns_list = ""
    id_double = 1

    column_names = list(df.columns.values)

    pattern_i = re.compile(index_pattern + "$")  # $ asserts position at the end of a line
    pattern_a = re.compile(aggregation_pattern + "$")  # $ asserts position at the end of a line
    pattern_u = re.compile(".*\[(.*)\].*")

    list_of_names_with_conflicts = []
    dict_of_names_with_conflicts = {}

    isMatching1 = False

    matcher1_list = {col: re.match(pattern_i, col) for col in df.columns if re.match(pattern_i, col)}
    matcher2_list = {col: re.match(pattern_a, col) for col in df.columns if re.match(pattern_a, col)}

    total_list_col1 = []
    total_list_col2 = []

    for col1, match1 in matcher1_list.items():
        matcher_unit = re.match(pattern_u, col1)
        colUnit = unit_var
        try:
            iterator1 = match1.group('i')
        except:
            iterator1 = match1.group(1)
        numberGroups1 = len(match1.groups())
        matchingIterator = False

        if matcher_unit and colUnit == "unit" and not calibration_bool:
            colUnit = matcher_unit.group(1)

        for col2, match2 in matcher2_list.items():
            numberGroups2 = len(match2.groups())
            if calibration_bool:
                matcher_unit = re.match(pattern_u, col2)
                colUnit = matcher_unit.group(1)
            if numberGroups2 == 0:
                new_name = new_name_start + "_" + iterator1 + new_name_suffix +"[" + colUnit + "]"
                if new_name in new_columns_list or new_name in column_names:
                    # raise Exception("A column with name " + new_name + " already exists!")
                    new_name_temp = new_name + " (#" + str(id_double) + ")"
                    id_double += 1
                    if new_name in column_names:
                        list_of_names_with_conflicts.append(new_name_temp)
                        dict_of_names_with_conflicts[new_name_temp] = new_name
                    new_name = new_name_temp

                new_columns_list = new_columns_list + new_name + ","

                if col1 not in list_of_columns_1:
                    list_of_columns_1.append(col1)
                if col2 not in list_of_columns_2:
                    list_of_columns_2.append(col2)

                total_list_col1.append(col1)
                total_list_col2.append(col2)

            elif numberGroups2 == 1 or numberGroups2 == 2:  # Parallel operation
                #if caller == 'SPLIT':
                #    logger.debug("Number of capture groups in aggregation pattern for a tree split node must be 0")

                try:
                    iterator2 = match2.group('i')
                except:
                    iterator2 = match2.group(1)
                if iterator1 == iterator2:
                    matchingIterator = True
                    new_name = new_name_start + '_' + match2.group(1)
                    if numberGroups1 == 2:
                        new_name = new_name + "_" + match1.group(2)
                    if numberGroups2 == 2:
                        new_name = new_name + "_" + match2.group(2)

                    new_name = new_name + new_name_suffix +"[" + colUnit + "]"
                    if new_name in new_columns_list or new_name in column_names:
                        # raise Exception("A column with name " + new_name + " already exists!")
                        new_name_temp = new_name + " (#" + str(id_double) + ")"
                        id_double += 1
                        if new_name in column_names:
                            list_of_names_with_conflicts.append(new_name_temp)
                            dict_of_names_with_conflicts[new_name_temp] = new_name
                        new_name = new_name_temp
                    new_columns_list = new_columns_list + new_name + ","

                    if col1 not in list_of_columns_1:
                        list_of_columns_1.append(col1)
                    if col2 not in list_of_columns_2:
                        list_of_columns_2.append(col2)

                    total_list_col1.append(col1)
                    total_list_col2.append(col2)

            else:
                raise Exception("Number of capture groups in aggregation pattern cannot be more than 2")
        if not matchingIterator and numberGroups2 == 1:
            raise Exception("Iterator not found in second column group: '" + iterator1 + "'")

    if not matcher2_list:
        raise Exception("No match found in input columns for pattern '" + aggregation_pattern + "'")
    if not matcher1_list:
        raise Exception("No match found in input columns for pattern \"" + index_pattern + "\"")

     # perform computations and store results:

    df_subset1 = df[total_list_col1]
    df_subset2 = df[total_list_col2]

    if aggregation_method == 'Product':
        output = df_subset1 * df_subset2.values
    elif aggregation_method == 'Sum':
        output = df_subset1 + df_subset2.values
    elif aggregation_method == "Division 1/2":
        if caller == "CAL":
            output = df_subset1
            mask1 = (df_subset1 != 0)
            mask2 = (df_subset2 != 0)
            mask21 = mask2.copy()
            mask21.columns = output.columns
            mask3 = df_subset1.isna()
            output[mask21] = df_subset1[mask21]/ df_subset2[mask2].values
            output[-mask21 & -mask3] = 100
            output[-mask1] = 0
        else:
            output = df_subset1 / df_subset2.values
            output = output.replace([np.inf, -np.inf], np.nan)
    elif aggregation_method == "Division 2/1":
        output = df_subset2.values / df_subset1
    elif aggregation_method == "Subtraction 1-2":
        output = df_subset1 - df_subset2.values
    elif aggregation_method == "Subtraction 2-1":
        output = df_subset2.values - df_subset1
    else:
        raise Exception("Unknown aggregation method!")

    list_of_new_column_names = new_columns_list.split(",")
    del list_of_new_column_names[-1]
    output.columns = list_of_new_column_names
    if aggregation_remove == 1:
        df = df.drop(list_of_columns_1+list_of_columns_2, axis=1)
        output = output.rename(columns=dict_of_names_with_conflicts)

    # t3 = timer()
    # logger.info("Third timer: " +str(t3-t2))
    # the 2 following lines are there to avoid mismatching between index resulting of adding new lines in the resulting output dataframe
    df = df.reset_index(drop=True)
    output = output.reset_index(drop=True)

    output_table = df.join(output)
    # logger.info('Final timer: ' + str(timer() - t3))
    # logger.info("Total TIMER: " +str(timer()-t_start))
    return output_table
