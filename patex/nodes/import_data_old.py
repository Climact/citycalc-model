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
    IMPORT DATA METANODE
    ===================
    Import your data to your workflow.

    KNIME options implemented:
        - All
"""

import pickle
import zlib
from pathlib import Path
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node
from patex.utils import get_size


class ImportDataNode(Node):

    def __init__(self, id, xml_file, node_type, module_name, knime_workspace=None, google_sheet_prefix=None):
        self.local_flow_vars = {}
        self.dataframe = pd.DataFrame()
        self.dataframe_fts = pd.DataFrame()
        self.google_sheet_prefix = google_sheet_prefix
        self.module_name = module_name
        self.max_size = 50*1e6  # in MB
        self.compressed = False
        self.compressed_fts = False
        super().__init__(id, xml_file, node_type, knime_workspace, self.module_name)
        ##COMMENT RECUPER LA VARIABLE DE FLUX metanode (definie dans wrapped_meta_node) = parent qui la définit ?!
        ##POURQUOI ON NE SAIT PAS AVOIR TOUTES LES VARIABLES DE FLUX ?? => investiguer !

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['variable_name'] = ('STRING', "metric-name")
        self.local_flow_vars['variable_type'] = ('STRING', "OTS/FTS")
        self.local_flow_vars['lever_name'] = ('STRING', "lever_.*")
        self.local_flow_vars['folder'] = ('STRING', "_interactions")


        # If google_sheet_prefix is None => default value = EU
        if self.google_sheet_prefix is None:
            self.google_sheet_prefix = "EU"

        # Define file path
        path_data = Path(self.local, self.local_flow_vars['folder'][1], "data",
                         self.google_sheet_prefix)

        # Find values of the flow variables provided by user
        for child in model:
            for grandchild in child:
                if 'module-selection-' in child.get('key'):
                    for ggrandchild in grandchild:
                        if ggrandchild.get('key') == "0":
                            value = ggrandchild.get('value')
                            self.local_flow_vars['module_selection'] = ('STRING', value)
                if 'string-input-' in child.get('key'):
                    value = grandchild.get('value')
                    self.local_flow_vars['variable_name'] = ('STRING', value)
                if 'single-selection-' in child.get('key'):
                    value = grandchild.get('value')
                    self.local_flow_vars['variable_type'] = ('STRING', value)

        # Select module
        try:
            if self.local_flow_vars['module_selection'][1] == 'Current module':
                current_module = self.module_name
            else:
                current_module = self.local_flow_vars['module_selection'][1].lower()
        except KeyError:
            current_module = self.module_name

        # Get trigram based on module name
        if current_module == "industry":
            self.local_flow_vars['trigram'] = ('STRING', "ind")
        elif current_module == "ammonia":
            self.local_flow_vars['trigram'] = ('STRING', "amm")
        elif current_module == "agriculture":
            self.local_flow_vars['trigram'] = ('STRING', "agr")
        elif current_module == "lifestyle":
            self.local_flow_vars['trigram'] = ('STRING', "lfs")
        elif current_module == "land use":
            self.local_flow_vars['trigram'] = ('STRING', "lus")
        elif current_module == "electricity_supply":
            self.local_flow_vars['trigram'] = ('STRING', "elc")
        elif current_module == "air_pollution":
            self.local_flow_vars['trigram'] = ('STRING', "air")
        elif current_module == "buildings":
            self.local_flow_vars['trigram'] = ('STRING', "bld")
        elif current_module == "transport":
            self.local_flow_vars['trigram'] = ('STRING', "tra")
        elif current_module == "technology":
            self.local_flow_vars['trigram'] = ('STRING', "tec")
        elif current_module == "water":
            self.local_flow_vars['trigram'] = ('STRING', "wat")
        elif current_module == "district heating":
            self.local_flow_vars['trigram'] = ('STRING', "dhg")
        elif current_module == "biodiversity":
            self.local_flow_vars['trigram'] = ('STRING', "bdy")
        elif current_module == "climate":
            self.local_flow_vars['trigram'] = ('STRING', "clm")
        elif current_module == "climate emissions":
            self.local_flow_vars['trigram'] = ('STRING', "clt")
        elif current_module == "employment":
            self.local_flow_vars['trigram'] = ('STRING', "emp")
        elif current_module == "materials":
            self.local_flow_vars['trigram'] = ('STRING', "mat")
        elif current_module == "minerals":
            self.local_flow_vars['trigram'] = ('STRING', "min")
        elif current_module == "scope_2_3" :
            self.local_flow_vars['trigram'] = ('STRING', "sco")

        # Initialize tables
        input_table = pd.DataFrame()
        input_table_fts = pd.DataFrame()
        ## ------------- OTS / FTS --------------- ##
        if self.local_flow_vars['variable_type'][1] == 'OTS/FTS':
            # Get fil names
            for country in self.flow_vars['country_filter'][1].split('|'):
                if country != "EU28":
                    file_name_ots = country + "_" + self.local_flow_vars['trigram'][1] + \
                                    "_" + self.local_flow_vars['variable_name'][1] + "_ots.csv"
                    file_name_fts = country + "_" + self.local_flow_vars['trigram'][1] + \
                                    "_" + self.local_flow_vars['variable_name'][1] + "_fts.csv"
                    # Read FTS data
                    df_t = pd.read_csv(Path(path_data, file_name_fts))
                    input_table_fts = pd.concat([input_table_fts, df_t], ignore_index=True)
                    # Read OTS data
                    df_t = pd.read_csv(Path(path_data, file_name_ots))
                    input_table = pd.concat([input_table, df_t], ignore_index=True)
            # Get lever_name
            # It's possible to have more than one lever for a given variable
            # (each one controlling a specific dimension)
            self.local_flow_vars['lever_name'] = ('LIST', list(set(input_table_fts['lever-name'])))
            # Change OTS table
            metric_name_ots = input_table["metric-name"][0]
            input_table.rename(columns={'ColumnValues': metric_name_ots}, inplace=True)
            del input_table['metric-name']

        ## ------------- OTS (only) --------------- ##
        elif self.local_flow_vars['variable_type'][1] == 'OTS (only)':
            for country in self.flow_vars['country_filter'][1].split('|'):
                if country != "EU28":
                    file_name = country + "_" + self.local_flow_vars['trigram'][1] + \
                                    "_" + self.local_flow_vars['variable_name'][1] + "_ots.csv"
                    df_t = pd.read_csv(Path(path_data, file_name))
                    input_table = pd.concat([input_table, df_t], ignore_index=True)
            metric_name = input_table["metric-name"][0]
            input_table.rename(columns={'ColumnValues': metric_name}, inplace=True)
            del input_table['metric-name']

        ## ------------- Calibration --------------- ##
        elif self.local_flow_vars['variable_type'][1] == 'Calibration':
            for country in self.flow_vars['country_filter'][1].split('|'):
                if country != "EU28":
                    file_name = country + "_" + self.local_flow_vars['trigram'][1] + \
                                "_" + self.local_flow_vars['variable_name'][1] + "_cal.csv"
                    df_t = pd.read_csv(Path(path_data, file_name))
                    input_table = pd.concat([input_table, df_t], ignore_index=True)

        ## ---------------- HTS ------------------ ##
        elif self.local_flow_vars['variable_type'][1] == 'HTS':
            for country in self.flow_vars['country_filter'][1].split('|'):
                if country != "EU28":
                    file_name = country + "_" + self.local_flow_vars['trigram'][1] + \
                                "_" + self.local_flow_vars['variable_name'][1] + "_rcp.csv"
                    df_t = pd.read_csv(Path(path_data, file_name))
                    input_table = pd.concat([input_table, df_t], ignore_index=True)

        ## ---------------- RCP ------------------ ##
        elif self.local_flow_vars['variable_type'][1] == 'RCP':
                for country in self.flow_vars['country_filter'][1].split('|'):
                    if country != "EU28":
                        file_name = country + "_" + self.local_flow_vars['trigram'][1] + \
                                "_" + self.local_flow_vars['variable_name'][1] + "_rcp.csv"
                        df_t = pd.read_csv(Path(path_data, file_name))
                        input_table = pd.concat([input_table, df_t], ignore_index=True)

        ## ---------------- CP ------------------ ##
        elif self.local_flow_vars['variable_type'][1] == 'CP':
            file_name = self.local_flow_vars['variable_name'][1] + "_cp.csv"
            input_table = pd.read_csv(Path(path_data, file_name))

        ## ******************* FOR ALL ******************* ##
        ## For all : Years type = int
        if 'Years' in input_table.columns:
            input_table['Years'] = input_table['Years'].astype(int)

        ## If big file (input_table) => compress
        if get_size(input_table) > self.max_size:  # if file size is greater than max size, we compress the dataframe
            self.compressed = True
            self.dataframe = zlib.compress(pickle.dumps(input_table))
        else:
            self.dataframe = input_table

        ## If big file (input_table_fts) => compress
        if get_size(input_table_fts) > self.max_size:  # if file size is greater than max size, we compress the dataframe
            self.compressed_fts = True
            self.dataframe_fts = zlib.compress(pickle.dumps(input_table_fts))
        else:
            self.dataframe_fts = input_table_fts

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "Import Data")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "Import Data")

        # --------------------------------------------------------#
        # If OTS/FTS : import data frame + apply lever selection and concat fts / ots
        if self.local_flow_vars['variable_type'][1] == 'OTS/FTS':
            # Import OTS
            if self.compressed:
                df_ots = pickle.loads(zlib.decompress(self.dataframe))
            else:
                df_ots = self.dataframe

            # Import FTS
            if self.compressed_fts:
                df = pickle.loads(zlib.decompress(self.dataframe_fts))
            else:
                df = self.dataframe_fts

            # Get lever values
            selected_levers = {'lever-name': [], 'lever-value-selected': []}
            for var_name, var_value in self.flow_vars.items():
                if var_name in self.local_flow_vars['lever_name'][1]:
                    selected_levers['lever-name'].append(var_name)
                    selected_levers['lever-value-selected'].append(var_value[1])
            df_levers = pd.DataFrame(selected_levers)

            # Add value selected as a column
            df = df.merge(df_levers, how='left', on='lever-name')

            # Add level 0
            df['level_0'] = 0

            # Loop on levers values and compute it
            df_fts = df.copy()
            df_tmp = []
            metric_name_fts = df_fts["metric-name"][0]
            for i in range(0, 4):
                # Select row based on lever-value-selected
                mask_level = (df_fts['lever-value-selected'] > i) & (df_fts['lever-value-selected'] <= i + 1)
                try:
                    df_tmp_tmp = df_fts.copy().loc[mask_level, :]
                    # Set useful lever values
                    level_niv1 = "level_" + str(i)
                    level_niv2 = "level_" + str(i + 1)
                    niv1 = df_tmp_tmp[level_niv1]
                    niv2 = df_tmp_tmp[level_niv2]
                    diff = df_tmp_tmp["lever-value-selected"] - i
                    # Calculate lever value
                    df_tmp_tmp[metric_name_fts] = niv1 + ((niv2 - niv1) * diff)
                    # Concatenate with df
                    if not df_tmp_tmp.empty:
                        df_tmp.append(df_tmp_tmp)
                except KeyError:
                    pass
            df_fts = df_tmp

            # Add OTS values
            output_table = pd.concat([df_ots] + df_fts, ignore_index=True)

            # Remove unused columns
            columns_to_delete = ["level_0", "level_1", "level_2", "level_3", "level_4", "metric-name",
                                 "lever-name",
                                 "lever-value-selected"]
            for i in columns_to_delete:
                del output_table[i]

            self.save_out_port(1, output_table)

        # --------------------------------------------------------#
        # Else (other than OTS / FTS) : Import data_frame
        else:
            if self.compressed:
                df = pickle.loads(zlib.decompress(self.dataframe))
                # Rename Region as Country
                if "Region" in df.columns:
                    df = df.rename(columns={"Region": "Country"})
                self.save_out_port(1, df)
            else:
                df = self.dataframe
                # Rename Region as Country
                if "Region" in df.columns:
                    df = df.rename(columns={"Region": "Country"})
                self.save_out_port(1, df)

        # logger
        t = timer() - start
        self.log_timer(t, "END", "Import Data")


if __name__ == "__main__":
    id = '6064'
    knime_workspace = '/Users/climact/XCalc/dev'
    module_name = 'transport'
    node_type = 'SubNode'
    xml_file = f'/Users/climact/XCalc/dev/electricity_supply/workflows/power_supply_processing/5_0 Power Su (#3776)/Electricity (#6431)/Import Data (#{id})/settings.xml'
    node = ImportDataNode(id, xml_file, node_type, module_name, knime_workspace)
    node.run()
    print(node)
