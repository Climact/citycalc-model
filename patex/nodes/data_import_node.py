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
    DATA IMPORT METANODE
    ============================
    KNIME options implemented:
        - All
"""
import collections
import os
import xml.etree.cElementTree as ET
from timeit import default_timer as timer

from patex.nodes.node import Node


class DataImportNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.model = []
        self.variable_name_List = []
        self.value_List = []
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None, 2: None}

    def build_node(self):
        start = timer()
        self.model = self.xml_root.find(self.xmlns + "config[@key='model']")
        # Find values of the flow variables provided by user:
        paths = []
        for child in self.model:
            path = child.get('key')
            paths.append(path)
            for grandchild in child:
                isLeaf = True
                for greatgrandchild in grandchild:
                    isLeaf = False
                    if greatgrandchild.get('key') != "array-size":
                        value = greatgrandchild.get('value')
                if isLeaf:
                    value = grandchild.get('value')
            self.value_List.append(value)
            if "string-input" in path:
                input_id = path.split("string-input-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "String Input (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()
                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                self.variable_name_List.append(inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value'))
            elif "single-selection" in path:
                input_id = path.split("single-selection-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "Single Selection (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()
                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                self.variable_name_List.append(inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value'))
            elif "data-type" in path:
                input_id = path.split("data-type-")[1]
                inputs_xml_file = os.path.join(
                    self.xml_file.split('settings.xml')[0] + "Multiple Selections (#" + input_id + ")", 'settings.xml')
                inputs_tree = ET.ElementTree(file=inputs_xml_file)
                inputs_root = inputs_tree.getroot()
                inputs_model = inputs_root.find(self.xmlns + "config[@key='model']")
                self.variable_name_List.append(inputs_model.find(self.xmlns + "entry[@key='flowvariablename']").get('value'))
            else:
                raise Exception("Unknown quickform node used in metanode (#" + str(
                    self.id) + '). Settings file is : ' + self.xml_file)

        self.logger.debug('The python source code is hardcoded in data_import_node.py ')
        self.source_code = "import pandas as pd%%00010import zipfile%%00010import lxml.etree%%00010import numpy as np%%00010%%00010datafolder = flow_variables['module']+'/data'%%00010trigrams = {'agriculture':'agr',%%00010%%00009%%00009%%00009'buildings':'bld',%%00010%%00009%%00009%%00009'climate':'clm',%%00010%%00009%%00009%%00009'electricity_supply':'elc',%%00010%%00009%%00009%%00009'employment':'emp',%%00010%%00009%%00009%%00009'industry':'ind',%%00010%%00009%%00009%%00009'lifestyles':'lfs',%%00010%%00009%%00009%%00009'materials':'mat',%%00010%%00009%%00009%%00009'social impacts':'sip',%%00010%%00009%%00009%%00009'transboundary':'trb',%%00010%%00009%%00009%%00009'transport':'tra'%%00010}%%00010mod_tri = trigrams.get(flow_variables['module'])%%00010flow_variables['mod_tri'] = mod_tri %%00010%%00010datalist = flow_variables['data-type'].split(&quot;,&quot;)%%00010%%00010%%00010#--------------------#%%00010#OUTPUT TABLE 1 = OTS%%00010#--------------------#%%00010if 'historical' in datalist:%%00010%%00009flow_variables['historical_switch'] = 'top'%%00010%%00009# the filepath is made by retrieving the global variables from the KNIME environment ('right-click&gt;KNIME variables' on the KNIME workflow)%%00010%%00009filepath = flow_variables['knime.workspace']+'/'+datafolder+'/ots_'+mod_tri+&quot;_&quot;+flow_variables['name']+&quot;.xlsx&quot;%%00010%%00009%%00010%%00009# read the excel files%%00010%%00009xl = pd.ExcelFile(filepath)#excel file containing data%%00010%%00009md = pd.ExcelFile(filepath.split(&quot;.&quot;,1)[0]+'_md.xlsx')#excel file containing metadata%%00010%%00009#l = len(xl.sheet_names)#number of sheets%%00010%%00009%%00010%%00009s = [n for n in xl.sheet_names if n[0:1] != '_'] # Excludes sheets starting with an underscore%%00010%%00009%%00010%%00009# retrieve metadatas%%00010%%00009metadata = md.parse('metadata')%%00010%%00009metadata = metadata.set_index('name')%%00010%%00009title = metadata.loc['Title'].value%%00010%%00009unit = metadata.loc['Unit'].value%%00010%%00009category = metadata.loc['data type'].value%%00010%%00009levername = metadata.loc['Variable'].value%%00010%%00009%%00010%%00009levername_column = &quot;lever_&quot;+levername%%00010%%00009%%00009%%00010%%00009init = 0%%00010%%00009# loop through all the excel sheets%%00010%%00009for i in s:%%00010%%00009%%00009df = xl.parse(i)#parsing the data of the sheet inside a dataframe%%00010%%00009%%00009df.columns = df.columns.astype(str)%%00010%%00009%%00009filter_col = [col for col in list(df) if col.startswith('2') or col.startswith('1')]#selecting all the columns that start with '2'. Those columns are the values of the pivot table. WARNING: if the name of the column are not converted in STRINGS, there will be an error running this line. To convert to string, 2 options: 1. str(col).startwith() 2. df.Years = df.Years.astype(str) %%00010%%00010%%00009%%00009# Note: this section modified to use any extra column as pivoting column.%%00010%%00009%%00009ID_VARS = list(set(df.columns) - set(filter_col))%%00010%%00009%%00009ID_MERGE = ID_VARS + ['Years']%%00010%%00009%%00009%%00009%%00010%%00009%%00009df = pd.melt(df, id_vars=ID_VARS, value_vars=filter_col ,var_name='Years',value_name=category+&quot;_&quot;+mod_tri+&quot;_&quot;+levername+&quot;_&quot;+i+&quot;[&quot;+unit+&quot;]&quot;)#pivoting the columns using 'Country' as pivot.%%00010%%00009%%00009if init == 0:%%00010%%00009%%00009%%00009output_table_1 = df%%00010%%00009%%00009%%00009init = 1%%00010%%00009%%00009else:%%00010%%00009%%00009%%00009output_table_1 = pd.merge(output_table_1, df, on=ID_MERGE)%%00010else:%%00010%%00009flow_variables['historical_switch'] = 'bottom'%%00010%%00009output_table_1 = pd.DataFrame()%%00010%%00010%%00010%%00009#--------------------#%%00010%%00009#OUTPUT TABLE 2 = LL%%00010%%00009#--------------------#%%00009%%00010if 'scenario' in datalist:%%00010%%00009flow_variables['scenario_switch'] = 'top'%%00010%%00009# the filepath is made by retrieving the global variables from the KNIME environment ('right-click&gt;KNIME variables' on the KNIME workflow)%%00010%%00009filepath = flow_variables['knime.workspace']+'/'+datafolder+'/ll_'+mod_tri+&quot;_&quot;+flow_variables['name']+&quot;.xlsx&quot;%%00010%%00009%%00010%%00009# read the excel files%%00010%%00009xl = pd.ExcelFile(filepath)#excel file containing data%%00010%%00009md = pd.ExcelFile(filepath.split(&quot;.&quot;,1)[0]+'_md.xlsx')#excel file containing metadata%%00010%%00009#l = len(xl.sheet_names)#number of sheets%%00010%%00010%%00009s = [n for n in xl.sheet_names if n[0:1] != '_'] # Excludes sheets starting with an underscore%%00010%%00010%%00009%%00010%%00009# retrieve metadatas%%00010%%00009metadata = md.parse('metadata')%%00010%%00009metadata = metadata.set_index('name')%%00010%%00009title = metadata.loc['Title'].value%%00010%%00009unit = metadata.loc['Unit'].value%%00010%%00009category = metadata.loc['data type'].value%%00010%%00009levername = metadata.loc['Variable'].value%%00010%%00009%%00010%%00009levername_column = &quot;lever_&quot;+levername%%00010%%00009flow_variables[&quot;levername_column&quot;] = levername_column%%00010%%00009%%00009%%00010%%00009init = 0%%00010%%00009# loop through all the excel sheets%%00010%%00009for i in s:%%00010%%00009%%00009df = xl.parse(i)#parsing the data of the sheet inside a dataframe%%00010%%00009%%00009df.columns = df.columns.astype(str)%%00010%%00009%%00009filter_col = [col for col in list(df) if col.startswith('2')]#selecting all the columns that start with '2'. Those columns are the values of the pivot table. WARNING: if the name of the column are not converted in STRINGS, there will be an error running this line. To convert to string, 2 options: 1. str(col).startwith() 2. df.Years = df.Years.astype(str) %%00010%%00009%%00009%%00010%%00009%%00009# Note: this section modified to use any extra column as pivoting column.%%00010%%00009%%00009ID_VARS = list(set(df.columns) - set(filter_col))%%00010%%00009%%00009ID_MERGE = ID_VARS + ['Years']%%00010%%00009%%00009%%00010%%00009%%00009%%00009%%00010%%00009%%00009df = pd.melt(df, id_vars=ID_VARS, value_vars=filter_col ,var_name='Years',value_name=category+&quot;_&quot;+mod_tri+&quot;_&quot;+levername+&quot;_&quot;+i+&quot;[&quot;+unit+&quot;]&quot;)%%00010%%00009%%00009if init == 0:%%00010%%00009%%00009%%00009output_table_2 = df%%00010%%00009%%00009%%00009init = 1%%00010%%00009%%00009else:%%00010%%00009%%00009%%00009output_table_2 = pd.merge(output_table_2, df, on=ID_MERGE)%%00010%%00009%%00010%%00009output_table_2.rename(columns = {'Level':levername_column}, inplace = True)%%00010%%00010else:%%00010%%00009flow_variables['scenario_switch'] = 'bottom'%%00010%%00009flow_variables['levername_column'] = 'no-scenario'%%00010%%00009output_table_2 = pd.DataFrame()%%00009%%00009"
        # Decode xml string of source code:
        self.source_code = self.source_code.replace('%%00010', '\n')
        self.source_code = self.source_code.replace('%%00009', '\t')
        self.source_code = self.source_code.replace('&quot;', '"')
        self.source_code = self.source_code + '\n'  # whitespace padding in case the code ends on a loop that requires a final "enter" to be run
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "EUCALC Data Import")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', "EUCALC Data Import")
        outer_flow_vars = dict(self.flow_vars)
        #  Set default values of the flow variable parameters:
        if 'module' not in self.flow_vars:
            self.flow_vars['module'] = ('STRING', "agriculture")
        if 'name' not in self.flow_vars:
            self.flow_vars['name'] = ('STRING', "lever")
        if 'data-type' not in self.flow_vars:
            self.flow_vars['data-type'] = ('STRING', "historical,scenario")
        for child in self.model:
            self.flow_vars[self.variable_name_List.popleft()] = ('STRING', self.value_List.popleft())

        # Convert flow variable dictionary to OrderedDictionary as in knime:
        flow_variables = collections.OrderedDict()
        for var in self.flow_vars:
            flow_variables[var] = self.flow_vars[var][1]
        # Execute code:
        ns = {'output_table_1': None, 'output_table_2': None, 'flow_variables': flow_variables}
        exec(self.source_code, ns)
        # Update flow_vars:
        for var in flow_variables:
            self.flow_vars[var] = ('UNKNOWN_CLASS', flow_variables[var])
        # logger
        t = timer() - start
        self.log_timer(t, "END", "EUCALC Data Import")
        self.flow_vars = outer_flow_vars  # Make sure we filter out all the new flow variables created
        self.save_out_port(1, ns['output_table_1'])
        self.save_out_port(2, ns['output_table_2'])