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
    PYTHON 2 TO 1 NODE
    ============================
    KNIME options implemented:
        - Everything except execution of Python 2 code, conversion of missing values/sentinel values and sentinel values other than MIN_VAL.
                    Row chunking, and row limits not implemented either.
"""

import collections

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode, FlowVars


class Python21Node(PythonNode, NativeNode):
    knime_name = "Python Script (2⇒1)"

    # TODO: See what row chunking does, and check if any exceptions need to be raised concerning it.
    def __init__(self, code: str):
        super().__init__()
        self.program = compile(code, 'code_String', 'exec', optimize=-1)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    @staticmethod
    def temp_code(input_table_1, input_table_2):
        import pandas as pd

        ## input
        # input_table_1=pd.read_excel('knime_python.xlsx',header=0,index_col=0)
        # output_table_2=pd.read_excel('tec_ccu.xlsx',header=0,index_col=0)
        output_table = input_table_1
        output_table['ccu_capex_methane-formation[MEUR]'] = [0] * 957
        output_table['ccu_capex_enhanced-oil-recovery[MEUR]'] = [0] * 957
        output_table['ccu_capex_depleted-oil-gas-reservoirs[MEUR]'] = [0] * 957
        output_table['ccu_capex_unmineable-coal-seams[MEUR]'] = [0] * 957
        output_table['ccu_capex_saline-aquifers[MEUR]'] = [0] * 957
        output_table['ccu_capex_transport[MEUR]'] = [0.0] * 957
        output_table_2 = input_table_2

        # index=output_table[output_table['Years']=='2020'].index
        # kk=output_table_2["capex_ccu_ccus_transport[MEUR]"]
        # output_table.loc[index,'ccu_capex_transport[MEUR]']=kk.values
        input_table_1 = input_table_1.reset_index()
        output_table_2 = output_table_2.reset_index()
        countries = input_table_1['Country'][0:29].tolist()
        output_table.set_index("Country")
        # capex
        for pays in countries:
            group = input_table_1.groupby('Country')
            country = group.get_group(pays)
            y = country[(country['Country'] == pays)]

            if sum(y['ccu_ccus_transport[Mt]'].values) != 0.0:
                index_temp = output_table_2[output_table_2['Country'] == pays].index
                spec_cost = output_table_2.loc[index_temp, "capex_ccu_ccus_transport[MEUR]"][output_table_2.loc[index_temp, "capex_ccu_ccus_transport[MEUR]"].index[0]] / max(
                    y['ccu_ccus_transport[Mt]'].values)
                index = y["ccu_ccus_transport[Mt]"].index
                tem_max = 0
                res = y["ccu_ccus_transport[Mt]"] * 0
                for t in range(0, 33):
                    qq = y["ccu_ccus_transport[Mt]"][y.index[t]]
                    if qq > tem_max:
                        res[t] = spec_cost * (qq - tem_max)
                        tem_max = qq
                output_table.loc[index, "ccu_capex_transport[MEUR]"] = res

                # output_table.loc[y.index,'ccu_capex_transport[MEUR]']=output_table_2.loc[index_temp,"capex_ccu_ccus_transport[MEUR]"][0]/sum(y['ccu_ccus_transport[Mt]'].values)*y['ccu_ccus_transport[Mt]']

            index = y["ccu_ccus_methane-formation[Mt]"].index
            spec_cost = output_table_2.loc[
                output_table_2["Country"] == pays, "capex_ccu_ccus_methane-formation[EUR/(t/y)]"]
            tem_max = 0
            res = y["ccu_ccus_methane-formation[Mt]"] * 0
            for t in range(0, 33):
                qq = y["ccu_ccus_methane-formation[Mt]"][y.index[t]]
                if qq > tem_max:
                    res[t] = spec_cost.values[0] / 30 * (qq - tem_max)
                    tem_max = qq
            index = y["ccu_ccus_hydrogen[TWh]"].index
            res2 = 37.8335813 / 30 * y["ccu_ccus_hydrogen[TWh]"]
            output_table.loc[index, "ccu_capex_methane-formation[MEUR]"] = res + res2

            index = y["ccu_ccus_enhanced-oil-recovery[Mt]"].index
            spec_cost = output_table_2.loc[
                output_table_2["Country"] == pays, "capex_ccu_ccus_enhanced-oil-recovery[EUR/(t/y)]"]
            tem_max = 0
            res = y["ccu_ccus_enhanced-oil-recovery[Mt]"] * 0
            for t in range(0, 33):
                qq = y["ccu_ccus_enhanced-oil-recovery[Mt]"][y.index[t]]
                if qq > tem_max:
                    res[t] = spec_cost.values[0] / 30 * (qq - tem_max)
                    tem_max = qq
            output_table.loc[index, "ccu_capex_enhanced-oil-recovery[MEUR]"] = res

            index = y["ccu_ccus_unmineable-coal-seams[Mt]"].index
            spec_cost = output_table_2.loc[
                output_table_2["Country"] == pays, "capex_ccu_ccus_unmineable-coal-seams[EUR/(t/y)]"]
            tem_max = 0
            res = y["ccu_ccus_unmineable-coal-seams[Mt]"] * 0
            for t in range(0, 33):
                qq = y["ccu_ccus_unmineable-coal-seams[Mt]"][y.index[t]]
                if qq > tem_max:
                    res[t] = spec_cost.values[0] / 30 * (qq - tem_max)
                    tem_max = qq
            output_table.loc[index, "ccu_capex_unmineable-coal-seams[MEUR]"] = res

            index = y["ccu_ccus_depleted-oil-gas-reservoirs[Mt]"].index
            spec_cost = output_table_2.loc[
                output_table_2["Country"] == pays, "capex_ccu_ccus_depleted-oil-gas-reservoirs[EUR/(t/y)]"]
            tem_max = 0
            res = y["ccu_ccus_depleted-oil-gas-reservoirs[Mt]"] * 0
            for t in range(0, 33):
                qq = y["ccu_ccus_depleted-oil-gas-reservoirs[Mt]"][y.index[t]]
                if qq > tem_max:
                    res[t] = spec_cost.values[0] / 30 * (qq - tem_max)
                    tem_max = qq
            output_table.loc[index, "ccu_capex_depleted-oil-gas-reservoirs[MEUR]"] = res

            index = y["ccu_ccus_deep-saline-formation[Mt]"].index
            spec_cost = output_table_2.loc[
                output_table_2["Country"] == pays, "capex_ccu_ccus_deep-saline-formation[EUR/(t/y)]"]
            tem_max = 0
            res = y["ccu_ccus_deep-saline-formation[Mt]"] * 0
            for t in range(0, 33):
                qq = y["ccu_ccus_deep-saline-formation[Mt]"][y.index[t]]
                if qq > tem_max:
                    res[t] = spec_cost.values[0] / 30 * (qq - tem_max)
                    tem_max = qq
            output_table.loc[index, "ccu_capex_saline-aquifers[MEUR]"] = res

        # opex
        spec_opex = output_table_2['opex_ccu_ccus_transport[EUR/t]']
        output_table = output_table.drop("ccu_ccus_hydrogen[TWh]", axis=1)
        spec_opex_all = pd.concat([spec_opex] * 33, ignore_index=True)
        spec_opex_all.index = input_table_1['ccu_ccus_transport[Mt]'].index
        output_table['ccu_opex_transport[MEUR]'] = spec_opex_all * input_table_1['ccu_ccus_transport[Mt]']

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        source_code = model.find(xmlns + "entry[@key='sourceCode']").get('value')
        python_version = model.find(xmlns + "entry[@key='pythonVersionOption']").get('value')
        convert_miss_to_python = model.find(xmlns + "entry[@key='convertMissingToPython']").get('value')
        convert_miss_from_python = model.find(xmlns + "entry[@key='convertMissingFromPython']").get('value')
        sentinel_option = model.find(xmlns + "entry[@key='sentinelOption']").get('value')

        if python_version != "PYTHON3" and python_version != "python3":
            self.logger.error("Error in node " + str(
                self.id) + " : execution of Python 2 code not implemented! Node's xml file is: " + xml_file)

        if convert_miss_to_python != "false":
            self.logger.error("Error in node " + str(
                self.id) + " : conversion of missing values to sentinel values not implemented! Node's xml file is: " + xml_file)

        if convert_miss_from_python != "false":
            self.logger.error("Error in node " + str(
                self.id) + " : conversion of sentinel values to missing values not implemented! Node's xml file is: " + xml_file)

        if sentinel_option != "MIN_VAL":
            self.logger.error("Error in node " + str(
                self.id) + " : sentinel value other than MIN_VAL not implemented! Node's xml file is: " + xml_file)

        source_code = source_code.replace('%%00010', '\n')
        source_code = source_code.replace('%%00009', '\t')
        source_code = source_code.replace('&quot;', '"')
        source_code = source_code + '\n'  # whitespace padding in case the code ends on a loop that requires a final "enter" to be run

        # FIXME We replace (hard code) np.float by float
        source_code = source_code.replace("np.float", "float")

        self = PythonNode.init_wrapper(cls, code=source_code)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, input_table_1, input_table_2) -> pd.DataFrame:
        object_columns_1 = [col for col in input_table_1.select_dtypes(include='object').columns if
                          col not in ["Country", "Years"]]
        for col in object_columns_1:
            input_table_1[col] = input_table_1[col].astype(dtype=float, errors='ignore')

        object_columns_2 = [col for col in input_table_2.select_dtypes(include='object').columns if
                          col not in ["Country", "Years"]]
        for col in object_columns_2:
            input_table_2[col] = input_table_2[col].astype(dtype=float, errors='ignore')

        ns = {'output_table': None, 'input_table_1': input_table_1, 'input_table_2': input_table_2}

        if getattr(self, "id", None) == 'xxx':
            self.temp_code(input_table_1, input_table_2)
        else:
            exec(self.program, ns)

        return ns['output_table']