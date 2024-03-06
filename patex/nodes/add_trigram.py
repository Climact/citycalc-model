# ----------------------------------------------------------------------------------------------------- #
# 2021, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    ADD TRIGRAM
    ===========
    KNIME options implemented:
        - ALL
"""

import pandas as pd

from patex.nodes.node import PythonNode, SubNode, FlowStr


class AddTrigram(PythonNode, SubNode):
    def __init__(self):
        super().__init__()

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}
        self.knime_input_mapping["module_name"] = "module-name"

    def apply(self, df, module_name: "FlowStr") -> pd.DataFrame:
        # Find variables
        dimensions = list(df.select_dtypes(['int', 'object']).columns)
        variables = [c for c in df.columns if c not in dimensions]
        df = df[dimensions + variables]

        # Rename variables
        if module_name == "lifestyle":
            trigram = "lfs_"
        elif module_name == "climate":
            trigram = "clm_"
        elif module_name == "technology":
            trigram = "tec_"
        elif module_name == "air_quality":
            trigram = "air_"
        elif module_name == "climate emissions":
            trigram = "clt_"
        elif module_name == "buildings":
            trigram = "bld_"
        elif module_name == "transport":
            trigram = "tra_"
        elif module_name == "industry":
            trigram = "ind_"
        elif module_name == "ammonia":
            trigram = "amm_"
        elif module_name == "power":
            trigram = "pow_"
        elif module_name == "land use":
            trigram = "lus_"
        elif module_name == "minerals":
            trigram = "min_"
        elif module_name == "agriculture":
            trigram = "agr_"
        elif module_name == "bioenergy balance":
            trigram = "bio_"
        elif module_name == "electricity_supply":
            trigram = "elc_"
        elif module_name == "scope_2_3":
            trigram = "sco_"
        elif module_name == "water":
            trigram = "wat_"
        else:
            msg = f"No trigram for following module-name: {module_name}."
            raise RuntimeError(msg)

        variables = [trigram + c for c in variables]

        # Create output
        df.columns = dimensions + variables
        return df


if __name__ == '__main__':
    id = '8253'
    relative_path = f'/Users/climact/patex-container/dev/lifestyle/workflows/lifestyle_processing/1_1 Lifestyl (#0)/Add Trigram (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = AddTrigram(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_run_pycharm.csv', index=False)
    node.update()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_update_pycharm.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_update_pycharm.csv'),
                                  pd.read_csv('/Users/climact/Desktop/out_run_pycharm.csv'), )
