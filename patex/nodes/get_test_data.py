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
    GET TEST DATA
    =============
    KNIME options implemented:
        - ALL
"""

from pathlib import Path
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class GetTestData(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.local_flow_vars = {}
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        # Find sectors provided by user
        for child in model:
            for grandchild in child:
                if 'origin-module-' in child.get('key'):
                    value = grandchild.get('value')
                    self.local_flow_vars['origin-module'] = ('STRING', value)
                if 'destination-module-' in child.get('key'):
                    value = grandchild.get('value')
                    self.local_flow_vars['destination-module'] = ('STRING', value)

        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "Get Test Data")

    def run(self):
        start = timer()
        self.log_timer(None, "START", "Get Test Data")

        # Define constants
        ORIGIN = self.local_flow_vars['origin-module'][1]
        DESTINATION = self.local_flow_vars['destination-module'][1]
        GOOGLE_PREFIX = self.flow_vars['google_sheet_prefix'][1]
        WORKSPACE = self.flow_vars['knime.workspace'][1]
        MODULE_DICT = {
            "1.1 Lifestyle": ("lfs", "lifestyle"),
            "1.3 Technology": ("tec", "technology"),
            "1.3 Technology Costs": ("tec_costs", "technology"),
            "1.4 Climate Emissions": ("clt", "climate"),
            "2.1 Buildings": ("bld", "buildings"),
            "2.2 Transport": ("tra", "transport"),
            "3.1a Industry": ("ind", "industry"),
            "3.1b Industry Ammonia": ("amm", "industry"),
            "4.1 Land-use": ("lus", "agriculture"),
            "4.3 Agriculture": ("agr", "agriculture"),
            "4.6 Bioenergy Balance": ("bio", "agriculture"),
            "5.1 Electricity": ("elc", "electricity_supply"),
            "6.3 Air Pollution": ("air", "air_pollution"),
            "Scope 2 and 3": ("sco", "scope_2_3"),
            "4.4 Water": ("wat", "water")
        }

        # Create path to file
        if ORIGIN == "1.3 Technology Costs":
            csv_file = Path(WORKSPACE, MODULE_DICT[ORIGIN][1], "output", GOOGLE_PREFIX, "tec_costs.csv")
        else:
            csv_file = Path(WORKSPACE, MODULE_DICT[ORIGIN][1], "output", GOOGLE_PREFIX, MODULE_DICT[ORIGIN][0] + "_" + MODULE_DICT[DESTINATION][0] + ".csv")

        # Read file
        df = pd.read_csv(csv_file)
        df["Years"] = df["Years"].astype('int')

        # Filter choosen countries (escape of + char for EU27+UK)
        df = df[df['Country'].str.fullmatch("EU28|"+self.flow_vars['country_filter'][1].replace('+', '\+'))]
        # Remove EU27+UK with getting EU27+UK by matching EU27 in regex
        if 'EU27+UK' not in self.flow_vars['country_filter'][1]:
            df = df[df['Country'] != 'EU27+UK']

        output_table = df

        # logger
        t = timer() - start
        self.log_timer(t, "END", "Get Test Data")

        self.save_out_port(1, output_table)
