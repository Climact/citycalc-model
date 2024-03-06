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
    SELECT COUNTRY
    ==============
    KNIME options implemented:
        - ALL
"""

import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, SubNode, FlowVars


class SelectCountry(PythonNode, SubNode):
    def __init__(
        self,
        country_filter: str,
    ):
        super().__init__()
        self.country_filter = country_filter

    def init_ports(self):
        self.out_ports = {1: pd.DataFrame()}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")

        # Find sectors provided by user
        rx = re.compile("[0-9]+")
        for child in model:
            for grandchild in child:
                if 'country-selection-' in child.get('key'):
                    list_countries = []
                    for ggrandchild in grandchild:
                        if rx.match(ggrandchild.get('key')):
                            list_countries.append(ggrandchild.get('value'))
                    countries = "EU28|" + '|'.join(list_countries)
                    countries = countries.replace('All Countries', '.*')
                    country_filter = countries

        self = PythonNode.init_wrapper(cls, country_filter=country_filter)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, flow_vars: "FlowVars") -> "FlowVars":
        if 'country_filter' not in flow_vars.keys():
            flow_vars['country_filter'] = self.country_filter
        return flow_vars
