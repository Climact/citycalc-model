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
    COLUMN MERGER NODE
    ============================
    KNIME options implemented:
        - All
"""

from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node


class ExcelWriterNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        self.sheetname = ""
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}

    def build_node(self):
        start = timer()
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        xml_filename = model.find(self.xmlns + "entry[@key='filename']")
        filename = xml_filename.get("value")
        filename = filename.replace('.xlsx', '_PY_OUT.xlsx')
        filename = filename.replace("knime://LOCAL", self.local._str)
        xml_sheetname = model.find(self.xmlns + "entry[@key='sheetname']")
        self.sheetname = xml_sheetname.get("value")
        self.writer = pd.ExcelWriter(filename)

    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)
        df.to_excel(self.writer, sheet_name=self.sheetname, index=False)
        self.writer.save()
        # logger
        t = timer() - start
        self.log_timer(t, "END")

        ## Keep SystemExist : it is used in test_module_converter / test_data_processing while comparing python and Excel
        self.logger.critical("System Exit for Debug purpose. Comment this line in the excel_writer_node.py file to avoid system exit")
        raise SystemExit

