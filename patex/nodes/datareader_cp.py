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
    DATA READER CP MEATANODE
    ============================
"""
import os
import pickle
import re
import zlib
from timeit import default_timer as timer

import pandas as pd

from patex.nodes.node import Node
from patex.utils import get_size
from patex.utils import reduce_mem_usage


class DataReaderCP(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None, google_sheet_prefix=None):
        self.dataframe = pd.DataFrame()
        self.column = None  # Name of the lever
        self.value = None  # Name of the flow variable containing the value for the lever_selection
        self.local_flow_vars = {}
        self.google_sheet_prefix = google_sheet_prefix
        self.compressed = False
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        self.log_timer(None, 'START', 'DataReader CP metanode')

        # Check code version
        workflow_template_information = self.xml_root.find(self.xmlns + "config[@key='workflow_template_information']")
        if workflow_template_information is None:
            self.logger.error(
                "The tree merge node (" + self.xml_file + ") is not connected to the EUCalc - Tree merge metanode template.")
        else:
            timestamp = workflow_template_information.find(self.xmlns + "entry[@key='timestamp']").get('value')
            if timestamp != '2019-11-13 14:42:16' and timestamp != "2020-04-21 15:37:53":
                self.logger.error(
                    "The template of the EUCalc - DataReader CP metanode was updated. Please check the version that you're using in " + self.xml_file)

        model = self.xml_root.find(self.xmlns + "config[@key='model']")

        #  Set default values of the flow variable parameters:
        self.local_flow_vars['pattern'] = ('STRING', "ots_.*")
        self.local_flow_vars['folder'] = ('STRING', "_interactions/data")

        # Set dictionary of flow_variables
        flow_vars_dict = {"string-input-554": "folder",
                          "string-input-553": "pattern"}

        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
            if child_key in flow_vars_dict:
                self.local_flow_vars[flow_vars_dict[child_key]] = ('STRING', value)

        path = self.local + "/" + self.local_flow_vars['folder'][1] + "/" + self.google_sheet_prefix
        pattern = re.compile(self.local_flow_vars['pattern'][1])
        files = os.listdir(path)
        cp_file = [filename for filename in files if pattern.match(filename)]

        path_file = path + "/" + cp_file[0]
        output_table = pd.read_csv(path_file)
        output_table, NAlist= reduce_mem_usage(output_table)

        if get_size(output_table) > 50000000:  # if file size is greater than 50MB, we compress the dataframe
            self.compressed = True
            self.dataframe = zlib.compress(pickle.dumps(output_table))
        else:
            self.dataframe = output_table
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD", "DataReader CP metanode")

    def run(self):
        start = timer()
        self.log_timer(None, 'START', 'DataReader CP metanode')

        if self.compressed:
            self.save_out_port(1, pickle.loads(zlib.decompress(self.dataframe)))
        else:
            self.save_out_port(1, self.dataframe)

        # logger
        t = timer() - start
        self.log_timer(t, "END",  'DataReader CP metanode')

