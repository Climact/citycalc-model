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
    COLUMN COMBINER NODE
    ============================
    KNIME options implemented:
        - None
"""
from patex.nodes.node import Node

class ColumnCombinerNode(Node):
    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None, 2: None}
        self.out_ports = {1: None}

    def build_node(self):
        raise Exception("Column Combiner Node not implemented (cfr. xml file : " + str(self.xml_file))



    def run(self):
        raise Exception("Column Combiner Node not implemented (cfr. xml file : " + str(self.xml_file))
        ## Need to be reviewer
        # id = str(self.id) + " " + self.xml_file
        # df_top = self.read_in_port(1)
        # df_bottom = self.read_in_port(2)
        # model = self.xml_root.find(self.xmlns + "config[@key='model']")
        # fail_execution = model.find(self.xmlns + "entry[@key='fail_on_duplicates']").get('value')
        # append_suffix = model.find(self.xmlns + "entry[@key='append_suffix']").get('value')
        # suffix = model.find(self.xmlns + "entry[@key='suffix']").get('value')
        # intersection = model.find(self.xmlns + "entry[@key='intersection_of_columns']").get('value')
        # enable_hiliting = model.find(self.xmlns + "entry[@key='enable_hiliting']").get('value')
        # if enable_hiliting == 'true':
        #     raise Exception("Hiliting has been enable for column concatenate (Concatenate node #" + id +", .xml file "+ str(self.xml_file)+")")
        #
        # columns_top = df_top.columns
        # columns_bottom = df_bottom.columns
        #
        # inter = [val for val in columns_top if val in columns_bottom]
        # union = list(set().union(columns_top, columns_bottom))
        #
        # if fail_execution == 'true':
        #     raise Exception(
        #         'Fail execution on row id handling has not been implemented for column concatenation ( Error occurred in Concatenate node #' + id +', .xml file '+ str(self.xml_file)+')')
        # elif append_suffix == 'true':
        #     self.logger.warning(
        #         "WARNING: Nothing is appended to rowID handling because we don't use the row ID in the converter (Concatenate node #" + id +", .xml file "+ str(self.xml_file)+")")
        #     if intersection == 'true':
        #         df = pd.concat([df_top, df_bottom], join='inner',
        #                        sort=False)  # This may cause an error if pandas version is not up to date (v0.23 and beyond)
        #     else:
        #         df = pd.concat([df_top, df_bottom], join='outer',
        #                        sort=False)  # This may cause an error if pandas version is not up to date (v0.23 and beyond)
        # else:
        #     if intersection == 'true':
        #         df = df_top[inter]
        #     else:
        #         df = pd.DataFrame()
        #         for column in union:
        #             if column in columns_top:
        #                 df[column] = df_top[column].values
        #             elif column in columns_bottom:
        #                 df[column] = np.nan

        # self.save_out_port(1, self.read_in_port(1))

