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
    SORTER NODE
    ============================
    KNIME options implemented:
        - Add new columns for sorting.
    KNIME Options NOT implemented:
        - 'Sort in memory', 'Move Missing Cells to end of sorted list'
    ATTENTION POINT :
        - it is not possible to control which type of data are sorted.
"""

from timeit import default_timer as timer

from patex.nodes.node import Node


class SorterNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None):
        super().__init__(id, xml_file, node_type, knime_workspace)

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    def build_node(self):
        start = timer()
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        incllist = model.find(self.xmlns + "config[@key='incllist']")
        sortOrder = model.find(self.xmlns + "config[@key='sortOrder']")
        totalNrbCol = incllist.find(self.xmlns + "entry[@key='array-size']").get('value')

        if model.find(self.xmlns + "entry[@key='sortinmemory']").get('value') == 'true':
            self.logger.debug(
                "The option 'Sort in memory' has been checked in the Sorter node (Cfr. xml file : " + str(
                    self.xml_file) + ") but this is not implemented in the Python converter")
        if model.find(self.xmlns + "entry[@key='missingToEnd']").get('value') == 'true':
            raise Exception(
                "The option 'Move Missing Cells to end of sorted list' has been checked in the Sorter node (Cfr. xml file : " + str(
                    self.xml_file) + ") but this is not implemented in the Python converter")

        self.colNameList = []
        self.ascendingList = []
        for i in range(0,int(totalNrbCol)):
            colName = incllist.find(self.xmlns + "entry[@key='"+str(i)+"']").get('value')
            self.colNameList.append(colName)
            ascending = sortOrder.find(self.xmlns + "entry[@key='"+str(i)+"']").get('value')
            self.ascendingList.append(ascending == "true")
        # logger
        t = timer() - start
        self.log_timer(t, "BUILD")


    def run(self):
        start = timer()
        self.log_timer(None, 'START')

        df = self.read_in_port(1)

        # self.logger.debug("This is column names list of the Sorter node (xml file : " + str(
        #             self.xml_file) + ") : " + str(colNameList))
        # self.logger.debug("This is ascending list of the Sorter node (xml file : " + str(
        #             self.xml_file) + ") : " + str(ascendingList))
        df1 = df.sort_values(self.colNameList, ascending=self.ascendingList,na_position="first")

        # logger
        t = timer() - start
        self.log_timer(t, "END")

        self.save_out_port(1, df1)


