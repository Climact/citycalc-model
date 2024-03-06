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
    WRAPPED METANODE
    ============================
    KNIME options implemented:
        - None to implement, all the options are in Wrapped node input and wrapped note output.
                Quickform flow variable creation from wrapped metanode's configuration dialog not implemented yet.
"""

import os
import re
from timeit import default_timer as timer

from patex.nodes.node import Node
from patex.workflow.workflow_runner import WorkflowRunner


class WrappedMetaNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None, db_host=None, db_port=None, db_user=None,
                 db_password=None, db_schema=None, google_sheet_prefix=None, module_name=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.path_splitted = None
        self.deepness = None
        self.runner = None
        self.google_sheet_prefix = google_sheet_prefix
        self.module_name = module_name
        super().__init__(id, xml_file, node_type, knime_workspace, self.module_name)

    def init_ports(self):
        self.graph, global_vars = self.init_graph()

        # self.graph = graph
        for node_id in self.graph.nodes:
            nx_node = self.graph.nodes[node_id]
            try:
                if nx_node['metaconnector']:
                    if self.graph.in_degree(node_id) > 0:
                        (from_node_id, to_node_id) = list(self.graph.in_edges(node_id))[0]
                        nx_edge = self.graph.edges[(from_node_id, to_node_id)]
                        connection = nx_edge['connection']
                        self.out_ports[connection.to_port] = None
                        self.graph.nodes[node_id]['node'].supernode = self
                    elif self.graph.out_degree(node_id) > 0:
                        (from_node_id, to_node_id) = list(self.graph.out_edges(node_id))[0]
                        nx_edge = self.graph.edges[(from_node_id, to_node_id)]
                        connection = nx_edge['connection']
                        self.in_ports[connection.from_port] = None
                        self.graph.nodes[node_id]['node'].supernode = self
                    else:
                        # self.logger.warning('Metanode port not connected: ' + node_id + '. File: ' + self.xml_file)
                        self.logger.error("Metanode port not connected: {0}".format(node_id) + ". File: " + self.xml_file)
            except KeyError as e:
                self.logger.error(e)

    def init_graph(self):
        from patex.workflow.workflow_builder import WorkflowBuilder

        # Parameters for the logger
        start = timer()
        remove_path = os.path.commonpath([self.local, self.xml_file])
        new_path = self.xml_file.replace(remove_path, '')
        self.path_splitted = re.split('\\\\|/', new_path)
        temp_number = len(self.path_splitted) - 4
        self.deepness = ''.join([char * temp_number for char in '#'])

        # Get module name if any
        model = self.xml_root.find(self.xmlns + "config[@key='model']")
        for child in model:
            child_key = child.get('key')
            for grandchild in child:
                value = grandchild.get('value')
                if child_key.find("string-input") is not None:
                    self.module_name = value

        # Get other values
        workflow_path = self.xml_file.split('/settings')[0]
        workflow_xml_path = os.path.join(workflow_path, 'workflow.knime')
        mn = WorkflowBuilder(workflow_xml_path, workflow_path, self.local, db_host=self.db_host, db_port=self.db_port,
                             db_user=self.db_user, db_password=self.db_password, db_schema=self.db_schema,
                             google_sheet_prefix=self.google_sheet_prefix, module_name=self.module_name)
        graph, global_vars = mn.build()

        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " BUILD "+self.path_splitted[-2]," " )
        #self.logger.info(
        #    'Size of Python metanode "{:20s}" in memory: {:12,} bytes'.format(self.path_splitted[-2], get_size(graph)))
        return graph, global_vars

    def build_node(self):
        workflow_path = self.xml_file.split('/settings')[0]
        workflow_xml_path = os.path.join(workflow_path, 'workflow.knime')
        #pass the global variable to all the nodes inside a metanode
        self.runner = WorkflowRunner(self.graph)

    def run(self):
        # logger
        start = timer()
        self.log_timer(None, self.deepness + " RUN " + self.path_splitted[-2], "START")

        self.runner.run()
        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " RUN "+self.path_splitted[-2],"END" )

    def update(self):
        # logger
        start = timer()
        self.log_timer(None, self.deepness + " UPDATE " + self.path_splitted[-2], "START")

        self.runner.run(update=True)
        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " UPDATE "+self.path_splitted[-2],"END" )
