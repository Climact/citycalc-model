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
    STANDARD METANODE
    ============================
    KNIME options implemented:
        - All
"""

import os
import re
from timeit import default_timer as timer

import networkx as nx
import pandas as pd

from patex.nodes.node import Node
from patex.workflow.workflow_runner import WorkflowRunner


class MetaNode(Node):

    def __init__(self, id, xml_file, node_type, knime_workspace=None, db_host=None, db_port=None, db_user=None,
                 db_password=None, db_schema=None, google_sheet_prefix=None, module_name=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.path_splitted = None
        self.deepness = None
        self.google_sheet_prefix = google_sheet_prefix
        self.module_name = module_name
        super().__init__(id, xml_file, node_type, knime_workspace, self.module_name)

    def init_ports(self):
        self.graph, global_vars = self.init_graph()
        for node_id in self.graph.nodes:
            nx_node = self.graph.nodes[node_id]
            if nx_node['metaconnector']:
                if self.graph.in_degree(node_id) > 0:
                    (from_node_id, to_node_id) = list(self.graph.in_edges(node_id))[0]
                    nx_edge = self.graph.edges[(from_node_id, to_node_id)]
                    connection = nx_edge['connection']
                    self.out_ports[connection.to_port] = None
                elif self.graph.out_degree(node_id) > 0:
                    (from_node_id, to_node_id) = list(self.graph.out_edges(node_id))[0]
                    nx_edge = self.graph.edges[(from_node_id, to_node_id)]
                    connection = nx_edge['connection']
                    self.in_ports[connection.from_port] = None
                else:
                    self.logger.error("Metanode port not connected: {0}".format(node_id))

    def init_graph(self):
        from patex.workflow.workflow_builder import WorkflowBuilder

        # parameters for the logger
        start = timer()
        remove_path = os.path.commonpath([self.local, self.xml_file])
        new_path = self.xml_file.replace(remove_path, '')
        self.path_splitted = re.split('\\\\|/', new_path)
        temp_number = len(self.path_splitted) - 4
        self.deepness = ''.join([char * temp_number for char in '#'])

        workflow_path = self.xml_file.split('workflow.knime')[0]
        mn = WorkflowBuilder(self.xml_file, workflow_path, self.local, db_host=self.db_host, db_port=self.db_port,
                             db_user=self.db_user, db_password=self.db_password, db_schema=self.db_schema,
                             google_sheet_prefix=self.google_sheet_prefix, module_name=self.module_name)
        graph, global_vars = mn.build()

        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " BUILD "+self.path_splitted[-2]," " )
        #self.logger.info(
         #   'Size of Python metanode "{:20s}" in memory: {:12,} bytes'.format(self.path_splitted[-2], get_size(graph)))
        return graph, global_vars

    def build_node(self):
        pass

    def run(self):
        # logger
        start = timer()
        self.log_timer(None, self.deepness + " RUN "+self.path_splitted[-2],"START" )

        port_out = []
        node_out_id = []
        for node_id in self.graph.nodes:
            if 'node_-1_out' in node_id:
                for i in range(0, len(list(self.graph.out_edges(node_id)))):
                    (from_node_id, to_node_id) = list(self.graph.out_edges(node_id))[i]
                    meta_out_port = from_node_id.split('_out_')[1]
                    to_node_in_port = to_node_id.split('_in_')[1]
                    to_node = self.graph.nodes[to_node_id]
                    if isinstance(self.read_in_port(int(meta_out_port)), pd.DataFrame):
                        to_node['node'].load_in_port(int(to_node_in_port), self.read_in_port(int(meta_out_port)).copy())
                    else:
                        to_node['node'].load_in_port(int(to_node_in_port), self.read_in_port(int(meta_out_port)))
                    if to_node['node'].knime_type == "MetaNode":
                        to_node['node'].flow_vars['in_port_' + str(to_node_in_port)] = self.flow_vars[
                            'in_port_' + str(
                                meta_out_port)]
                    else:
                        to_node['node'].flow_vars.update(self.flow_vars['in_port_' + str(meta_out_port)])
                    # try:
                    #     to_node['node'].flow_vars.update(self.flow_vars['in_port_' + str(meta_out_port)])
                    # except KeyError:
                    #     self.logger.warning("The flow variables are not copied from the previous node but from the "
                    #                         "metanode connector. This usually happens when the metanode connector is "
                    #                         "connected directly to a new metanode (xml: " + self.xml_file + ")")
                    #     to_node['node'].flow_vars.update(self.flow_vars)
                # node_to_remove.append(node_id)
            elif 'node_-1_in' in node_id:
                (from_node_id, to_node_id) = list(self.graph.in_edges(node_id))[0]
                if len(list(self.graph.in_edges(node_id))) > 1:
                    raise Exception("There is more than 1 input in the output port of the metanode: " + self.xmlns)
                meta_in_port = to_node_id.split('_in_')[1]
                node_out_id.append(from_node_id)
                port_out.append(meta_in_port)
                # node_to_remove.append(node_id)
        # for this_node in node_to_remove:
        #     # self.graph.remove_node(this_node)
        #     self.graph.nodes[this_node]['node'].was_run = True

        if len(self.in_ports) == 0:
            global_vars = self.flow_vars
        else:
            global_vars = self.flow_vars['in_port_0']

        runner = WorkflowRunner(self.graph, global_vars)

        if self.in_ports == {}:
            # update flow_vars in all root nodes in the metanode with the metanode's flow_vars:
            for node_id in nx.topological_sort(self.graph):
                nx_node = self.graph.nodes[node_id]
                if len(nx.ancestors(self.graph, node_id)) == 0:
                    nx_node['node'].flow_vars.update(self.flow_vars)

        result = runner.run(node_out_id)  # pass node_out_id as arg to make sure these are included in result

        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " RUN "+self.path_splitted[-2],"END" )

        for i in range(len(node_out_id)):
            self.save_out_port(int(port_out[i]), result[node_out_id[i]])
            self.flow_vars['out_port_' + str(i)] = self.graph.nodes[node_out_id[i]]['node'].flow_vars
            self.graph.nodes[node_out_id[i]]['node'].flow_vars = {}  # re-initialization of output_nodes flow_vars

    def update(self):
        # logger
        start = timer()
        self.log_timer(None, self.deepness + " UPDATE "+self.path_splitted[-2], "START")

        port_out = []
        node_out_id = []
        for node_id in self.graph.nodes:
            if 'node_-1_out' in node_id:
                for i in range(0, len(list(self.graph.out_edges(node_id)))):
                    (from_node_id, to_node_id) = list(self.graph.out_edges(node_id))[i]
                    meta_out_port = from_node_id.split('_out_')[1]
                    to_node_in_port = to_node_id.split('_in_')[1]
                    to_node = self.graph.nodes[to_node_id]
                    if isinstance(self.read_in_port(int(meta_out_port)), pd.DataFrame):
                        to_node['node'].load_in_port(int(to_node_in_port), self.read_in_port(int(meta_out_port)).copy())
                    else:
                        to_node['node'].load_in_port(int(to_node_in_port), self.read_in_port(int(meta_out_port)))
                    if to_node['node'].knime_type == "MetaNode":
                        to_node['node'].flow_vars['in_port_' + str(to_node_in_port)] = self.flow_vars[
                            'in_port_' + str(
                                meta_out_port)]  # FIXME: is it really in_port_ ? different in the workflow_runner (out_port_)
                    else:
                        to_node['node'].flow_vars.update(self.flow_vars['in_port_' + str(meta_out_port)])
                    # try:
                    #     to_node['node'].flow_vars.update(self.flow_vars['in_port_' + str(meta_out_port)])
                    # except KeyError:
                    #     self.logger.warning("The flow variables are not copied from the previous node but from the "
                    #                         "metanode connector. This usually happens when the metanode connector is "
                    #                         "connected directly to a new metanode (xml: " + self.xml_file + ")")
                    #     to_node['node'].flow_vars.update(self.flow_vars)
                # node_to_remove.append(node_id)
            elif 'node_-1_in' in node_id:
                (from_node_id, to_node_id) = list(self.graph.in_edges(node_id))[0]
                if len(list(self.graph.in_edges(node_id))) > 1:
                    raise Exception("There is more than 1 input in the output port of the metanode: " + self.xmlns)
                meta_in_port = to_node_id.split('_in_')[1]
                node_out_id.append(from_node_id)
                port_out.append(meta_in_port)
                # node_to_remove.append(node_id)
        # for this_node in node_to_remove:
        #     # self.graph.remove_node(this_node)
        #     self.graph.nodes[this_node]['node'].was_run = True

        if len(self.in_ports) == 0:
            global_vars = self.flow_vars
        else:
            global_vars = self.flow_vars['in_port_0']

        runner = WorkflowRunner(self.graph, global_vars)

        if self.in_ports == {}:
            # update flow_vars in all root nodes in the metanode with the metanode's flow_vars:
            for node_id in nx.topological_sort(self.graph):
                nx_node = self.graph.nodes[node_id]
                if len(nx.ancestors(self.graph, node_id)) == 0:
                    nx_node['node'].flow_vars.update(self.flow_vars)

        result = runner.run(node_out_id, update=True)  # pass node_out_id as arg to make sure these are included in result

        # logger
        t = timer() - start
        self.log_timer(t, self.deepness + " UPDATE "+self.path_splitted[-2],"END" )

        for i in range(len(node_out_id)):
            self.save_out_port(int(port_out[i]), result[node_out_id[i]])
            self.flow_vars['out_port_' + str(i)] = self.graph.nodes[node_out_id[i]]['node'].flow_vars
            self.graph.nodes[node_out_id[i]]['node'].flow_vars = {}  # re-initialization of output_nodes flow_vars
