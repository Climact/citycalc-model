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
    WORKFLOW BUILDER
    ============================
"""

import logging
import os
import xml.etree.cElementTree as ET

import networkx as nx

from patex.nodes.connection import Connection
from patex.nodes.node_builder import KnimeNodeBuilder
from patex.nodes.node import Context


class WorkflowBuilder(object):
    def __init__(self, knime_workflow_path, knime_workflow_folder_path, knime_workspace=None, db_host=None,
                 db_port=None, db_user=None, db_password=None, db_schema=None, google_sheet_prefix=None,
                 baseyear='2021', module_name=None):
        self.ctx = Context(knime_workspace, module_name=module_name)
        self.knime_workflow_folder_path = knime_workflow_folder_path
        self.knime_workflow_path = knime_workflow_path
        if knime_workspace is None:
            # TODO: historical way of specifying the path to the workspace in the tests. Should be removed (and tested
            #  with the unit tests)
            self.knime_workspace = os.path.join('C:\\EUCalc', 'knime2python')
        else:
            self.knime_workspace = knime_workspace
        self.xmlns = "{http://www.knime.org/2008/09/XMLConfig}"
        self.logger = logging.getLogger(__name__)
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.google_sheet_prefix = google_sheet_prefix
        self.baseyear = baseyear
        self.module_name = module_name

    def build(self):
        """ Build a directed graph based on the Knime workflow specified by the workflow path.
        Each Knime node is split into "in ports" and "out ports" that form the nodes of the directed graph while the
        edges are the links between those ports.

        :return: directed graph and flow variables associated to it
        """
        tree = ET.ElementTree(file=self.knime_workflow_path)
        root = tree.getroot()
        knime_nodes = self.build_knime_nodes(root)
        knime_connections = self.build_knime_connections(root, knime_nodes)
        graph = self.build_graph(knime_nodes, knime_connections)
        flow_vars = self.get_global_vars()

        return graph, flow_vars

    def quickscan(self):
        """ Ligth version of the build function to build the nodes without building the graph

        :return: None
        """
        tree = ET.ElementTree(file=self.knime_workflow_path)
        root = tree.getroot()
        self.build_knime_nodes(root)

    def build_knime_nodes(self, root):
        """ Walk through the Knime workflow and create a python representation of each Knime node.

        :param root: xml root
        :return: List of python representation of Knime nodes
        """
        xml_nodes = root.find(self.xmlns + "config[@key='nodes']").findall(self.xmlns + "config")
        builder = KnimeNodeBuilder(self.db_host, self.db_port, self.db_user, self.db_password, self.db_schema,
                                   self.google_sheet_prefix, self.baseyear, self.module_name)
        knime_nodes = {}
        for xml_node in xml_nodes:
            id = xml_node.find(self.xmlns + "entry[@key='id']").get('value')
            node_settings_file = xml_node.find(self.xmlns + "entry[@key='node_settings_file']").get('value')
            node_is_meta = xml_node.find(self.xmlns + "entry[@key='node_is_meta']").get('value')
            node_type = xml_node.find(self.xmlns + "entry[@key='node_type']").get('value')
            filepath = os.path.join(self.knime_workflow_folder_path, node_settings_file)
            node = builder.build(self.ctx, id, filepath, node_settings_file, node_is_meta, node_type, self.knime_workspace)
            node._xml_file_path = filepath
            knime_nodes[id] = node
        return knime_nodes

    def build_knime_connections(self, root, knime_nodes):
        """ Walk through the Knime workflow and create a representation of the links between the in/out
        ports of the Knime nodes.

        :param root: xml root
        :param knime_nodes: list of python representation of Knime nodes
        :return: list of connections between in/out ports
        """
        knime_connections = []
        xml_connections = root.find(self.xmlns + "config[@key='connections']").findall(self.xmlns + "config")
        for xml_connection in xml_connections:
            source_id = int(xml_connection.find(self.xmlns + "entry[@key='sourceID']").get('value'))
            dest_id = int(xml_connection.find(self.xmlns + "entry[@key='destID']").get('value'))
            source_port = int(xml_connection.find(self.xmlns + "entry[@key='sourcePort']").get('value'))
            dest_port = int(xml_connection.find(self.xmlns + "entry[@key='destPort']").get('value'))
            # Initialisation of out port for a node with specific id
            if source_port == 0 and source_id != -1:
                # self.logger.debug("Creating output_port_0 on node : " + str(source_id))
                knime_nodes[str(source_id)].create_0_out_port()
            # Initialisation of in port for a node with specific id
            if dest_port == 0 and dest_id != -1:
                # self.logger.debug("Creating input_port_0 on node : " + str(dest_id))
                knime_nodes[str(dest_id)].create_0_in_port()
            knime_connections.append(Connection(source_id, source_port, dest_id, dest_port))
        return knime_connections

    def build_graph(self, knime_nodes, knime_connections):
        """ Build the directed graph with the list of nodes and connections from previous functions

        :param knime_nodes: list of nodes
        :param knime_connections: list of connections
        :return: directed graph
        """
        G = nx.DiGraph()
        builder = KnimeNodeBuilder(self.db_host, self.db_port, self.db_user, self.db_password, self.db_schema)
        # Create connections of ports inside a node.
        # e.g. if a node have 2 in ports and 1 out port, 2 connections will be created:
        # in_port_1 to out_port_1
        # in_port_2 to out_port_1
        # => the ports of the Knime workflow are the nodes of the directed graph in the python converter with a
        # distinction between the input ports (copying data from previous output port) and the output ports
        # (waiting for the input ports to be filled before doing the calculations dedicated to the Knime node)
        for (knime_node_id, knime_node) in knime_nodes.items():
            # The WrappedNode are converted to Component starting from Knime 4.0.
            # Keep the 2 conditions to allow the converter to work with different releases of Knime
            if knime_node.knime_name == 'WrappedNode Input' or knime_node.knime_name == "Component Input":
                meta_connector = True
            elif knime_node.knime_name == 'WrappedNode Output' or knime_node.knime_name == "Component Output":
                meta_connector = True
            else:
                meta_connector = False

            # Add input ports as "copying" nodes in the directed graph
            for in_id in knime_node.in_ports.keys():
                in_name = "node_{0}_in_{1}".format(knime_node_id, in_id)
                # self.logger.debug("adding in_port {0} to graph".format(in_name))
                G.add_node(in_name, node_type="copy_data", node=knime_node, metaconnector=meta_connector)
            # Add output ports as "running" nodes in the directed graph
            for out_id in knime_node.out_ports.keys():
                out_name = "node_{0}_out_{1}".format(knime_node_id, out_id)
                # self.logger.debug("adding out_port {0} to graph".format(out_name))
                G.add_node(out_name, node_type="run_node", node=knime_node, metaconnector=meta_connector)
            # Add edges between the input and the output ports of a node
            for in_id in knime_node.in_ports.keys():  # edges between ports inside a Knime node
                for out_id in knime_node.out_ports.keys():
                    in_name = "node_{0}_in_{1}".format(knime_node_id, in_id)
                    out_name = "node_{0}_out_{1}".format(knime_node_id, out_id)
                    G.add_edge(in_name, out_name)

        # FIXME: raise exception if port number = 0 (the port number 0 is only used for flow variables except for
        #  standard metanodes that use the port number 0 as first number)
        for c in knime_connections:
            from_name = "node_{0}_out_{1}".format(c.from_node, c.from_port)
            to_name = "node_{0}_in_{1}".format(c.to_node, c.to_port)
            # self.logger.debug("adding edge from {0} to {1}".format(from_name, to_name))
            if c.from_node == -1:  # if we are dealing with a standard metanode, the number of the node is '-1'
                node_id = c.from_node
                filepath = None
                node_settings_file = 'Metaconnector'
                node_is_meta = 'true'
                node_type = "TempNode"
                node = builder.build(self.ctx, node_id, filepath, node_settings_file, node_is_meta, node_type, self.knime_workspace)
                G.add_node(from_name, node_type="run_node", node=node, metaconnector=True)
            elif c.to_node == -1:
                node_id = c.to_node
                filepath = None
                node_settings_file = 'Metaconnector'
                node_is_meta = 'true'
                node_type = "TempNode"
                node = builder.build(self.ctx, node_id, filepath, node_settings_file, node_is_meta, node_type, self.knime_workspace)
                G.add_node(to_name, node_type="copy_data", node=node, metaconnector=True)
            G.add_edge(from_name, to_name, connection=c)

        return G

    def get_global_vars(self):
        """ Find and store all the global flow variables

        :return: flow variables
        """
        tree = ET.ElementTree(file=self.knime_workflow_path)
        root = tree.getroot()
        workflow_vars = root.find(self.xmlns + "config[@key='workflow_variables']")

        global_vars = {'knime.workspace': ("STRING", self.knime_workspace)}
        if workflow_vars is not None:
            xml_global_vars = workflow_vars.findall(self.xmlns + "config")

            for xml_global_var in xml_global_vars:
                var_name = xml_global_var.find(self.xmlns + "entry[@key='name']").get('value')
                var_class = xml_global_var.find(self.xmlns + "entry[@key='class']").get('value')
                var_value = xml_global_var.find(self.xmlns + "entry[@key='value']").get('value')
                global_vars[var_name] = (var_class, var_value)

        return global_vars
