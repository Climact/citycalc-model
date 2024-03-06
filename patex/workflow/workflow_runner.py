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
    WORKFLOW RUNNER
    ===============
"""
import logging
from timeit import default_timer as timer

import networkx as nx
import pandas as pd

from patex.nodes.node import PythonNode


class WorkflowRunner(object):
    def __init__(self, graph, global_vars=None):
        self.start = timer()
        if global_vars is None:
            global_vars = {}
        self.graph = graph
        self.xmlns = "{http://www.knime.org/2008/09/XMLConfig}"
        self.logger = logging.getLogger(__name__)
        self.global_vars = global_vars
        self.num_ancestors = {}
        self.num_descendants = {}
        for node_id in nx.topological_sort(self.graph):
            self.num_ancestors[node_id] = len(nx.ancestors(self.graph, node_id))
            self.num_descendants[node_id] = len(nx.descendants(self.graph, node_id))

    def run(self, output_nodes=None, reset_after_run='HARD', update=False, global_vars=None):
        """ Main function of the workflow runner. Determine how to run the converter after building the graph

        :param output_nodes: list of output nodes where to collect the dataframes that need to be returned
        :param reset_after_run: hard reset of the node after run (set to None)
        :param update: boolean (True: use the 'update' capabilities of the nodes, False: only use the 'run'). CAREFULL: the update boolean must be set to FALSE for the first RUN of the model
        :param global_vars: list of global variables
        :return: list of dataframes from output_nodes
        """
        if global_vars is not None:
            self.global_vars = global_vars
        # includes the outputs of these nodes in output
        return self.sequential_run(output_nodes, reset_after_run, update)

    def sequential_run(self, output_nodes=None, reset_after_run='HARD', update=False):
        """ Run the nodes sequentially one by one

        :param output_nodes: list of output nodes where to collect the dataframes that need to be returned
        :param reset_after_run: hard reset of the node after run (set to 0)
        :param update: boolean (True: use the 'update' capabilities of the nodes, False: only use the 'run')
        :return: list of dataframes from output_nodes
        """
        output = {}
        try:
            # Run node one by one following topological order
            for node_id in nx.topological_sort(self.graph):
                self.activate_node(node_id, output, output_nodes, update)
            self.reset_graph(output_nodes, reset_after_run)
            return output
        except Exception as e:
            self.reset_graph(output_nodes)
            raise e

    def activate_node(self, node_id, output, output_nodes, update=False):
        """ Activate a specific node means either
        - Running the node (using update function if available and enabled with the 'update' boolean) => output port of
        the Knime workflow
        - Copying data from the previous node => input port of the Knime workflow

        :param node_id: id of the node (number)
        :param output: dictionary to store the output dataframes
        :param output_nodes: list of output_nodes to save in the output dictionary
        :param update: boolean (use the update function ?)
        :return: None
        """
        nx_node = self.graph.nodes[node_id]
        if self.num_ancestors[node_id] == 0:
            # all root nodes are init with access to the global vars
            nx_node['node'].flow_vars.update(self.global_vars)

        if nx_node == {}:
            raise Exception("Empty " + str(node_id) + ": edge " + str(list(self.graph.in_edges(node_id))[
                                                                          0]))  # + ". This is usually caused by unconnected node (in or out)" + ". File: " + self.xml_file)

        if nx_node['node_type'] == "run_node" and nx_node['node'].knime_type != 'TempNode':
            self.run_node(node_id, nx_node, output, output_nodes, update)

        elif nx_node['node_type'] == "copy_data" and nx_node['node'].knime_type != 'TempNode':
            self.copy_node(node_id, nx_node, output, update)

    def run_node(self, node_id, nx_node, output, output_nodes, update):
        """ Run (or update) a specific node after all the ancestors run

        :param node_id: node id (number)
        :param nx_node: dictionary of the node
        :param output: output dictionary to store the output dataframes
        :param output_nodes: list of output nodes to save in the output dictionary
        :param update: boolean
        :return: None
        """
        # Avoid running multiple times the same node (is useless in topological order but important in parallel
        # runs)
        if not nx_node['node'].was_run:
            # self.logger.debug("Running node " + nx_node['node'].id +" ("+ nx_node['node'].xml_file+")")
            if not update:
                # The flow variables are modifying the way we read the xml so we need to prepare the node
                # for run if there is flow variables impacting the node parameters
                self.prepare_node_for_run(nx_node['node'])
                nx_node['node'].run()
            elif hasattr(nx_node['node'], 'update'):
                nx_node['node'].update()
            else:
                nx_node['node'].run()
            nx_node['node'].was_run = True  # So we know what nodes have been run and don't run them again
        if self.num_descendants[node_id] == 0:
            # The node is at the end of the workflow so we save the data in the output dictionary
            temp_unused, out_port = node_id.split('_out_')
            output[node_id] = nx_node['node'].read_out_port(int(out_port))
        if output_nodes is not None:
            if node_id in output_nodes:
                # The node id is present in the output_nodes list so we save the data in the output dictionary
                temp_unused, out_port = node_id.split('_out_')
                output[node_id] = nx_node['node'].read_out_port(int(out_port))

    def copy_node(self, node_id, nx_node, output, update):
        """ Copy data from the previous node (= output port of the previous Knime node)

        :param node_id: node id (number)
        :param nx_node: definition of the node (dictionary)
        :param output: output dictionary to store the output dataframes
        :param update: boolean
        :return: None
        """
        nx_node_id = self.graph.nodes[node_id]
        port_to_load = node_id.split('_in_')[1]
        # Assure that the port is empty (except if we are updating the nodes)
        if nx_node_id['node'].in_ports[int(port_to_load)] is None:
            if list(self.graph.in_edges(node_id)):  # Makes sure there is actually data to be copied in
                (from_node_id, to_node_id) = list(self.graph.in_edges(node_id))[0]
                nx_edge = self.graph.edges[(from_node_id, to_node_id)]
                connection = nx_edge['connection']
                nx_from_node = self.graph.nodes[from_node_id]
                nx_to_node = self.graph.nodes[to_node_id]
                from_node = nx_from_node['node']
                to_node = nx_to_node['node']

                if isinstance(from_node.read_out_port(connection.from_port),
                              pd.DataFrame) and self.graph.out_degree(from_node_id) > 1:
                    df_to_be_input = from_node.read_out_port(connection.from_port).copy()
                    # self.logger.info(
                    #     "  Hard copy from {}, port {}. Source shape: {}x{}".format(nx_from_node['node'].id,
                    #                                                                connection.from_port,
                    #                                                                df_to_be_input.shape[0],
                    #                                                                df_to_be_input.shape[1]))
                else:
                    df_to_be_input = from_node.read_out_port(connection.from_port)
                # self.logger.info(
                #     "  Source shape: {}x{}".format(df_to_be_input.shape[0], df_to_be_input.shape[1]))

                # FIXME : In all the following, one should make sure there are no conflicts when
                #  updating flow_vars since: e.g node 1 and node 2 could both be sending var_0
                #  to node 3, and one should know which of their values to keep! Current
                #  implementation just picks whichever is last in execution flow. But should pick
                #  the top value, like KNIME.

                # the 'Flow variable ports' in Knime are the port 0 or the nodes. We don't need to copy the
                # dataframe from those ports as we are only copying the flow variables
                if (connection.from_port != 0 or from_node.knime_type == "MetaNode") and (
                        connection.to_port != 0 or to_node.knime_type == "MetaNode"):
                    # No point copying data to or from flow variable ports, hence the if condition!
                    to_node.load_in_port(connection.to_port, df_to_be_input)

                #  Metanodes store different flow variable dictionaries per port,
                #  so their flow_vars look a little different:
                if from_node.knime_type == "MetaNode":
                    if to_node.knime_type == "MetaNode":
                        to_node.flow_vars['in_port_' + str(connection.to_port)] = from_node.flow_vars[
                            'out_port_' + str(connection.from_port)]
                    else:
                        to_node.flow_vars.update(
                            from_node.flow_vars['out_port_' + str(connection.from_port)])
                else:
                    if to_node.knime_type == "MetaNode":
                        to_node.flow_vars['in_port_' + str(connection.to_port)] = from_node.flow_vars
                    else:
                       to_node.flow_vars.update(from_node.flow_vars)

        # Node has no descendants: the copy_data node must run after copying the data
        if self.num_descendants[node_id] == 0:
            # ---> But only if all the input ports are loaded!
            all_in_ports_are_loaded = True

            for in_port in nx_node['node'].in_ports:
                if nx_node['node'].in_ports[in_port] is None:
                    all_in_ports_are_loaded = False

            if all_in_ports_are_loaded:
                # self.logger.debug("Running node " + nx_node['node'].id + " because it has no descendants")
                # self.prepare_node_for_run(nx_node['node'])
                if update and hasattr(nx_node['node'], 'update'):
                    nx_node['node'].update()
                else:
                    nx_node['node'].run()

                nx_node['node'].was_run = True
                # self.logger.debug("Flow variables available to node " + nx_node['node'].id + " are: " + str(
                #     nx_node['node'].flow_vars))
                output[node_id] = nx_node['node'].read_in_port(1)
            else:
                self.logger.debug(
                    self.graph.nodes[node_id]["node"].knime_name + "(#" + self.graph.nodes[node_id][
                        "node"].id + "): No descendants - Waiting for input to run")

    def iterate_graph(self, graph):
        """ This function (not used currently) is built to iterate over the directed graph and log the list of
        nodes and connections

        :param graph: directed graph
        :return: None
        """
        for node in graph.nodes():
            predecessors = list(graph.predecessors(node))
            successors = list(graph.successors(node))
            if len(predecessors) == 0:
                self.logger.debug(node + " is a root node")
            else:
                self.logger.debug(node + " has predecessors: " + predecessors)

            if len(successors) == 0:
                self.logger.debug(node + " is a final node".format(node))
            else:
                self.logger.debug(node + " has successors: " + successors)

    def reset_graph(self, output_nodes, reset_type='HARD'):
        """ Set the values of the nodes to None

        :param output_nodes: list of output nodes
        :param reset_type: type of reset
        :return: None
        """
        for node_id, nx_node in self.graph.nodes.items():
            self.reset_node(node_id,reset_type)
            temp_name = 'node_' + str(nx_node['node'].id)
            if output_nodes is None or not any(temp_name in s for s in output_nodes) and reset_type=='HARD':
                # don't reset the flow vars of the output_nodes because there are used in the meta_node.py
                # run to pass the flow variables to the following nodes
                nx_node['node'].flow_vars = {}

    def reset_node(self, node_id, reset_type='HARD'):
        """ Set the value of a node to None

        :param node_id: node id (number)
        :param reset_type: type of reset
        :return: None
        """
        nx_node = self.graph.nodes[node_id]
        node_port = int(node_id.split("_")[-1])
        node_dataio = node_id.split("_")[-2]
        if reset_type == 'HARD':
            nx_node['node'].reset_ports(node_port, node_dataio)
        nx_node['node'].was_run = False

    def prepare_node_for_run(self, node):
        """Checks if node is controlled by any flow variables, if so,
            the xml settings tree is modified so the controlled parameters values are set by the
            flow variables

        :param node: node dictionary
        :return: None
        """

        if isinstance(node, PythonNode):
            # This is handled in `PythonNode` itself
            return

        variables = node.xml_root.find(self.xmlns + "config[@key='variables']")
        if variables is not None:
            node.uses_flow_vars = True
            self.logger.debug("The node #" + str(
                node.id) + " (" + node.knime_name + ") is using Flow Variables to control the parameters")

            used_variables = []
            self.find_used_variables(variables, "", used_variables)

            model_in_settings_file = node.xml_root.find(self.xmlns + "config[@key='model']")

            for used_variable in used_variables:
                (path, var) = used_variable
                keys = path.split('/')
                keys.pop(0)  # get rid of the useless '' entry at index 0
                if 'tree' in keys:
                    keys.remove('tree')
                parameter_to_modify = model_in_settings_file

                for key in keys:
                    if parameter_to_modify.find(self.xmlns + "config[@key='" + key + "']") is not None:
                        parameter_to_modify = parameter_to_modify.find(self.xmlns + "config[@key='" + key + "']")
                    else:
                        parameter_to_modify = parameter_to_modify.find(self.xmlns + "entry[@key='" + key + "']")

                if var in node.flow_vars:
                    parameter_to_modify.attrib["value"] = node.flow_vars[var][1]
                else:
                    self.logger.error("Node " + str(
                        node.id) + " requires flow variable " + var + " which does not exist (in its scope at least)")

    def find_used_variables(self, subtree, path, used_variables):
        """Recursively reads the xml subtree, finds all the flow variables in use, returns
            a list containing the names of the variables and their paths in the xml tree

        :param subtree:
        :param path:
        :param used_variables:
        :return:
        """
        if subtree.get('key') != "used_variable":
            if subtree.get('key') != "variables":
                path = path + "/" + subtree.get('key')

            for child in subtree:
                # print(ET.tostring(child, encoding='utf8').decode('utf8'))
                self.find_used_variables(child, path, used_variables)

        else:
            used_variables.append((path, subtree.get('value')))
