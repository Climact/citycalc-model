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

import os
import xml.etree.cElementTree as ET

from patex.nodes.add_missing_years import AddMissingYearsNode
from patex.nodes.add_specific_trigram import AddSpecificTrigram
from patex.nodes.add_trigram import AddTrigram
from patex.nodes.api_interface_metanode import APIinterfaceNode
from patex.nodes.assemble_all_output_metanode import AssembleAllOutputNode
from patex.nodes.build_cube_format_metanode import BuildCubeFormatNode
from patex.nodes.building_stock_logic import BuildingStockLogicNode
from patex.nodes.calibration_multidimension_node import CalibrationMultiDimensionNode
from patex.nodes.calibration_node import CalibrationNode
from patex.nodes.calibration_node_eu import CalibrationNodeEu
from patex.nodes.cell_splitter_node import CellSplitterNode
from patex.nodes.column_aggregator_node import ColumnAggregatorNode
from patex.nodes.column_appender_node import ColumnAppenderNode
from patex.nodes.column_combiner_node import ColumnCombinerNode
from patex.nodes.column_filter_node import ColumnFilterNode
from patex.nodes.column_merger_node import ColumnMergerNode
from patex.nodes.column_rename_node import ColumnRenameNode
from patex.nodes.column_rename_regex_node import ColumnRenameRegexNode
from patex.nodes.column_resorter_node import ColumnResorterNode
from patex.nodes.column_splitter_node import ColumnSplitterNode
from patex.nodes.combine_dimensions import CombineDimensions
from patex.nodes.compute_costs import ComputeCosts
from patex.nodes.concatenate_node import ConcatenateNode
from patex.nodes.constant_import_node import ConstantImportNode
from patex.nodes.constant_value_column_node import ConstantValueColumnNode
from patex.nodes.convert_unit import ConvertUnit
from patex.nodes.cost_calculation_node_eu import CostCalculationNodeEU
from patex.nodes.cross_joiner_node import CrossJoinerNode
from patex.nodes.data_import_node import DataImportNode
from patex.nodes.data_validation_node import DataValidationNode
from patex.nodes.database_delete_node import DatabaseDeleteNode
from patex.nodes.database_reader_node import DatabaseReaderNode
from patex.nodes.database_writer_node import DatabaseWriterNode
from patex.nodes.datareader_cp import DataReaderCP
from patex.nodes.datareader_fts import DataReaderFTS
from patex.nodes.datareader_ots import DataReaderOTS
from patex.nodes.divide_by_year import DivideYear
from patex.nodes.double_to_int_node import DoubleToIntNode
from patex.nodes.excel_reader_node import ExcelReaderNode
from patex.nodes.excel_writer_node import ExcelWriterNode
from patex.nodes.export_variable import ExportVariableNode
from patex.nodes.extract_column_header_node import ExtractColumnHeaderNode
from patex.nodes.filter_dimension import FilterDimension
from patex.nodes.fuel_mix import FuelMixNode
from patex.nodes.get_test_data import GetTestData
from patex.nodes.google_sheet_interactive_service_provider_node import \
    GoogleSheetsInteractiveServiceProviderNode
from patex.nodes.google_sheet_updater_node import GoogleSheetsUpdaterNode
from patex.nodes.graph_visualization_node import GraphVisualizationNode
from patex.nodes.group_by_node import GroupByNode
from patex.nodes.groupby_dimensions import GroupByDimensions
from patex.nodes.import_data import ImportDataNode
from patex.nodes.inject_variables_data_node import InjectVariablesDataNode
from patex.nodes.interface_validation_node import InterfaceValidationNode
from patex.nodes.joiner_node import JoinerNode
from patex.nodes.json_reader_node import JSONReaderNode
from patex.nodes.json_to_table_node import JSONtoTableNode
from patex.nodes.lag_and_rename_column_node import LagAndRenameColumnNode
from patex.nodes.lag_column_node import LagColumnNode
from patex.nodes.lag_variable import LagVariable
from patex.nodes.spread_capital_node import SpreadCapital
from patex.nodes.math_formula_multi_column_node import MathFormulaMultiColumnNode
from patex.nodes.math_formula_node import MathFormulaNode
from patex.nodes.math_formula_variable_node import MathFormulaVariableNode
from patex.nodes.mcd import MCDNode
from patex.nodes.merge_variables_node import MergeVariablesNode
from patex.nodes.meta_node import MetaNode
from patex.nodes.metaconnector_node import MetaconnectorNode
from patex.nodes.missing_value_apply_node import MissingValueApplyNode
from patex.nodes.missing_value_filter_node import MissingValueColumnFilterNode
from patex.nodes.missing_value_node import MissingValueNode
from patex.nodes.mysql_connector_node import MySQLConnectorNode
from patex.nodes.number_to_string_node import NumberToStringNode
from patex.nodes.pivoting_node import PivotingNode
from patex.nodes.python_1_1_node import Python11Node
from patex.nodes.python_1_2_node import Python12Node
from patex.nodes.python_2_1_node import Python21Node
from patex.nodes.python_2_2_node import Python22Node
from patex.nodes.python_edit_variable_node import PythonEditVariableNode
from patex.nodes.quality_check_node import QualityCheckNode
from patex.nodes.row_filter_node import RowFilterNode
from patex.nodes.row_id_node import RowIDNode
from patex.nodes.row_splitter_node import RowSplitterNode
from patex.nodes.rule_engine_node import RuleEngineNode
from patex.nodes.select_country import SelectCountry
from patex.nodes.sorter_node import SorterNode
from patex.nodes.split_column_by_group_node import SplitColumnByGroup
from patex.nodes.string_input_node import StringInputNode
from patex.nodes.string_manipulation_metanode import StringManipulationMetaNode
from patex.nodes.string_manipulation_node import StringManipulationNode
from patex.nodes.string_manipulation_variable_node import StringManipulationVariableNode
from patex.nodes.string_replacer_node import StringReplacerNode
from patex.nodes.string_to_number_node import StringToNumberNode
from patex.nodes.table_column_to_variable_node import TableColumnToVariableNode
from patex.nodes.table_creator_node import TableCreatorNode
from patex.nodes.table_row_to_variable_node import TableRowToVariableNode
from patex.nodes.temp_node import TempNode
from patex.nodes.timer_info_node import TimerInfoNode
from patex.nodes.transpose_node import TransposeNode
from patex.nodes.tree_aggregator_node import TreeAggregatorNode
from patex.nodes.tree_merge_groups_node import TreeMergeGroupsNode
from patex.nodes.tree_merge_node import TreeMergeNode
from patex.nodes.tree_parallel_operation_node import TreeParallelOperationNode
from patex.nodes.tree_split_node import TreeSplitNode
from patex.nodes.unit_conversion_2_metanode import UnitConversion2Node
from patex.nodes.unit_conversion_be_metanode import UnitConversionBENode
from patex.nodes.unit_conversion_metanode import UnitConversionNode
from patex.nodes.unpivoting_node import UnpivotingNode
from patex.nodes.use_variable import UseVariableNode
from patex.nodes.validate_interface import ValidateInterface
from patex.nodes.variable_to_table_column_node import VariableToTableColumnNode
from patex.nodes.wrapped_meta_node import WrappedMetaNode
from patex.nodes.wrapped_node_input import WrappedNodeInput
from patex.nodes.wrapped_node_output import WrappedNodeOutput
from patex.nodes.write_to_database_metanode import WriteToDBNode
from patex.nodes.x_switch import XSwitchNode


def prepare_flow_vars(xml_root, xmlns):
    def find_used_variables(subtree, path, used_variables):
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
                find_used_variables(child, path, used_variables)

        else:
            used_variables.append((path, subtree.get('value')))

    variables = xml_root.find(xmlns + "config[@key='variables']")
    if variables is None:
        return None

    flow_vars_params = {}

    used_variables = []
    find_used_variables(variables, "", used_variables)

    model_in_settings_file = xml_root.find(xmlns + "config[@key='model']")

    for (path, var) in used_variables:
        keys = path.split('/')
        keys.pop(0)  # Get rid of the useless '' entry at index 0

        # In the metanode template, there is a 'tree' key that does not exist in the 'model' entry.
        # We remove it.
        if 'tree' in keys:
            keys.remove('tree')
        parameter_to_modify = model_in_settings_file

        for key in keys:
            if parameter_to_modify.find(xmlns + "config[@key='" + key + "']") is not None:
                parameter_to_modify = parameter_to_modify.find(xmlns + "config[@key='" + key + "']")
            else:
                parameter_to_modify = parameter_to_modify.find(xmlns + "entry[@key='" + key + "']")
        if parameter_to_modify is not None:
            parameter_to_modify.attrib["value"] = var
            flow_vars_params[key] = var

    return flow_vars_params


class KnimeNodeBuilder(object):
    def __init__(self, db_host=None, db_port=None, db_user=None, db_password=None, db_schema=None,
                 google_sheet_prefix=None, baseyear=None, module_name=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_schema = db_schema
        self.google_sheet_prefix = google_sheet_prefix
        self.baseyear = baseyear
        self.module_name = module_name
        self.xmlns = "{http://www.knime.org/2008/09/XMLConfig}"

    def build(self, ctx, id, relative_path, node_settings_file, node_is_meta, node_type, knime_workspace):
        """
        Assign python script to each Knime node based on their names

        :param id: id of the node (number)
        :param relative_path: relative path to the node
        :param node_settings_file: node settings file name
        :param node_is_meta: boolean to specify if the node is a meatnode
        :param node_type: type of node (Metanode, Subnode, TempNode or NativeNode)
        :param knime_workspace: the path to the knime workspace
        :return: return the python version of the knime node
        """

        xml_root = ET.ElementTree(file=relative_path).getroot()
        flow_vars_params = None
        if xml_root is not None:
            flow_vars_params = prepare_flow_vars(xml_root, self.xmlns)

        if node_is_meta == "true":
            if node_type == "MetaNode":  # Standard Metanode (not wrapped)
                return MetaNode(id, relative_path, node_type, knime_workspace, self.db_host, self.db_port, self.db_user,
                                      self.db_password, self.db_schema,  self.google_sheet_prefix, self.module_name)
            elif node_type == "SubNode":  # Components Metanode (wrapped)
                workflow_path = relative_path.split('/settings')[0]
                workflow_xml_path = os.path.join(workflow_path, 'workflow.knime')

                tree = ET.ElementTree(file=workflow_xml_path)
                root = tree.getroot()

                subnode_name = root.find(self.xmlns + "entry[@key='name']").get('value')

                # The following Components are coded directly in the converter to allow faster conversion. It allows
                # the developpement of complex Compenents in Knime with a correspondance in python in the converter
                if subnode_name == "Tree aggregator (Java)":
                    return TreeAggregatorNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Tree merge":
                    return TreeMergeNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Tree parallel operation":
                    return TreeParallelOperationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Tree split":
                    return TreeSplitNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "DataImport":
                    return DataImportNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Data Validation (All Countries and Years)":
                    return DataValidationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "BECalc - Data Validation (All Countries and Years)":
                    return DataValidationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Data Validation":
                    return DataValidationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "BECalc - Data Validation":
                    return DataValidationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "ConstantImport":
                    return ConstantImportNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Graph Visualization":
                    return GraphVisualizationNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "EUCalc - Interface Validation":
                    return InterfaceValidationNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "TRA_lag_and_rename_column":
                    return LagAndRenameColumnNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Calibration":
                    return CalibrationNodeEu(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Calibration (6 dimensions)":
                    return CalibrationMultiDimensionNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "API interface":
                    return APIinterfaceNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Write to DB (wide table)":
                    return WriteToDBNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Write to DB (cube)":
                    return WriteToDBNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Write to CSV":
                    return WriteToDBNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Build cube format":
                    return BuildCubeFormatNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "Assemble all Pathway Explorer output":
                    return AssembleAllOutputNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "EUCalc - Graph Visualization (WebApp)":
                    return GraphVisualizationNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "EUCalc - Cost calculation":
                    return CostCalculationNodeEU(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - DataReader CP (regex)":
                    return DataReaderCP(id, relative_path, node_type, knime_workspace, self.google_sheet_prefix)
                elif subnode_name == "EUCalc - DataReader OTS (regex)":
                    return DataReaderOTS(id, relative_path, node_type, knime_workspace, self.google_sheet_prefix)
                elif subnode_name == "EUCalc - DataReader FTS (regex)":
                    return DataReaderFTS(id, relative_path, node_type, knime_workspace, self.google_sheet_prefix)
                elif subnode_name == "EUCalc - Split column by group":
                    return SplitColumnByGroup(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Unit conversion":
                    return UnitConversionNode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "BECalc - Unit conversion":
                    return UnitConversionBENode(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Unit conversion (2)":
                    return UnitConversion2Node(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "EUCalc - Tree merge (Groups)":
                    return TreeMergeGroupsNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif ("Quality_check" in subnode_name) or ("Quality check" in subnode_name):
                    # The QualityCheckNode is an empty node in the converter. I allows the user to make quality checks
                    # that won't be converted in the converter
                    return QualityCheckNode(id, relative_path, node_type, knime_workspace)
                elif "Temp_node" in subnode_name:
                    # The Tempnode is an empty node in the converter. I allows the user to make tests that won't be
                    # converted in the converter
                    return TempNode(id, relative_path, node_type, knime_workspace)
                elif "String_Manipulation" in subnode_name:
                    return StringManipulationMetaNode(id, relative_path, node_type, knime_workspace)
                elif "Timer Benchmark" in subnode_name:
                    return TempNode(id, relative_path, node_type, knime_workspace)
                #elif "6.5 Macro Economy" in subnode_name:
                #    return MacroEconomyTemp(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "Use Variable":
                    return UseVariableNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "MCD":
                    return MCDNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Export Variable":
                    return ExportVariableNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Calibrate":
                    return CalibrationNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "XCalc - Fuel Mix":
                    return FuelMixNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "X Switch":
                    return XSwitchNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Import Data":
                    return ImportDataNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Add Missing Years":
                    return AddMissingYearsNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Compute Costs":
                    return ComputeCosts.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Spread capital":
                    return SpreadCapital.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Convert Unit":
                    return ConvertUnit.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "GroupBy dimensions":
                    return GroupByDimensions.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Filter dimension":
                    return FilterDimension.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Lag Variable":
                    return LagVariable.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Divide by year":
                    return DivideYear.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Validate Interface":
                    return ValidateInterface.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Get Test Data":
                    return GetTestData(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "Combine Dimensions":
                    return CombineDimensions(id, relative_path, node_type, knime_workspace)
                elif subnode_name == "Select Country":
                    return SelectCountry.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Add Trigram":
                    return AddTrigram.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                elif subnode_name == "Add Specific Trigram":
                    return AddSpecificTrigram(id, relative_path, node_type, knime_workspace)
                elif "Building sto" in node_settings_file:
                    return BuildingStockLogicNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
                #############################################
                # Include any other special components here #
                #############################################
                else:
                    # if the component is not in the list above, the converter is reading the nodes inside
                    return WrappedMetaNode(id, relative_path, node_type, knime_workspace, self.db_host, self.db_port, self.db_user,
                                      self.db_password, self.db_schema, self.google_sheet_prefix, self.module_name)
            elif node_type == "TempNode":  # Metaconnector Metanode
                return MetaconnectorNode(id, relative_path, node_type, knime_workspace)
            else:
                raise Exception("Unknown metanode type: {0}".format(relative_path))
        elif 'WrappedNode Input' in node_settings_file or 'Component Input' in node_settings_file:
            return WrappedNodeInput.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif 'WrappedNode Output' in node_settings_file or 'Component Output' in node_settings_file:
            return WrappedNodeOutput.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Excel Reader" in node_settings_file:
            return ExcelReaderNode(id, relative_path, node_type, knime_workspace)
        elif "Excel Writer" in node_settings_file:
            return ExcelWriterNode(id, relative_path, node_type, knime_workspace)
        elif "Unpivoting" in node_settings_file:
            return UnpivotingNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Pivoting" in node_settings_file:
            return PivotingNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Missing Value Column" in node_settings_file:
            return MissingValueColumnFilterNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Column Filter" in node_settings_file:
            return ColumnFilterNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Column Rename _Regex_" in node_settings_file:
            return ColumnRenameRegexNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Column Rename" in node_settings_file:
            return ColumnRenameNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Cross Joiner" in node_settings_file:
            return CrossJoinerNode(id, relative_path, node_type, knime_workspace)
        elif "Joiner" in node_settings_file:
            return JoinerNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Row Filter" in node_settings_file:
            return RowFilterNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Python Script _1_1_" in node_settings_file:
            return Python11Node.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Python Script _1_2_" in node_settings_file:
            return Python12Node(id, relative_path, node_type, knime_workspace)
        elif "Python Script _2_1_" in node_settings_file:
            return Python21Node.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Python Script _2_2_" in node_settings_file:
            return Python22Node(id, relative_path, node_type, knime_workspace)
        elif "Table Creator" in node_settings_file:
            return TableCreatorNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Math Formula _Multi Column" in node_settings_file:
            return MathFormulaMultiColumnNode(id, relative_path, node_type, knime_workspace)
        elif "Math Formula (" in node_settings_file:
            return MathFormulaNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Math Formula _Variable_" in node_settings_file:
            return MathFormulaVariableNode(id, relative_path, node_type, knime_workspace)
        elif "Column Aggregator" in node_settings_file:
            return ColumnAggregatorNode(id, relative_path, node_type, knime_workspace)
        elif "Table Column to Variable" in node_settings_file:
            return TableColumnToVariableNode(id, relative_path, node_type, knime_workspace)
        elif "String Manipulation _Variable_" in node_settings_file:
            return StringManipulationVariableNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Inject Variables _Data_" in node_settings_file:
            return InjectVariablesDataNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Column Splitter" in node_settings_file:
            return ColumnSplitterNode(id, relative_path, node_type, knime_workspace)
        elif "Column Combiner" in node_settings_file:
            return ColumnCombinerNode(id, relative_path, node_type, knime_workspace)
        elif "MySQL Connector" in node_settings_file:
            return MySQLConnectorNode(id, relative_path, node_type, knime_workspace, self.db_host, self.db_port, self.db_user,
                                      self.db_password, self.db_schema)
        elif "String Input" in node_settings_file:
            return StringInputNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Database Reader" in node_settings_file:
            return DatabaseReaderNode(id, relative_path, node_type, knime_workspace, self.db_host, self.db_port, self.db_user,
                                      self.db_password, self.db_schema)
        elif "Database Writer" in node_settings_file:
            return DatabaseWriterNode(id, relative_path, node_type, knime_workspace)
        elif "Column Resorter" in node_settings_file:
            return ColumnResorterNode(id, relative_path, node_type, knime_workspace)
        elif "GroupBy" in node_settings_file:
            return GroupByNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Concatenate" in node_settings_file:
            return ConcatenateNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Constant Value Column" in node_settings_file:
            return ConstantValueColumnNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Sorter" in node_settings_file:
            return SorterNode(id, relative_path, node_type, knime_workspace)
        elif "Missing Value _Apply_" in node_settings_file:
            return MissingValueApplyNode(id, relative_path, node_type, knime_workspace)
        elif "Missing Value" in node_settings_file:
            return MissingValueNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Column Appender" in node_settings_file:
            return ColumnAppenderNode(id, relative_path, node_type, knime_workspace)
        elif "JSON Reader" in node_settings_file:
            return JSONReaderNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "JSON to Table" in node_settings_file:
            return JSONtoTableNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Table Row to Variable" in node_settings_file:
            return TableRowToVariableNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "String To Number" in node_settings_file:
            return StringToNumberNode(id, relative_path, node_type, knime_workspace)
        elif "Number To String" in node_settings_file:
            return NumberToStringNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Extract Column Header" in node_settings_file:
            return ExtractColumnHeaderNode(id, relative_path, node_type, knime_workspace)
        elif "Transpose" in node_settings_file:
            return TransposeNode(id, relative_path, node_type, knime_workspace)
        elif "Cell Splitter" in node_settings_file:
            return CellSplitterNode(id, relative_path, node_type, knime_workspace)
        elif "Timer Info" in node_settings_file:
            return TimerInfoNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Google Sheets Interactive Service Provider" in node_settings_file:
            return GoogleSheetsInteractiveServiceProviderNode(id, relative_path, node_type, knime_workspace)
        elif "Google Sheets Updater" in node_settings_file:
            return GoogleSheetsUpdaterNode(id, relative_path, node_type, knime_workspace)
        elif "Variable to Table Column" in node_settings_file:
            return VariableToTableColumnNode(id, relative_path, node_type, knime_workspace)
        elif "Column Merger" in node_settings_file:
            return ColumnMergerNode(id, relative_path, node_type, knime_workspace)
        elif "Lag Column" in node_settings_file:
            return LagColumnNode(id, relative_path, node_type, knime_workspace)
        elif "Row Splitter" in node_settings_file:
            return RowSplitterNode(id, relative_path, node_type, knime_workspace)
        elif "String Replacer" in node_settings_file:
            return StringReplacerNode(id, relative_path, node_type, knime_workspace)
        elif "Database Delete" in node_settings_file:
            return DatabaseDeleteNode(id, relative_path, node_type, knime_workspace)
        elif "Python Edit Variable" in node_settings_file:
            return PythonEditVariableNode(id, relative_path, node_type, knime_workspace)
        elif "RowID" in node_settings_file:
            return RowIDNode(id, relative_path, node_type, knime_workspace)
        elif "Rule Engine" in node_settings_file:
            return RuleEngineNode(id, relative_path, node_type, knime_workspace)
        elif "Double To Int" in node_settings_file:
            return DoubleToIntNode(id, relative_path, node_type, knime_workspace)
        elif "String Manipulation" in node_settings_file:
            return StringManipulationNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        elif "Merge Variables" in node_settings_file:
            return MergeVariablesNode.from_knime(ctx, id, xml_root, self.xmlns, flow_vars_params)
        else:
            raise Exception("Unknown node type: {0} - {0}".format(node_settings_file, relative_path))
