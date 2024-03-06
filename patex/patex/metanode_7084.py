import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_7084(port_01):

    module_name = 'electricity_supply'
    # Energy demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)
    # Keep only energy-carrier fossil-fuels
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-natural', 'liquid-ff', 'solid-ff-coal', 'gaseous-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh.loc[~energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-natural', 'liquid-ff', 'solid-ff-coal', 'gaseous-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    # primary-category = primary for elec / hydrogen / chp / heat
    out_6375_1 = TableCreatorNode(df=pd.DataFrame(columns=['primary-category', 'sector'], data=[['primary', 'heat'], ['primary', 'chp'], ['primary', 'electricity'], ['primary', 'hydrogen'], ['primary', 'efuels']]))()
    # Add primary-category based on sector
    out_6374_1 = JoinerNode(joiner='left', left_input=['sector'], right_input=['sector'])(df_left=energy_demand_TWh_2, df_right=out_6375_1)
    # If missing primary set non-primary
    out_6374_1 = MissingValueNode(dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='non-primary')(df=out_6374_1)
    # Add energy-carrier-category based on energy-carrier
    out_6374_1 = StringManipulationNode(expression='substr($energy-carrier$, 0, indexOf($energy-carrier$, "-"))', var_name='energy-carrier-category')(df=out_6374_1)
    # Group by  country, years, energy-carrier-category, primary-category
    out_6374_1_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'primary-category', 'energy-carrier-category'], aggregation_method='Sum')(df=out_6374_1)

    # Calibration
    # Note : for the moment : only for oil - non primary demand

    # Calibration: fossil-energy-demand [TWh]
    fossil_energy_demand_TWh = ImportDataNode(trigram='elc', variable_name='fossil-energy-demand', variable_type='Calibration')()
    # Group by energy-carrier-category, primary-category (sum)  We don't want to have the energy-carrier because data are calibrated on the totals.
    fossil_energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier-category', 'primary-category'], aggregation_method='Sum')(df=fossil_energy_demand_TWh)
    # Apply Calibration on energy-demand[TWh]
    _, out_0_2, out_0_3 = CalibrationNode(data_to_be_cal='energy-demand[TWh]', data_cal='fossil-energy-demand[TWh]')(input_table=out_6374_1_2, cal_table=fossil_energy_demand_TWh)
    # energy-demand[TWh] (replace) = energy-demand[TWh] * cal_rate
    energy_demand_TWh = MCDNode(operation_selection='x * y', output_name='energy-demand[TWh]')(input_table_1=out_6374_1, input_table_2=out_0_2)
    energy_demand_TWh = pd.concat([energy_demand_TWh_excluded, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])

    # Keep Calibration rate

    # cal rate for fossil-energy-demand[TWh]
    cal_rate_energy_demand_TWh = UseVariableNode(selected_variable='cal_rate_energy-demand[TWh]')(input_table=out_0_3)
    # Add  energy-carrier = fossil-fuel
    cal_rate_energy_demand_TWh['energy-carrier'] = "fossil-fuel"

    return cal_rate_energy_demand_TWh, energy_demand_TWh


