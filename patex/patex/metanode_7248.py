import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_7248(port_01):

    module_name = 'electricity_supply'

    # Heat demand (from other sectors)

    # Energy demand [TWh]
    energy_demand_TWh_2 = use_variable(input_table=port_01, selected_variable='energy-demand[TWh]')
    # Keep only energy-carrier = heat
    energy_demand_TWh = energy_demand_TWh_2.loc[energy_demand_TWh_2['energy-carrier'].isin(['heat'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh_2.loc[~energy_demand_TWh_2['energy-carrier'].isin(['heat'])].copy()
    # Group by  energy-carrier (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Calibration

    # Calibration: energy-demand [TWh]
    energy_demand_TWh_3 = import_data(trigram='elc', variable_name='energy-demand', variable_type='Calibration')
    # Apply Calibration on energy-demand[TWh]
    _, out_7100_2, out_7100_3 = calibration(input_table=energy_demand_TWh_2, cal_table=energy_demand_TWh_3, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand[TWh]')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * cal_rate  Set cal_rate to 1 when missing
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=out_7100_2, operation_selection='x * y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_excluded.set_index(energy_demand_TWh_excluded.index.astype(str) + '_dup')])

    return energy_demand_TWh, out_7100_3


