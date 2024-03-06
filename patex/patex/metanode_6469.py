import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_6469(port_01, port_02, port_03, port_04):
    # How computation of exogenous vs endogenous costs is made
    # 
    #   Endogenous :
    #   We compute value based on the costs of production (capex and opex) divided by the energy production.
    #   With these costs we can see the impact of (according to our model prediction) :
    #     - increase / decrease energy production through time
    #     - build new capacities / maintenance of existing capacities
    # 
    #   As our model is not perfect, we calibrate values based on historical (known) values of electricity costs (cfr exogenous costs => we only consider historical values (= OTS (only)). The trend computed by the model is kept thanks to this calibration.
    # 
    #   Exogenous :
    #   We do not take into account the operation / construction of capacities neither the energy production based on our model.
    #   We consider the cost as "fixed" (not depending on our production / capacities).



    module_name = 'electricity_supply'

    # Final Energy Costs

    # Group by  country, years
    port_04_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=port_04)

    # COE

    # Group by  country, years, way-of-prod
    port_02_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')(df=port_02)
    # capex [MEUR]
    capex_MEUR = UseVariableNode(selected_variable='capex[MEUR]')(input_table=port_03)
    # capex-opex[MEUR] = capex[MEUR] + opex[MEUR]
    capex_opex_MEUR = MCDNode(operation_selection='x + y', output_name='capex-opex[MEUR]')(input_table_1=capex_MEUR, input_table_2=capex_MEUR)
    # Group by  country, years, sector
    capex_opex_MEUR_2 = GroupByDimensions(groupby_dimensions=['Years', 'Country', 'sector'], aggregation_method='Sum')(df=capex_opex_MEUR)

    # Endogenous Price
    # 
    # = Total Costs / production

    # total-costs[MEUR] = primary-costs[MEUR] + capex-opex[MEUR]
    total_costs_MEUR_2 = MCDNode(operation_selection='x + y', output_name='total-costs[MEUR]')(input_table_1=capex_opex_MEUR_2, input_table_2=port_04_2)
    # Group by  country, years, way-of-prod
    capex_opex_MEUR = GroupByDimensions(groupby_dimensions=['Years', 'Country', 'way-of-production'], aggregation_method='Sum')(df=capex_opex_MEUR)
    # total-costs[MEUR] = primary-costs[MEUR] + capex-opex[MEUR]  Set primary-costs to 0 when missing
    total_costs_MEUR = MCDNode(operation_selection='x + y', output_name='total-costs[MEUR]', fill_value_bool='Left [x] Outer Join')(input_table_1=capex_opex_MEUR, input_table_2=port_04)
    # electricity-COE [EUR/MWh] = total-costs[MEUR] / energy-production[TWh] 
    electricity_COE_EUR_per_MWh = MCDNode(operation_selection='y / x', output_name='electricity-COE[EUR/MWh]')(input_table_1=port_02_2, input_table_2=total_costs_MEUR)
    # Set to 0 (no production)
    electricity_COE_EUR_per_MWh = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=electricity_COE_EUR_per_MWh)
    # Group by  country, years, sector
    port_02 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')(df=port_02)
    # fuel-price-endogenous[MEUR/TWh] = total-costs[MEUR] / energy-production[TWh]
    fuel_price_endogenous_MEUR_per_TWh = MCDNode(operation_selection='y / x', output_name='fuel-price-endogenous[MEUR/TWh]')(input_table_1=port_02, input_table_2=total_costs_MEUR_2)

    # Calibrate Fuel Price

    # OTS (only) exogenous-energy-costs [MEUR/TWh] from TEC  Metrics = OTS/FTS but for calibration we only want OTS value
    exogenous_energy_costs_MEUR_per_TWh = ImportDataNode(trigram='tec', variable_name='exogenous-energy-costs', variable_type='OTS (only)')()
    # Keep only electricity energy-carrier
    exogenous_energy_costs_MEUR_per_TWh = exogenous_energy_costs_MEUR_per_TWh.loc[exogenous_energy_costs_MEUR_per_TWh['energy-carrier'].isin(['electricity'])].copy()
    # Group by  Country, Years (MEAN)
    exogenous_energy_costs_MEUR_per_TWh = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Mean')(df=exogenous_energy_costs_MEUR_per_TWh)

    # Calibration

    # Apply Calibration on fuel-price [MEUR/TWh]  Only endogenous is calibrated
    fuel_price_endogenous_MEUR_per_TWh, _, _ = CalibrationNode(data_to_be_cal='fuel-price-endogenous[MEUR/TWh]', data_cal='exogenous-energy-costs[EUR/MWh]')(input_table=fuel_price_endogenous_MEUR_per_TWh, cal_table=exogenous_energy_costs_MEUR_per_TWh)

    # Switch Endogenous to Exogenous Price
    # 
    # Apply lever link-costs-to-activity
    # 
    # If :  link-costs-to-activity == 1 => Endogenous
    # Else => Exogenous

    # fuel-price-endogenous [MEUR/TWh]
    fuel_price_endogenous_MEUR_per_TWh = UseVariableNode(selected_variable='fuel-price-endogenous[MEUR/TWh]')(input_table=fuel_price_endogenous_MEUR_per_TWh)
    # OTS/FTS link-costs-to-activity
    link_costs_to_activity = ImportDataNode(trigram='elc', variable_name='link-costs-to-activity')()

    # Apply exogenous costs levers

    # OTS/FTS exogenous-energy-costs [MEUR/TWh] from TEC
    exogenous_energy_costs_MEUR_per_TWh = ImportDataNode(trigram='tec', variable_name='exogenous-energy-costs')()
    # Keep only electricity energy-carrier
    exogenous_energy_costs_MEUR_per_TWh = exogenous_energy_costs_MEUR_per_TWh.loc[exogenous_energy_costs_MEUR_per_TWh['energy-carrier'].isin(['electricity'])].copy()
    # fuel-price-exogenous[MEUR/TWh] = exogenous-energy-costs * (1-link-costs-to-activity[-])
    fuel_price_exogenous_MEUR_per_TWh = MCDNode(operation_selection='(1-x) * y', output_name='fuel-price-exogenous[MEUR/TWh]')(input_table_1=link_costs_to_activity, input_table_2=exogenous_energy_costs_MEUR_per_TWh)
    # fuel-price-endogenous[MEUR/TWh] = (replace) fuel-price-endogenous[MEUR/TWh] * link-costs-to-activity[-]
    fuel_price_endogenous_MEUR_per_TWh = MCDNode(operation_selection='x * y', output_name='fuel-price-endogenous[MEUR/TWh]')(input_table_1=fuel_price_endogenous_MEUR_per_TWh, input_table_2=link_costs_to_activity)
    # fuel-price[MEUR/TWh] = fuel-price-exogenous[MEUR/TWh] + fuel-price-endogenous[MEUR/TWh]
    fuel_price_MEUR_per_TWh = MCDNode(operation_selection='x + y', output_name='fuel-price[MEUR/TWh]')(input_table_1=fuel_price_endogenous_MEUR_per_TWh, input_table_2=fuel_price_exogenous_MEUR_per_TWh)

    # Final Energy Costs
    # 
    # = Fuel price [MEUR/TWh] x demand [TWH]

    # Group by  country, years, energy-carrier (MEAN)
    fuel_price_MEUR_per_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Mean')(df=fuel_price_MEUR_per_TWh)
    # final-energy-costs[MEUR] = fuel-price[MEUR/TWh] * energy-demand[TWh]
    final_energy_costs_MEUR = MCDNode(operation_selection='x * y', output_name='final-energy-costs[MEUR]')(input_table_1=port_01, input_table_2=fuel_price_MEUR_per_TWh)

    return final_energy_costs_MEUR, fuel_price_MEUR_per_TWh, electricity_COE_EUR_per_MWh


