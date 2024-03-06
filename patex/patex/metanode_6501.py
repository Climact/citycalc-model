import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_6501(port_01, port_02, port_03, port_04):
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
    # opex [MEUR]
    capex_MEUR = UseVariableNode(selected_variable='capex[MEUR]')(input_table=port_03)
    # Group by  country, years, sector
    port_04 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')(df=port_04)
    # Remove  dimension values
    port_04 = port_04.loc[port_04['sector'].isin(['efuels'])].copy()
    # Group by  dimensions
    port_02 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=port_02)
    # capex-opex[MEUR] = capex[MEUR] + opex[MEUR]
    capex_opex_MEUR = MCDNode(operation_selection='x + y', output_name='capex-opex[MEUR]')(input_table_1=capex_MEUR, input_table_2=capex_MEUR)
    # Group by  Country, Years, sector
    capex_opex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')(df=capex_opex_MEUR)

    # Endogenous Price
    # 
    # = Total Costs / production

    # total-costs[MEUR] = primary-costs[MEUR] + capex-opex[MEUR]
    total_costs_MEUR = MCDNode(operation_selection='x + y', output_name='total-costs[MEUR]')(input_table_1=capex_opex_MEUR, input_table_2=port_04)
    # fuel-price-endogenous[MEUR/TWh] = total-costs[MEUR] / energy-production[TWh]
    fuel_price_endogenous_MEUR_per_TWh = MCDNode(operation_selection='y / x', output_name='fuel-price-endogenous[MEUR/TWh]')(input_table_1=port_02, input_table_2=total_costs_MEUR)
    # Set to 0 (no production)
    fuel_price_endogenous_MEUR_per_TWh = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=fuel_price_endogenous_MEUR_per_TWh)

    # Calibrate Fuel Price

    # OTS (only) exogenous-energy-costs [MEUR/TWh] from TEC  Metrics = OTS/FTS but for calibration we only want OTS value
    exogenous_energy_costs_MEUR_per_TWh = ImportDataNode(trigram='tec', variable_name='exogenous-energy-costs', variable_type='OTS (only)')()
    # Keep only synfuels energy-carrier
    exogenous_energy_costs_MEUR_per_TWh = exogenous_energy_costs_MEUR_per_TWh.loc[exogenous_energy_costs_MEUR_per_TWh['energy-carrier'].isin(['synfuels'])].copy()
    # Group by  Country, Years (MEAN)
    exogenous_energy_costs_MEUR_per_TWh = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Mean')(df=exogenous_energy_costs_MEUR_per_TWh)

    # Calibration

    # Apply Calibration on fuel-price[MEUR/TWh]
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
    # Keep only synfuels energy-carrier
    exogenous_energy_costs_MEUR_per_TWh = exogenous_energy_costs_MEUR_per_TWh.loc[exogenous_energy_costs_MEUR_per_TWh['energy-carrier'].isin(['synfuels'])].copy()
    # fuel-price-exogenous[MEUR/TWh] = exogenous-energy-costs * (1-link-costs-to-activity[-])
    fuel_price_exogenous_MEUR_per_TWh = MCDNode(operation_selection='(1-x) * y', output_name='fuel-price-exogenous[MEUR/TWh]')(input_table_1=link_costs_to_activity, input_table_2=exogenous_energy_costs_MEUR_per_TWh)
    # fuel-price-endogenous[MEUR/TWh] = (replace) fuel-price-endogenous[MEUR/TWh] * link-costs-to-activity[-]
    fuel_price_endogenous_MEUR_per_TWh = MCDNode(operation_selection='x * y', output_name='fuel-price-endogenous[MEUR/TWh]')(input_table_1=fuel_price_endogenous_MEUR_per_TWh, input_table_2=link_costs_to_activity)
    # fuel-price[MEUR/TWh] = fuel-price-exogenous[MEUR/TWh] + fuel-price-endogenous[MEUR/TWh]
    fuel_price_MEUR_per_TWh = MCDNode(operation_selection='x + y', output_name='fuel-price[MEUR/TWh]')(input_table_1=fuel_price_endogenous_MEUR_per_TWh, input_table_2=fuel_price_exogenous_MEUR_per_TWh)

    # Final Energy Costs
    # 
    # = Fuel price [MEUR/TWh] x demand [TWH]

    # Group by  country, years, energy-carrier
    fuel_price_MEUR_per_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')(df=fuel_price_MEUR_per_TWh)
    # Keep only energy-carrier liquid *
    port_01_2 = port_01.loc[port_01['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    port_01_excluded = port_01.loc[~port_01['energy-carrier'].isin(['liquid-syn', 'liquid-syn-diesel', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil'])].copy()
    # Group by  country, years, sector
    port_01_excluded = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')(df=port_01_excluded)
    # energy-carrier-cat = gaseous-syn
    port_01_excluded['energy-carrier-cat'] = "gaseous-syn"
    # Group by  country, years, sector
    port_01 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')(df=port_01_2)
    # energy-carrier-cat = liquid-syn
    port_01['energy-carrier-cat'] = "liquid-syn"
    port_01 = pd.concat([port_01, port_01_excluded.set_index(port_01_excluded.index.astype(str) + '_dup')])
    # final-energy-costs[MEUR] = fuel-price[MEUR/TWh] * energy-demand[TWh]
    final_energy_costs_MEUR = MCDNode(operation_selection='x * y', output_name='final-energy-costs[MEUR]')(input_table_1=port_01, input_table_2=fuel_price_MEUR_per_TWh)

    return final_energy_costs_MEUR, fuel_price_MEUR_per_TWh


