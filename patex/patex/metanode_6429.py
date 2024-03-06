import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_6429(port_01, port_02):
    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : CCU[Mt]


    # Energy - production


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : primary-energy-demand[TWh]


    # In future :
    # 
    # Should be moved in HTS declaration


    # In future :
    # 
    # Account for these savings in costs (// industry)



    module_name = 'electricity_supply'

    # E-FUELS Production

    # Efuels demand (from other sectors)

    # energy-demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)
    # Keep only energy-carrier synfuels
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['liquid-syn-diesel', 'gaseous-syn', 'liquid-syn-gasoline', 'liquid-syn-kerosene', 'liquid-syn-marinefueloil', 'liquid-syn'])].copy()
    # Group by  country, years, energy-carrier
    energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')(df=energy_demand_TWh)

    # Efuels production (and primary-energy-demand)

    # Get energy-carrier-category (liquid / gaseous / solid) GENERIC : based on first caracters
    energy_demand_TWh = StringManipulationNode(expression='substr($energy-carrier$, 0, indexOf($energy-carrier$, "-"))', var_name='energy-carrier-category')(df=energy_demand_TWh)

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import- efuels
    energy_net_import_efuels = ImportDataNode(trigram='elc', variable_name='energy-net-import-efuels')()
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = MCDNode(operation_selection='x * y', output_name='energy-imported[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_net_import_efuels)

    # Formating data for other modules + Pathway Explorer

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh_2 = UseVariableNode(selected_variable='energy-imported[TWh]')(input_table=energy_imported_TWh)
    # sector = efuels
    energy_imported_TWh_2['sector'] = "efuels"
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = MCDNode(operation_selection='x - y', output_name='energy-producted[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh)

    # Apply technology-share levers 
    # => determine with which technology efuels are produced

    # OTS / FTS technology-share-efuels [%]
    technology_share_efuels_percent = ImportDataNode(trigram='elc', variable_name='technology-share-efuels')()
    # energy-production[TWh] = energy-producted[TWh] * technology-share-efuels[%]
    energy_production_TWh = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=energy_producted_TWh, input_table_2=technology_share_efuels_percent)

    # Costs CAPEX / OPEX

    # energy-production [TWh]
    energy_production_TWh = UseVariableNode(selected_variable='energy-production[TWh]')(input_table=energy_production_TWh)
    # Unit conversion TWh to kW
    energy_production_kW = energy_production_TWh.drop(columns='energy-production[TWh]').assign(**{'energy-production[kW]': energy_production_TWh['energy-production[TWh]'] * 114077.116130504})
    # Group by  country, years, energy-carrier-category, way-of-production
    energy_production_kW = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier-category', 'way-of-production'], aggregation_method='Sum')(df=energy_production_kW)
    # energy-production [kW]
    energy_production_kW_2 = UseVariableNode(selected_variable='energy-production[kW]')(input_table=energy_production_kW)
    # RCP costs-for-efuels [MEUR/kW] from TEC
    costs_for_efuels_MEUR_per_kW = ImportDataNode(trigram='tec', variable_name='costs-for-efuels', variable_type='RCP')()
    # RCP price-indices [-] from TEC
    price_indices_ = ImportDataNode(trigram='tec', variable_name='price-indices', variable_type='RCP')()
    # Lag  energy-production[kW]
    out_7534_1, out_7534_2 = LagVariable(in_var='energy-production[kW]')(df=energy_production_kW)
    # energy-production -lagged [kW]
    energy_production_lagged_kW = UseVariableNode(selected_variable='energy-production_lagged[kW]')(input_table=out_7534_1)
    # new-capacities[kW] = energy-production[kW] - energy-production-lagged[kW]
    new_capacities_kW = MCDNode(operation_selection='x - y', output_name='new-capacities[kW]')(input_table_1=energy_production_kW_2, input_table_2=energy_production_lagged_kW)
    # if < 0 then 0
    mask = new_capacities_kW['new-capacities[kW]'] < 0
    new_capacities_kW.loc[mask, 'new-capacities[kW]'] =  0
    new_capacities_kW.loc[~mask, 'new-capacities[kW]'] =  new_capacities_kW.loc[~mask, 'new-capacities[kW]']
    # Timestep
    Timestep = UseVariableNode(selected_variable='Timestep')(input_table=out_7534_2)
    # annual-new-capacities[kW] = new-capacities[kW] / Timestep
    annual_new_capacities_kW = MCDNode(operation_selection='x / y', output_name='annual-new-capacities[kW]')(input_table_1=new_capacities_kW, input_table_2=Timestep)
    # OTS/FTS wacc [%] from TEC
    wacc_percent = ImportDataNode(trigram='tec', variable_name='wacc')()
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')(df=wacc_percent)
    # Compute capex for new efuels capacities[kW]
    out_9527_1 = ComputeCosts(activity_variable='annual-new-capacities[kW]')(df_activity=annual_new_capacities_kW, df_unit_costs=costs_for_efuels_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # Compute opex for efuels energy-production[kW]
    out_9523_1 = ComputeCosts(activity_variable='energy-production[kW]', cost_type='OPEX')(df_activity=energy_production_kW, df_unit_costs=costs_for_efuels_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # RCP efuels-cost-user [-]
    efuels_cost_user_ = ImportDataNode(trigram='elc', variable_name='efuels-cost-user', variable_type='RCP')()
    # opex[MEUR] = opex[MEUR] * efuels-cost-user[-]
    opex_MEUR = MCDNode(operation_selection='x * y', output_name='opex[MEUR]')(input_table_1=out_9523_1, input_table_2=efuels_cost_user_)
    # Group by  Country, Years (sum)
    opex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')(df=opex_MEUR)
    # capex[MEUR] = capex[MEUR] * efuels-cost-user[-]
    capex_MEUR = MCDNode(operation_selection='x * y', output_name='capex[MEUR]')(input_table_1=efuels_cost_user_, input_table_2=out_9527_1)
    # Group by  Country, Years (sum)
    capex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'cost-user', 'Years'], aggregation_method='Sum')(df=capex_MEUR)
    MEUR = pd.concat([opex_MEUR, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])

    # Capex / Opex

    # Keep capex / opex + Country, Years
    MEUR = ColumnFilterNode(pattern='^.*$')(df=MEUR)
    # sector = efuels
    MEUR['sector'] = "efuels"

    # Energy demand by technology (conv = the only one for the moment)

    # Apply efficiency-improvement levers 
    # => will influence the quantity of primary energy carrier to use for efuels production

    # OTS / FTS efficiency-imp-efuels [-]
    efficiency_imp_efuels_ = ImportDataNode(trigram='elc', variable_name='efficiency-imp-efuels')()
    # OTS (only) energy-efficiency-efuels [-]
    energy_efficiency_efuels_ = ImportDataNode(trigram='elc', variable_name='energy-efficiency-efuels', variable_type='OTS (only)')()
    # Same as last available year
    energy_efficiency_efuels_ = AddMissingYearsNode()(df_data=energy_efficiency_efuels_)
    # efficiency-imp-efuels (replace) = 1 - efficiency-imp-efuels
    efficiency_imp_efuels_percent = efficiency_imp_efuels_.assign(**{'efficiency-imp-efuels[%]': 1.0-efficiency_imp_efuels_['efficiency-imp-efuels[%]']})
    # energy-efficiency[-] = energy-efficiency-efuels * efficiency-imp-efuels * and not / ???
    energy_efficiency = MCDNode(operation_selection='x * y', output_name='energy-efficiency[-]')(input_table_1=efficiency_imp_efuels_percent, input_table_2=energy_efficiency_efuels_)
    # primary-energy-demand[TWh] = energy-production[TWh] * energy-efficiency[-] * and not / ??
    primary_energy_demand_TWh = MCDNode(operation_selection='x * y', output_name='primary-energy-demand[TWh]')(input_table_1=energy_production_TWh, input_table_2=energy_efficiency)
    # sector = efuels
    primary_energy_demand_TWh['sector'] = "efuels"

    # Carbon capture use (CCU), Direct Air Capture (DAC) and primary energy-demand for DAC
    # 
    # 1) We determine the quantity of CO2 used for efuels production or industry feedstock / process (=CCU)
    # 2) We determine de % of CCU that comes from DAC. As DAC required energy, we compute the primary energy demand for DAC as well
    # 3) The remaining CCU demand (after DAC supply), is furnished by CC (in another sub-module)

    # For efuels production

    # OTS (only) gaes-capture-efuels [kg/kWh]
    gaes_capture_efuels_kg_per_kWh = ImportDataNode(trigram='elc', variable_name='gaes-capture-efuels', variable_type='OTS (only)')()
    # Same as last available year
    gaes_capture_efuels_kg_per_kWh = AddMissingYearsNode()(df_data=gaes_capture_efuels_kg_per_kWh)
    # Unit conversion kg/kWh to Mt/TWh
    gaes_capture_efuels_Mt_per_TWh = gaes_capture_efuels_kg_per_kWh.drop(columns='gaes-capture-efuels[kg/kWh]').assign(**{'gaes-capture-efuels[Mt/TWh]': gaes_capture_efuels_kg_per_kWh['gaes-capture-efuels[kg/kWh]'] * 1.0})
    # CCU[Mt] = gaes-capture-efuels[Mt/TWh] * energy-production[TWh]
    CCU_Mt = MCDNode(operation_selection='x * y', output_name='CCU[Mt]')(input_table_1=energy_production_TWh, input_table_2=gaes_capture_efuels_Mt_per_TWh)
    # sector = efuels
    CCU_Mt['sector'] = "efuels"
    # sector = efuels
    energy_production_TWh['sector'] = "efuels"
    # CCU [Mt]
    CCU_Mt_2 = UseVariableNode(selected_variable='CCU[Mt]')(input_table=port_02)

    # For industry : CO2 need (other than efuels => efuels feestock are in included in efuels demand from industry : cfr above)

    # CCU [Mt]
    CCU_Mt_2 = UseVariableNode(selected_variable='CCU[Mt]')(input_table=CCU_Mt_2)
    # technology as way-of-production  In future : we should harmonize dimension names ; technology = way of prod.
    out_7558_1 = CCU_Mt_2.rename(columns={'technology': 'way-of-production'})
    # gaes = CO2
    out_7558_1['gaes'] = "CO2"
    # Node 5876
    out_7540_1 = pd.concat([CCU_Mt, out_7558_1.set_index(out_7558_1.index.astype(str) + '_dup')])

    # Apply direct-air-capture levers 
    # => Determine the % of CO2 used produced by DAC.
    # => The rest of the CO2 demand (CCU) is furnished by Carbon Capture (CC)

    # OTS / FTS direct-air-capture [-]
    direct_air_capture_ = ImportDataNode(trigram='elc', variable_name='direct-air-capture')()
    # OTS (only) energy-demand-for-DAC [kWh/tCO2e]
    energy_demand_for_DAC_kWh_per_tCO2e = ImportDataNode(trigram='elc', variable_name='energy-demand-for-DAC', variable_type='OTS (only)')()
    # Same as last available year
    energy_demand_for_DAC_kWh_per_tCO2e = AddMissingYearsNode()(df_data=energy_demand_for_DAC_kWh_per_tCO2e)
    # Unit conversion kWh/tCO2 to TWh/MtCO2
    energy_demand_for_DAC_TWh_per_MtCO2e = energy_demand_for_DAC_kWh_per_tCO2e.drop(columns='energy-demand-for-DAC[kWh/tCO2e]').assign(**{'energy-demand-for-DAC[TWh/MtCO2e]': energy_demand_for_DAC_kWh_per_tCO2e['energy-demand-for-DAC[kWh/tCO2e]'] * 0.001})
    # way-of-production = DAC
    energy_demand_for_DAC_TWh_per_MtCO2e['way-of-production'] = "DAC"
    # sector =  DAC
    energy_demand_for_DAC_TWh_per_MtCO2e['sector'] = "DAC"
    # Group by  Country, Years, way-of-production, sector, gaes
    out_7540_1 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'gaes', 'sector'], aggregation_method='Sum')(df=out_7540_1)
    # CCU [Mt]
    CCU_Mt = ExportVariableNode(selected_variable='CCU[Mt]')(input_table=out_7540_1)

    # Remainging Carbon Capture Use (CCU)

    # CCU [Mt]
    CCU_Mt_2 = UseVariableNode(selected_variable='CCU[Mt]')(input_table=CCU_Mt)
    # Group by  Country, Years, gaes
    CCU_Mt = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')(df=CCU_Mt)
    # direct-air-capture[Mt] = direct-air-capture[-] * CCU[Mt]
    direct_air_capture_Mt = MCDNode(operation_selection='x * y', output_name='direct-air-capture[Mt]')(input_table_1=CCU_Mt, input_table_2=direct_air_capture_)
    # direct-air-capture [Mt]
    direct_air_capture_Mt = ExportVariableNode(selected_variable='direct-air-capture[Mt]')(input_table=direct_air_capture_Mt)

    # Direct air capture (DAC)

    # direct-air-capture [Mt]
    direct_air_capture_Mt_2 = UseVariableNode(selected_variable='direct-air-capture[Mt]')(input_table=direct_air_capture_Mt)
    # primary-energy-demand[TWh] = direct-air-capture[Mt] * energy-demand-for-DAC[TWh/Mt]
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x * y', output_name='primary-energy-demand[TWh]')(input_table_1=direct_air_capture_Mt, input_table_2=energy_demand_for_DAC_TWh_per_MtCO2e)

    # Apply DAC efficiency levers 
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS efficiency-imp-DAC [-]
    efficiency_imp_DAC_ = ImportDataNode(trigram='elc', variable_name='efficiency-imp-DAC')()
    # primary-energy-demand[TWh] (replace) = primary-energy-demand[TWh] * (1 - efficiency-imp-DAC[%])
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x * (1-y)', output_name='primary-energy-demand[TWh]')(input_table_1=primary_energy_demand_TWh_2, input_table_2=efficiency_imp_DAC_)
    primary_energy_demand_TWh = pd.concat([primary_energy_demand_TWh_2, primary_energy_demand_TWh.set_index(primary_energy_demand_TWh.index.astype(str) + '_dup')])
    # Group by  Country, Years, primary-energy-carrier, way-of-production, sector
    primary_energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'sector'], aggregation_method='Sum')(df=primary_energy_demand_TWh)

    # Primary energy demand for electricity production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)

    return energy_imported_TWh_2, energy_production_TWh, MEUR, primary_energy_demand_TWh, CCU_Mt_2, direct_air_capture_Mt_2


