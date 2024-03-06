import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_6430(port_01):
    # Energy - production


    # Note : new-capacities by timestep
    # => should be yearly capacities ?
    # If yes : add missing years (timestep should be = to 1) and divide new-capapcities / timestep to get value for these years



    module_name = 'electricity_supply'

    # HYDROGEN Production

    # Hydrogen demand (from other sectors)

    # energy-demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)
    # Keep only energy-carrier hydrogen
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['hydrogen'])].copy()
    # Group by  country, years, energy-carrier
    energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')(df=energy_demand_TWh)

    # Hydrogen production (and primary-energy-demand)

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import
    energy_net_import = ImportDataNode(trigram='elc', variable_name='energy-net-import')()
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = MCDNode(operation_selection='x * y', output_name='energy-imported[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_net_import)
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = MCDNode(operation_selection='x - y', output_name='energy-producted[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh)

    # Apply technology-share levers 
    # => determine with which technology hydrogen is produced

    # OTS / FTS technology-share-hydrogen [%]
    technology_share_hydrogen_percent = ImportDataNode(trigram='elc', variable_name='technology-share-hydrogen')()
    # energy-production[TWh] = energy-producted[TWh] * technology-share-hydrogen[%]
    energy_production_TWh = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=energy_producted_TWh, input_table_2=technology_share_hydrogen_percent)

    # Costs CAPEX / OPEX

    # energy-production [TWh]
    energy_production_TWh = UseVariableNode(selected_variable='energy-production[TWh]')(input_table=energy_production_TWh)
    # Unit conversion TWh to kW
    energy_production_kW = energy_production_TWh.drop(columns='energy-production[TWh]').assign(**{'energy-production[kW]': energy_production_TWh['energy-production[TWh]'] * 114077.116130504})
    # Group by  country, years, way-of-prod.
    energy_production_kW = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')(df=energy_production_kW)
    # RCP costs-for-energy-by-way-of-prod [MEUR/kW] from TEC
    costs_for_energy_by_way_of_prod_MEUR_per_kW = ImportDataNode(trigram='tec', variable_name='costs-for-energy-by-way-of-prod', variable_type='RCP')()
    # RCP price-indices [-] from TEC
    price_indices_ = ImportDataNode(trigram='tec', variable_name='price-indices', variable_type='RCP')()
    # energy-production [kW]
    energy_production_kW_2 = UseVariableNode(selected_variable='energy-production[kW]')(input_table=energy_production_kW)
    # Lag  energy-production[kW]
    out_7530_1, out_7530_2 = LagVariable(in_var='energy-production[kW]')(df=energy_production_kW)
    # energy-production -lagged [kW]
    energy_production_lagged_kW = UseVariableNode(selected_variable='energy-production_lagged[kW]')(input_table=out_7530_1)
    # new-capacities[kW] = energy-production[kW] - energy-production-lagged[kW]
    new_capacities_kW = MCDNode(operation_selection='x - y', output_name='new-capacities[kW]')(input_table_1=energy_production_kW_2, input_table_2=energy_production_lagged_kW)
    # if < 0 then 0
    mask = new_capacities_kW['new-capacities[kW]'] < 0
    new_capacities_kW.loc[mask, 'new-capacities[kW]'] =  0
    new_capacities_kW.loc[~mask, 'new-capacities[kW]'] =  new_capacities_kW.loc[~mask, 'new-capacities[kW]']
    # Timestep
    Timestep = UseVariableNode(selected_variable='Timestep')(input_table=out_7530_2)
    # annual-new-capacities[kW] = new-capacities[kW] / Timstep
    annual_new_capacities_kW = MCDNode(operation_selection='x / y', output_name='annual-new-capacities[kW]')(input_table_1=new_capacities_kW, input_table_2=Timestep)
    # OTS/FTS wacc [%] from TEC
    wacc_percent = ImportDataNode(trigram='tec', variable_name='wacc')()
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')(df=wacc_percent)
    # Compute capex for new hydrogen capacities[kW]
    out_9527_1 = ComputeCosts(activity_variable='annual-new-capacities[kW]')(df_activity=annual_new_capacities_kW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # Compute opex for hydrogen energy-production[kW]
    out_9523_1 = ComputeCosts(activity_variable='energy-production[kW]', cost_type='OPEX')(df_activity=energy_production_kW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # RCP energy-production-cost-user [-]
    energy_production_cost_user_ = ImportDataNode(trigram='elc', variable_name='energy-production-cost-user', variable_type='RCP')()
    # opex[MEUR] = opex[MEUR] * energy-production-cost-user[-]
    opex_MEUR = MCDNode(operation_selection='x * y', output_name='opex[MEUR]')(input_table_1=out_9523_1, input_table_2=energy_production_cost_user_)
    # Group by  Country, Years (sum)
    opex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')(df=opex_MEUR)
    # capex[MEUR] = capex[MEUR] * energy-production-cost-user[-]
    capex_MEUR = MCDNode(operation_selection='x * y', output_name='capex[MEUR]')(input_table_1=energy_production_cost_user_, input_table_2=out_9527_1)
    # Group by  Country, Years (sum)
    capex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'cost-user', 'Years'], aggregation_method='Sum')(df=capex_MEUR)
    MEUR = pd.concat([opex_MEUR, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])

    # Formating data for other modules + Pathway Explorer

    # Capex / Opex

    # Keep capex / opex + Country, Years
    MEUR = ColumnFilterNode(pattern='^.*$')(df=MEUR)
    # sector = hydrogen
    MEUR['sector'] = "hydrogen"

    # Energy demand by technology (for the moment : electro / steamref)

    # Apply efficiency-improvment levers 
    # => will influence the quantity of primary energy carrier to use for efuels production

    # OTS (only) energy-efficiency-hydrogen [-]
    energy_efficiency_hydrogen_ = ImportDataNode(trigram='elc', variable_name='energy-efficiency-hydrogen', variable_type='OTS (only)')()
    # OTS / FTS efficiency-imp-hydrogen [-]
    efficiency_imp_hydrogen_ = ImportDataNode(trigram='elc', variable_name='efficiency-imp-hydrogen')()
    # Same as last available year
    energy_efficiency_hydrogen_ = AddMissingYearsNode()(df_data=energy_efficiency_hydrogen_)
    # energy-efficiency[-] = energy-efficiency-efuels * 1 - efficiency-imp-hydrogen[%]
    energy_efficiency = MCDNode(operation_selection='x * (1-y)', output_name='energy-efficiency[-]')(input_table_1=energy_efficiency_hydrogen_, input_table_2=efficiency_imp_hydrogen_)
    # primary-energy-demand[TWh] = energy-production[TWh] * energy-efficiency[-]
    primary_energy_demand_TWh = MCDNode(operation_selection='x * y', output_name='primary-energy-demand[TWh]')(input_table_1=energy_production_TWh, input_table_2=energy_efficiency)
    # primary-denergy-demand [TWh]
    primary_energy_demand_TWh = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)

    # Primary energy demand for electricity production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)
    # sector = hydrogen
    primary_energy_demand_TWh['sector'] = "hydrogen"
    # sector = hydrogen
    energy_production_TWh['sector'] = "hydrogen"

    # Energy - import

    # energy-imported [TWh]
    energy_imported_TWh = UseVariableNode(selected_variable='energy-imported[TWh]')(input_table=energy_imported_TWh)
    # sector = hydrogen
    energy_imported_TWh['sector'] = "hydrogen"

    return energy_imported_TWh, energy_production_TWh, MEUR, primary_energy_demand_TWh


