import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_6432(port_01):
    # Calibration rate


    # Energy - production (net)


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : net-energy-production[TWh]


    # Note : 
    # We used to calibrate gross and net production (old methodology) but as the model is not perfect, (we don't modelised transfert-and-co, ...) this led to some errors (ex. primary energy demand negative !)
    # So, now, we only apply one calibration : for the primary energy demand we apply calibration based on the difference between gross and net production !


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : primary-energy-demand[TWh]
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # NOT USED FOR THE MOMENT


    # To be improved
    # Takes into account the energy efficiency linked to CC (cfr industry / Heat / CCU and Electricity !)



    module_name = 'electricity_supply'

    # Fossil fuel production (and primary-energy-demand)

    # Liquid
    # => Determine which technologies are used to produce it

    # Should be set in google sheet
    # 
    # Note : we don't take account of the efficiency ratio here as the switch of fuel is done in the same "fuel category". We don't expect any changes of efficiency from one fuel to another inside a same category.

    # Correlation for fuel mix
    out_9505_1 = TableCreatorNode(df=pd.DataFrame(columns=['category-from', 'primary-energy-carrier-from', 'category-to', 'primary-energy-carrier-to'], data=[['liquid', 'liquid-ff-crudeoil', 'gaseous', 'gaseous-ff'], ['fffuels', 'gaseous-ff', 'biofuels', 'gaseous-bio'], ['fffuels', 'gaseous-ff', 'synfuels', 'gaseous-syn']]))()
    # ratio[-] = 1
    ratio = out_9505_1.assign(**{'ratio[-]': 1.0})

    # Fossil fuel demand (from other sectors)

    # Energy demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)
    # Keep only energy-carrier fossil-fuels
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-natural', 'liquid-ff', 'solid-ff-coal', 'gaseous-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    # Group by  country, years, energy-carrier-category
    energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier-category'], aggregation_method='Sum')(df=energy_demand_TWh)

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import -refineries
    energy_net_import_refineries = ImportDataNode(trigram='elc', variable_name='energy-net-import-refineries')()
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = MCDNode(operation_selection='x * y', output_name='energy-imported[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_net_import_refineries)

    # Formating data for other modules + Pathway Explorer

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh_2 = UseVariableNode(selected_variable='energy-imported[TWh]')(input_table=energy_imported_TWh)
    # sector = refineries
    energy_imported_TWh_2['sector'] = "refineries"
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = MCDNode(operation_selection='x - y', output_name='energy-producted[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh)
    # Remove  energy-carrier-category liquid
    energy_producted_TWh_excluded = energy_producted_TWh.loc[energy_producted_TWh['energy-carrier-category'].isin(['liquid'])].copy()
    energy_producted_TWh = energy_producted_TWh.loc[~energy_producted_TWh['energy-carrier-category'].isin(['liquid'])].copy()

    # Solid and gaseous
    # => Do nothing


    def helper_6436(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # For gaseous
        mask = (output_table['energy-carrier-category'] == "gaseous")
        output_table.loc[mask, 'way-of-production'] = "refineries-gas"
        output_table.loc[mask, 'primary-energy-carrier'] = "gaseous-ff"
        
        # For solid
        mask = (output_table['energy-carrier-category'] == "solid")
        output_table.loc[mask, 'way-of-production'] = "refineries-coal"
        output_table.loc[mask, 'primary-energy-carrier'] = "solid-ff"
        return output_table
    # Add  way-of-production and primary-energy-carrier
    out_6436_1 = helper_6436(input_table=energy_producted_TWh)
    # energy-producted[TWh] = net-energy-production[TWh]
    out_6462_1 = out_6436_1.rename(columns={'energy-producted[TWh]': 'net-energy-production[TWh]'})

    # Net production
    # [TWh]
    # gas / coal

    # net-energy-production [TWh]
    net_energy_production_TWh_2 = ExportVariableNode(selected_variable='net-energy-production[TWh]')(input_table=out_6462_1)

    # Apply exogenous production levers 
    # 
    # Determine if we use production computed in our model or exogenous production
    # => If link-refineries-to-activity = 1 => computed production
    # => Else : exogenous production

    # OTS / FTS link-refineries-to-activity
    link_refineries_to_activity = ImportDataNode(trigram='elc', variable_name='link-refineries-to-activity')()
    # Keep only years 2050
    link_refineries_to_activity, _ = FilterDimension(dimension='Years', operation_selection='=', value_years='2050')(df=link_refineries_to_activity)
    # Group by  country, energy-carrier-category
    link_refineries_to_activity = GroupByDimensions(groupby_dimensions=['Country', 'energy-carrier-category'], aggregation_method='Sum')(df=link_refineries_to_activity)
    # endo-energy-producted[TWh] = energy-producted[TWh] * link-refineries-to-activities
    endo_energy_producted_TWh = MCDNode(operation_selection='x * y', output_name='endo-energy-producted[TWh]')(input_table_1=energy_producted_TWh_excluded, input_table_2=link_refineries_to_activity)
    # OTS / FTS fuel-production
    fuel_production = ImportDataNode(trigram='elc', variable_name='fuel-production')()
    # exo-energy-producted[TWh] = fuel-production[TWh] * (1 - link-refineries-to-activities)
    exo_energy_producted_TWh = MCDNode(operation_selection='(1-x) * y', output_name='exo-energy-producted[TWh]')(input_table_1=link_refineries_to_activity, input_table_2=fuel_production)
    # fossil-net-energy-production[TWh] = endo-energy-producted[TWh] + exo-energy-producted[TWh]
    fossil_net_energy_production_TWh = MCDNode(operation_selection='x + y', output_name='fossil-net-energy-production[TWh]')(input_table_1=endo_energy_producted_TWh, input_table_2=exo_energy_producted_TWh)

    # Net production
    # [TWh]
    # liquid

    # fossil-net-energy-production [TWh]
    fossil_net_energy_production_TWh = ExportVariableNode(selected_variable='fossil-net-energy-production[TWh]')(input_table=fossil_net_energy_production_TWh)

    # Apply technology-share levers 
    # => will determine quantity of oil produced by each technologies

    # fossil-net-energy-production [TWh]
    fossil_net_energy_production_TWh = UseVariableNode(selected_variable='fossil-net-energy-production[TWh]')(input_table=fossil_net_energy_production_TWh)
    # OTS / FTS technology-share-refineries [%]
    technology_share_refineries_percent = ImportDataNode(trigram='elc', variable_name='technology-share-refineries')()
    # net-energy-production[TWh] = fossil-net-energy-production[TWh] * technology-share[%]
    net_energy_production_TWh = MCDNode(operation_selection='x * y', output_name='net-energy-production[TWh]')(input_table_1=fossil_net_energy_production_TWh, input_table_2=technology_share_refineries_percent)
    net_energy_production_TWh_2 = pd.concat([net_energy_production_TWh_2, net_energy_production_TWh.set_index(net_energy_production_TWh.index.astype(str) + '_dup')])

    # Merge All type of fossil fuels
    # => Liquid / Gaseous / Solid

    # Net energy production
    # for refineries process
    # (all types)
    # [TWh]

    # net-energy-production [TWh]
    net_energy_production_TWh_2 = ExportVariableNode(selected_variable='net-energy-production[TWh]')(input_table=net_energy_production_TWh_2)

    # Costs CAPEX / OPEX

    # net-energy-production [TWh]
    net_energy_production_TWh_2 = UseVariableNode(selected_variable='net-energy-production[TWh]')(input_table=net_energy_production_TWh_2)
    # RCP costs-for-energy-by-way-of-prod [MEUR/kW] from TEC
    costs_for_energy_by_way_of_prod_MEUR_per_kW = ImportDataNode(trigram='tec', variable_name='costs-for-energy-by-way-of-prod', variable_type='RCP')()
    # RCP price-indices [-] from TEC
    price_indices_ = ImportDataNode(trigram='tec', variable_name='price-indices', variable_type='RCP')()
    # OTS/FTS wacc [%] from TEC
    wacc_percent = ImportDataNode(trigram='tec', variable_name='wacc')()
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')(df=wacc_percent)
    # Group by  country, years, way-of-production
    net_energy_production_TWh_3 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')(df=net_energy_production_TWh_2)
    # Compute capex for refineries capacities[kW]
    out_9527_1 = ComputeCosts(activity_variable='net-energy-production[TWh]')(df_activity=net_energy_production_TWh_3, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # Compute opex for refineries energy-production[kW]
    out_9523_1 = ComputeCosts(activity_variable='net-energy-production[TWh]', cost_type='OPEX')(df_activity=net_energy_production_TWh_3, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
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

    # Capex / Opex

    # Keep capex / opex
    MEUR = ColumnFilterNode(pattern='^Country$|^Years$|^.*capex.*$|^.*opex.*$|^cost-user$')(df=MEUR)
    # sector = refineries
    MEUR['sector'] = "refineries"
    # sector = refineries
    net_energy_production_TWh_2['sector'] = "refineries"

    # Apply efficiency-improvment levers 
    # => Gives the gross production

    # net-energy-production [TWh]
    net_energy_production_TWh = UseVariableNode(selected_variable='net-energy-production[TWh]')(input_table=net_energy_production_TWh)
    # OTS (only) energy-efficiency-fossil [-]
    energy_efficiency_fossil_ = ImportDataNode(trigram='elc', variable_name='energy-efficiency-fossil', variable_type='OTS (only)')()
    # Same as last available year
    energy_efficiency_fossil_ = AddMissingYearsNode()(df_data=energy_efficiency_fossil_)
    # OTS / FTS efficiency-imp-refineries [-]
    efficiency_imp_refineries_ = ImportDataNode(trigram='elc', variable_name='efficiency-imp-refineries')()
    # efficiency-imp-refineries [-] = 1 + efficiency-imp-refineries [-]
    efficiency_imp_refineries_percent = efficiency_imp_refineries_.assign(**{'efficiency-imp-refineries[%]': 1.0+efficiency_imp_refineries_['efficiency-imp-refineries[%]']})
    # energy-efficiency[-] = energy-efficiency-fossil * efficiency-imp-fossil[%]
    energy_efficiency = MCDNode(operation_selection='x * y', output_name='energy-efficiency[-]')(input_table_1=energy_efficiency_fossil_, input_table_2=efficiency_imp_refineries_percent)
    # gross-production[TWh] = net-energy-production[TWh] / energy-efficiency[-]
    gross_production_TWh = MCDNode(operation_selection='x / y', output_name='gross-production[TWh]')(input_table_1=net_energy_production_TWh, input_table_2=energy_efficiency)

    # Gross production
    # [TWh]

    # gross-production [TWh]
    gross_production_TWh = ExportVariableNode(selected_variable='gross-production[TWh]')(input_table=gross_production_TWh)

    # Energy - production (gross)

    # gross-production [TWh]
    gross_production_TWh = UseVariableNode(selected_variable='gross-production[TWh]')(input_table=gross_production_TWh)
    # gross-production to energy-production
    out_6722_1 = gross_production_TWh.rename(columns={'gross-production[TWh]': 'energy-production[TWh]'})
    # sector = refineries
    out_6722_1['sector'] = "refineries"

    # Energy used by the refinery process

    # primary-energy-demand[TWh] = gross-production[TWh] - fossil-net-energy-production[TWh]
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x - y', output_name='primary-energy-demand[TWh]')(input_table_1=gross_production_TWh, input_table_2=fossil_net_energy_production_TWh)

    # Calibration
    # Note : based on "gross - net" value

    # Calibration: fossil-net-production [TWh]
    fossil_net_production_TWh = ImportDataNode(trigram='elc', variable_name='fossil-net-production', variable_type='Calibration')()
    # Calibration: fossil-gross-production [TWh]
    fossil_gross_production_TWh = ImportDataNode(trigram='elc', variable_name='fossil-gross-production', variable_type='Calibration')()
    # primary-energy-demand[TWh] = fossil-gross-production[TWh] - fossil-net-production[TWh]
    primary_energy_demand_TWh = MCDNode(operation_selection='y - x', output_name='primary-energy-demand[TWh]')(input_table_1=fossil_net_production_TWh, input_table_2=fossil_gross_production_TWh)
    # Apply Calibration on total-gross-production [TWh]
    primary_energy_demand_TWh, _, out_7130_3 = CalibrationNode(data_to_be_cal='primary-energy-demand[TWh]', data_cal='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh_2, cal_table=primary_energy_demand_TWh)

    # Keep calibration rate

    # cal rate for primary-energy-demand [TWh]
    cal_rate_primary_energy_demand_TWh = UseVariableNode(selected_variable='cal_rate_primary-energy-demand[TWh]')(input_table=out_7130_3)

    # Primary energy demand
    # calibrated
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)

    # Should be set in google sheet

    # Correlation for energy-carrier-category
    out_6411_1 = TableCreatorNode(df=pd.DataFrame(columns=['energy-carrier-category', 'primary-energy-carrier'], data=[['liquid', 'liquid-ff-crudeoil'], ['gaseous', 'gaseous-ff']]))()

    # Apply fuel-switch levers 
    # => will determine the amount of primary energy carrier switched to another one

    # LEFT JOIN Add primary-energy-carrier based on energy-carrier-category
    out_6412_1 = JoinerNode(joiner='left', left_input=['energy-carrier-category'], right_input=['energy-carrier-category'])(df_left=primary_energy_demand_TWh, df_right=out_6411_1)
    # Remove energy-carrier-category
    out_6412_1 = ColumnFilterNode(columns_to_drop=['energy-carrier-category'])(df=out_6412_1)
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = ImportDataNode(trigram='elc', variable_name='fuel-switch')()
    # energy-carrier to primary-energy-carrier TO CHANGE IN INPUT DATA  INSTEAD!!!
    out_7122_1 = fuel_switch_percent.rename(columns={'energy-carrier-from': 'primary-energy-carrier-from', 'energy-carrier-to': 'primary-energy-carrier-to'})

    def helper_7140(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table['primary-energy-carrier-to'] == "gaseous")
        output_table.loc[mask, 'primary-energy-carrier-to'] = "gaseous-ff"
        mask = (output_table['primary-energy-carrier-from'] == "liquid")
        output_table.loc[mask, 'primary-energy-carrier-from'] = "liquid-ff-crudeoil"
        return output_table
    # replace gaseous by gaseous-ff and liquid by liquid-ff-crudeoil TO DO IN INPUT DATA INSTEAD
    out_7140_1 = helper_7140(input_table=out_7122_1)
    # Fuel Switch liquid to gaseous
    out_7139_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='liquid', category_to_selected='gaseous')(demand_table=out_6412_1, switch_table=out_7140_1, correlation_table=ratio)
    # Fuel Switch ff to biofuels (only gaseous)
    out_9519_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier')(demand_table=out_7139_1, switch_table=out_7140_1, correlation_table=ratio)
    # Fuel Switch ff to efuels (only gaseous)
    out_9520_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='synfuels')(demand_table=out_9519_1, switch_table=out_7140_1, correlation_table=ratio)

    # Energy demand
    # for refineries process
    # (liquid)
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=out_9520_1)

    # Energy demand
    # for refineries process
    # (all types)
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)

    # Primary energy demand for electricity production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)
    # Group by  country, years, primagy-energy-carrier, way-of-prod
    primary_energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier-category'], aggregation_method='Sum')(df=primary_energy_demand_TWh_2)
    # sector = refineries
    primary_energy_demand_TWh['sector'] = "refineries"

    # Emissions
    # For carbon-capture and energy required for carbon capture : apply emission factor of fossil fuels !
    # As emission factor of bio- and syn-fuels are set to 0 to represent the fact that CO2 emissions were previously captured by vegetation / H2 syn, it is not possible to determine CO2 captured based on these emissions.
    # In order to have these values : we assume bio- / syn-fuels emit the same way as there fossil fuel equivalent and apply, then, carbon capture and energy required for CC on these emissions
    # => Liquid fuels (bio / syn / ff) : liquid fossil emission factor
    # => Gaseous fuels (bio / syn / ff) : gaseous fossil emission factor
    # => Solid fuels (bio / syn / ff) : solid fossil emission factor

    # RCP elc-emission-factor [g/kWh]
    elc_emission_factor_g_per_kWh = ImportDataNode(trigram='elc', variable_name='elc-emission-factor', variable_type='RCP')()
    # Convert Unit g/kWh to Mt/TWh (* 0.001)
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_g_per_kWh.drop(columns='elc-emission-factor[g/kWh]').assign(**{'elc-emission-factor[Mt/TWh]': elc_emission_factor_g_per_kWh['elc-emission-factor[g/kWh]'] * 0.001})
    # emissions[Mt] = primary-energy-demand[TWh] * emission-factor[Mt/TWh]
    emissions_Mt = MCDNode(operation_selection='x * y', output_name='emissions[Mt]')(input_table_1=primary_energy_demand_TWh_2, input_table_2=elc_emission_factor_Mt_per_TWh)
    # Group by  country, years, gaes, way-of-production
    emissions_Mt_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'gaes'], aggregation_method='Sum')(df=emissions_Mt)

    # Calibration

    # Calibration: ref-emissions [Mt]
    ref_emissions_Mt = ImportDataNode(trigram='elc', variable_name='ref-emissions', variable_type='Calibration')()
    # Apply Calibration on emissions[Mt]
    _, out_7101_2, out_7101_3 = CalibrationNode(data_to_be_cal='emissions[Mt]', data_cal='ref-emissions[Mt]')(input_table=emissions_Mt_2, cal_table=ref_emissions_Mt)
    # cal rate for emissions[Mt]
    cal_rate_emissions_Mt = UseVariableNode(selected_variable='cal_rate_emissions[Mt]')(input_table=out_7101_3)
    cal_rate = pd.concat([cal_rate_primary_energy_demand_TWh, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])
    # emissions[Mt] (replace) = emissions[Mt] * cal_rate  cal_rate set to 1 if missing
    emissions_Mt = MCDNode(operation_selection='x * y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)(input_table_1=emissions_Mt, input_table_2=out_7101_2)
    # Set to 0 (no emissions)
    emissions_Mt = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=emissions_Mt)

    # Apply CC levers 
    # => determine the quantity of CO2 emissions than can be capture
    # => decrease CO2 emissions

    # OTS / FTS : CCS-ratio-refineries
    CCS_ratio_refineries = ImportDataNode(trigram='elc', variable_name='CCS-ratio-refineries')()

    # Emissions

    # emissions [Mt]
    emissions_Mt = UseVariableNode(selected_variable='emissions[Mt]')(input_table=emissions_Mt)
    # Group by  country, years, gaes, way-of-production, primary-energy-carrier
    emissions_Mt = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier-category', 'gaes'], aggregation_method='Sum')(df=emissions_Mt)
    # sector = refineries
    emissions_Mt['sector'] = "refineries"
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_Mt_per_TWh.loc[~elc_emission_factor_Mt_per_TWh['primary-energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'liquid-bio'])].copy()

    def helper_8017(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        values_needed = {  # LEFT = to be add if missing - RIGHT = values to use when missing
        "solid-biomass": "solid-ff",
        "liquid-bio": "liquid-ff",
        "liquid-syn": "liquid-ff",
        "gaseous-bio": "gaseous-ff",
        "gaseous-syn": "gaseous-ff",
        }
        
        # Get existing energy-carrier list
        enegy_carrier_list = output_table["primary-energy-carrier"].unique()
        
        # For values needed : if not in list => copy values from corresponding
        for val in values_needed.keys():
            if val not in enegy_carrier_list:
                val_mask = values_needed[val]
                mask = (output_table["primary-energy-carrier"].str.find(val_mask) >= 0)
                temp = output_table.loc[mask,:]
                temp["primary-energy-carrier"] = val
                output_table = pd.concat([output_table,temp], ignore_index=True)
        return output_table
    # syn / bio : add them with ff values
    out_8017_1 = helper_8017(input_table=elc_emission_factor_Mt_per_TWh)
    # insitu-emissions[Mt] = primary-energy-demand[TWh] * elc-emission-factor [Mt/TWh]
    insitu_emissions_Mt = MCDNode(operation_selection='x * y', output_name='insitu-emissions[Mt]')(input_table_1=primary_energy_demand_TWh_2, input_table_2=out_8017_1)
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier
    insitu_emissions_Mt = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')(df=insitu_emissions_Mt)
    # Apply cal-rate on insitu-emissions[Mt] 
    insitu_emissions_Mt = MCDNode(operation_selection='x * y', output_name='insitu-emissions[Mt]')(input_table_1=insitu_emissions_Mt, input_table_2=out_7101_2)
    # Set to 0 (no emissions)
    insitu_emissions_Mt = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=insitu_emissions_Mt)
    # Keep only CO2
    insitu_emissions_Mt = insitu_emissions_Mt.loc[insitu_emissions_Mt['gaes'].isin(['CO2'])].copy()
    # CC[Mt] = insitu-emissions[Mt] * CCS-ratio-refineries[%]  if missing elec-CCS-ratio set to 0
    CC_Mt = MCDNode(operation_selection='x * y', output_name='CC[Mt]', fill_value_bool='Left [x] Outer Join')(input_table_1=insitu_emissions_Mt, input_table_2=CCS_ratio_refineries)
    # CC[Mt]
    CC_Mt = ExportVariableNode(selected_variable='CC[Mt]')(input_table=CC_Mt)

    # Compute CC energy demand 
    # This is already accounted in the primary energy demand of the sector through energy-imp-efficiency factor.
    # This is only required to create a graph.

    # Apply CC energy efficiency levers 
    # (linked to ind energy efficiency !)
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS CC-energy-efficiency [%]  FROM INDUSTRY (we should transfer data to TECHNOLOGY)
    CC_energy_efficiency_percent = ImportDataNode(trigram='ind', variable_name='CC-energy-efficiency')()
    # RCP CC-specific-energy-consumption [TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt = ImportDataNode(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')()
    # Keep co2-concentration = low (for power production concentration is always low)
    CC_specific_energy_consumption_TWh_per_Mt = CC_specific_energy_consumption_TWh_per_Mt.loc[CC_specific_energy_consumption_TWh_per_Mt['co2-concentration'].isin(['low'])].copy()
    # CC-specific-energy-consumption[TWh/Mt] (replace) = CC-specific-energy-consumption[TWh/Mt] * (1 - CC-energy-efficiency[%])
    CC_specific_energy_consumption_TWh_per_Mt = MCDNode(operation_selection='x * (1-y)', output_name='CC-specific-energy-consumption[TWh/Mt]')(input_table_1=CC_specific_energy_consumption_TWh_per_Mt, input_table_2=CC_energy_efficiency_percent)
    # Group by Country co2-concentration Years (sum)  only eletrcicity as specific consumption (other = set to 0) and we want to remove column "energy-carrier"
    CC_specific_energy_consumption_TWh_per_Mt = GroupByDimensions(groupby_dimensions=['Country', 'co2-concentration', 'Years'], aggregation_method='Sum')(df=CC_specific_energy_consumption_TWh_per_Mt)

    # Emissions Capture

    # CC [Mt]
    CC_Mt_2 = UseVariableNode(selected_variable='CC[Mt]')(input_table=CC_Mt)
    # sector = refineries
    CC_Mt_2['sector'] = "refineries"
    # Keep CO2
    CC_Mt = CC_Mt.loc[CC_Mt['gaes'].isin(['CO2'])].copy()
    # primary-energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x * y', output_name='primary-energy-demand[TWh]')(input_table_1=CC_Mt, input_table_2=CC_specific_energy_consumption_TWh_per_Mt)
    # primary-energy-demand [TWh]  only for Carbon capture  DO NOT ADD TO  primary-energy-demand to we avoid loop to electricity sector (assumed to be low and not in the total)
    primary_energy_demand_TWh_2 = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh_2)

    # CC primary energy demand

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh_2)

    return cal_rate, out_6722_1, energy_imported_TWh_2, net_energy_production_TWh_2, MEUR, primary_energy_demand_TWh, emissions_Mt, CC_Mt_2, primary_energy_demand_TWh_2


