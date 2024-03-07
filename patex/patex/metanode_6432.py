import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


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
    out_9505_1 = pd.DataFrame(columns=['category-from', 'primary-energy-carrier-from', 'category-to', 'primary-energy-carrier-to'], data=[['liquid', 'liquid-ff-crudeoil', 'gaseous', 'gaseous-ff'], ['fffuels', 'gaseous-ff', 'biofuels', 'gaseous-bio'], ['fffuels', 'gaseous-ff', 'synfuels', 'gaseous-syn']])
    # ratio[-] = 1
    ratio = out_9505_1.assign(**{'ratio[-]': 1.0})

    # Fossil fuel demand (from other sectors)

    # Energy demand [TWh]
    energy_demand_TWh = use_variable(input_table=port_01, selected_variable='energy-demand[TWh]')
    # Keep only energy-carrier fossil-fuels
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-natural', 'liquid-ff', 'solid-ff-coal', 'gaseous-ff', 'liquid-ff-diesel', 'liquid-ff-gasoline', 'liquid-ff-kerosene', 'liquid-ff-marinefueloil', 'liquid-ff-oil'])].copy()
    # Group by  country, years, energy-carrier-category
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier-category'], aggregation_method='Sum')

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import -refineries
    energy_net_import_refineries = import_data(trigram='elc', variable_name='energy-net-import-refineries')
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_net_import_refineries, operation_selection='x * y', output_name='energy-imported[TWh]')

    # Formating data for other modules + Pathway Explorer

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # sector = refineries
    energy_imported_TWh_2['sector'] = "refineries"
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh, operation_selection='x - y', output_name='energy-producted[TWh]')
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
    net_energy_production_TWh_2 = export_variable(input_table=out_6462_1, selected_variable='net-energy-production[TWh]')

    # Apply exogenous production levers 
    # 
    # Determine if we use production computed in our model or exogenous production
    # => If link-refineries-to-activity = 1 => computed production
    # => Else : exogenous production

    # OTS / FTS link-refineries-to-activity
    link_refineries_to_activity = import_data(trigram='elc', variable_name='link-refineries-to-activity')
    # Keep only years 2050
    link_refineries_to_activity, _ = filter_dimension(df=link_refineries_to_activity, dimension='Years', operation_selection='=', value_years='2050')
    # Group by  country, energy-carrier-category
    link_refineries_to_activity = group_by_dimensions(df=link_refineries_to_activity, groupby_dimensions=['Country', 'energy-carrier-category'], aggregation_method='Sum')
    # endo-energy-producted[TWh] = energy-producted[TWh] * link-refineries-to-activities
    endo_energy_producted_TWh = mcd(input_table_1=energy_producted_TWh_excluded, input_table_2=link_refineries_to_activity, operation_selection='x * y', output_name='endo-energy-producted[TWh]')
    # OTS / FTS fuel-production
    fuel_production = import_data(trigram='elc', variable_name='fuel-production')
    # exo-energy-producted[TWh] = fuel-production[TWh] * (1 - link-refineries-to-activities)
    exo_energy_producted_TWh = mcd(input_table_1=link_refineries_to_activity, input_table_2=fuel_production, operation_selection='(1-x) * y', output_name='exo-energy-producted[TWh]')
    # fossil-net-energy-production[TWh] = endo-energy-producted[TWh] + exo-energy-producted[TWh]
    fossil_net_energy_production_TWh = mcd(input_table_1=endo_energy_producted_TWh, input_table_2=exo_energy_producted_TWh, operation_selection='x + y', output_name='fossil-net-energy-production[TWh]')

    # Net production
    # [TWh]
    # liquid

    # fossil-net-energy-production [TWh]
    fossil_net_energy_production_TWh = export_variable(input_table=fossil_net_energy_production_TWh, selected_variable='fossil-net-energy-production[TWh]')

    # Apply technology-share levers 
    # => will determine quantity of oil produced by each technologies

    # fossil-net-energy-production [TWh]
    fossil_net_energy_production_TWh = use_variable(input_table=fossil_net_energy_production_TWh, selected_variable='fossil-net-energy-production[TWh]')
    # OTS / FTS technology-share-refineries [%]
    technology_share_refineries_percent = import_data(trigram='elc', variable_name='technology-share-refineries')
    # net-energy-production[TWh] = fossil-net-energy-production[TWh] * technology-share[%]
    net_energy_production_TWh = mcd(input_table_1=fossil_net_energy_production_TWh, input_table_2=technology_share_refineries_percent, operation_selection='x * y', output_name='net-energy-production[TWh]')
    net_energy_production_TWh_2 = pd.concat([net_energy_production_TWh_2, net_energy_production_TWh.set_index(net_energy_production_TWh.index.astype(str) + '_dup')])

    # Merge All type of fossil fuels
    # => Liquid / Gaseous / Solid

    # Net energy production
    # for refineries process
    # (all types)
    # [TWh]

    # net-energy-production [TWh]
    net_energy_production_TWh_2 = export_variable(input_table=net_energy_production_TWh_2, selected_variable='net-energy-production[TWh]')

    # Costs CAPEX / OPEX

    # net-energy-production [TWh]
    net_energy_production_TWh_2 = use_variable(input_table=net_energy_production_TWh_2, selected_variable='net-energy-production[TWh]')
    # RCP costs-for-energy-by-way-of-prod [MEUR/kW] from TEC
    costs_for_energy_by_way_of_prod_MEUR_per_kW = import_data(trigram='tec', variable_name='costs-for-energy-by-way-of-prod', variable_type='RCP')
    # RCP price-indices [-] from TEC
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')
    # OTS/FTS wacc [%] from TEC
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Group by  country, years, way-of-production
    net_energy_production_TWh_3 = group_by_dimensions(df=net_energy_production_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Compute capex for refineries capacities[kW]
    out_9527_1 = compute_costs(df_activity=net_energy_production_TWh_3, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='net-energy-production[TWh]')
    # Compute opex for refineries energy-production[kW]
    out_9523_1 = compute_costs(df_activity=net_energy_production_TWh_3, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='net-energy-production[TWh]', cost_type='OPEX')
    # RCP energy-production-cost-user [-]
    energy_production_cost_user_ = import_data(trigram='elc', variable_name='energy-production-cost-user', variable_type='RCP')
    # opex[MEUR] = opex[MEUR] * energy-production-cost-user[-]
    opex_MEUR = mcd(input_table_1=out_9523_1, input_table_2=energy_production_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    # Group by  Country, Years (sum)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    # capex[MEUR] = capex[MEUR] * energy-production-cost-user[-]
    capex_MEUR = mcd(input_table_1=energy_production_cost_user_, input_table_2=out_9527_1, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by  Country, Years (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'cost-user', 'Years'], aggregation_method='Sum')
    MEUR = pd.concat([opex_MEUR, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])

    # Capex / Opex

    # Keep capex / opex
    MEUR = column_filter(df=MEUR, pattern='^Country$|^Years$|^.*capex.*$|^.*opex.*$|^cost-user$')
    # sector = refineries
    MEUR['sector'] = "refineries"
    # sector = refineries
    net_energy_production_TWh_2['sector'] = "refineries"

    # Apply efficiency-improvment levers 
    # => Gives the gross production

    # net-energy-production [TWh]
    net_energy_production_TWh = use_variable(input_table=net_energy_production_TWh, selected_variable='net-energy-production[TWh]')
    # OTS (only) energy-efficiency-fossil [-]
    energy_efficiency_fossil_ = import_data(trigram='elc', variable_name='energy-efficiency-fossil', variable_type='OTS (only)')
    # Same as last available year
    energy_efficiency_fossil_ = add_missing_years(df_data=energy_efficiency_fossil_)
    # OTS / FTS efficiency-imp-refineries [-]
    efficiency_imp_refineries_ = import_data(trigram='elc', variable_name='efficiency-imp-refineries')
    # efficiency-imp-refineries [-] = 1 + efficiency-imp-refineries [-]
    efficiency_imp_refineries_percent = efficiency_imp_refineries_.assign(**{'efficiency-imp-refineries[%]': 1.0+efficiency_imp_refineries_['efficiency-imp-refineries[%]']})
    # energy-efficiency[-] = energy-efficiency-fossil * efficiency-imp-fossil[%]
    energy_efficiency = mcd(input_table_1=energy_efficiency_fossil_, input_table_2=efficiency_imp_refineries_percent, operation_selection='x * y', output_name='energy-efficiency[-]')
    # gross-production[TWh] = net-energy-production[TWh] / energy-efficiency[-]
    gross_production_TWh = mcd(input_table_1=net_energy_production_TWh, input_table_2=energy_efficiency, operation_selection='x / y', output_name='gross-production[TWh]')

    # Gross production
    # [TWh]

    # gross-production [TWh]
    gross_production_TWh = export_variable(input_table=gross_production_TWh, selected_variable='gross-production[TWh]')

    # Energy - production (gross)

    # gross-production [TWh]
    gross_production_TWh = use_variable(input_table=gross_production_TWh, selected_variable='gross-production[TWh]')
    # gross-production to energy-production
    out_6722_1 = gross_production_TWh.rename(columns={'gross-production[TWh]': 'energy-production[TWh]'})
    # sector = refineries
    out_6722_1['sector'] = "refineries"

    # Energy used by the refinery process

    # primary-energy-demand[TWh] = gross-production[TWh] - fossil-net-energy-production[TWh]
    primary_energy_demand_TWh_2 = mcd(input_table_1=gross_production_TWh, input_table_2=fossil_net_energy_production_TWh, operation_selection='x - y', output_name='primary-energy-demand[TWh]')

    # Calibration
    # Note : based on "gross - net" value

    # Calibration: fossil-net-production [TWh]
    fossil_net_production_TWh = import_data(trigram='elc', variable_name='fossil-net-production', variable_type='Calibration')
    # Calibration: fossil-gross-production [TWh]
    fossil_gross_production_TWh = import_data(trigram='elc', variable_name='fossil-gross-production', variable_type='Calibration')
    # primary-energy-demand[TWh] = fossil-gross-production[TWh] - fossil-net-production[TWh]
    primary_energy_demand_TWh = mcd(input_table_1=fossil_net_production_TWh, input_table_2=fossil_gross_production_TWh, operation_selection='y - x', output_name='primary-energy-demand[TWh]')
    # Apply Calibration on total-gross-production [TWh]
    primary_energy_demand_TWh, _, out_7130_3 = calibration(input_table=primary_energy_demand_TWh_2, cal_table=primary_energy_demand_TWh, data_to_be_cal='primary-energy-demand[TWh]', data_cal='primary-energy-demand[TWh]')

    # Keep calibration rate

    # cal rate for primary-energy-demand [TWh]
    cal_rate_primary_energy_demand_TWh = use_variable(input_table=out_7130_3, selected_variable='cal_rate_primary-energy-demand[TWh]')

    # Primary energy demand
    # calibrated
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = export_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')

    # Should be set in google sheet

    # Correlation for energy-carrier-category
    out_6411_1 = pd.DataFrame(columns=['energy-carrier-category', 'primary-energy-carrier'], data=[['liquid', 'liquid-ff-crudeoil'], ['gaseous', 'gaseous-ff']])

    # Apply fuel-switch levers 
    # => will determine the amount of primary energy carrier switched to another one

    # LEFT JOIN Add primary-energy-carrier based on energy-carrier-category
    out_6412_1 = joiner(df_left=primary_energy_demand_TWh, df_right=out_6411_1, joiner='left', left_input=['energy-carrier-category'], right_input=['energy-carrier-category'])
    # Remove energy-carrier-category
    out_6412_1 = column_filter(df=out_6412_1, columns_to_drop=['energy-carrier-category'])
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = import_data(trigram='elc', variable_name='fuel-switch')
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
    out_7139_1 = x_switch(demand_table=out_6412_1, switch_table=out_7140_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='liquid', category_to_selected='gaseous')
    # Fuel Switch ff to biofuels (only gaseous)
    out_9519_1 = x_switch(demand_table=out_7139_1, switch_table=out_7140_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier')
    # Fuel Switch ff to efuels (only gaseous)
    out_9520_1 = x_switch(demand_table=out_9519_1, switch_table=out_7140_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='synfuels')

    # Energy demand
    # for refineries process
    # (liquid)
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = export_variable(input_table=out_9520_1, selected_variable='primary-energy-demand[TWh]')

    # Energy demand
    # for refineries process
    # (all types)
    # [TWh]

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = export_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')

    # Primary energy demand for electricity production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, primagy-energy-carrier, way-of-prod
    primary_energy_demand_TWh = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier-category'], aggregation_method='Sum')
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
    elc_emission_factor_g_per_kWh = import_data(trigram='elc', variable_name='elc-emission-factor', variable_type='RCP')
    # Convert Unit g/kWh to Mt/TWh (* 0.001)
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_g_per_kWh.drop(columns='elc-emission-factor[g/kWh]').assign(**{'elc-emission-factor[Mt/TWh]': elc_emission_factor_g_per_kWh['elc-emission-factor[g/kWh]'] * 0.001})
    # emissions[Mt] = primary-energy-demand[TWh] * emission-factor[Mt/TWh]
    emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh_2, input_table_2=elc_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  country, years, gaes, way-of-production
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'gaes'], aggregation_method='Sum')

    # Calibration

    # Calibration: ref-emissions [Mt]
    ref_emissions_Mt = import_data(trigram='elc', variable_name='ref-emissions', variable_type='Calibration')
    # Apply Calibration on emissions[Mt]
    _, out_7101_2, out_7101_3 = calibration(input_table=emissions_Mt_2, cal_table=ref_emissions_Mt, data_to_be_cal='emissions[Mt]', data_cal='ref-emissions[Mt]')
    # cal rate for emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out_7101_3, selected_variable='cal_rate_emissions[Mt]')
    cal_rate = pd.concat([cal_rate_primary_energy_demand_TWh, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])
    # emissions[Mt] (replace) = emissions[Mt] * cal_rate  cal_rate set to 1 if missing
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_7101_2, operation_selection='x * y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Set to 0 (no emissions)
    emissions_Mt = missing_value(df=emissions_Mt, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Apply CC levers 
    # => determine the quantity of CO2 emissions than can be capture
    # => decrease CO2 emissions

    # OTS / FTS : CCS-ratio-refineries
    CCS_ratio_refineries = import_data(trigram='elc', variable_name='CCS-ratio-refineries')

    # Emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Group by  country, years, gaes, way-of-production, primary-energy-carrier
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier-category', 'gaes'], aggregation_method='Sum')
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
    insitu_emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh_2, input_table_2=out_8017_1, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier
    insitu_emissions_Mt = group_by_dimensions(df=insitu_emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')
    # Apply cal-rate on insitu-emissions[Mt] 
    insitu_emissions_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=out_7101_2, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Set to 0 (no emissions)
    insitu_emissions_Mt = missing_value(df=insitu_emissions_Mt, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Keep only CO2
    insitu_emissions_Mt = insitu_emissions_Mt.loc[insitu_emissions_Mt['gaes'].isin(['CO2'])].copy()
    # CC[Mt] = insitu-emissions[Mt] * CCS-ratio-refineries[%]  if missing elec-CCS-ratio set to 0
    CC_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=CCS_ratio_refineries, operation_selection='x * y', output_name='CC[Mt]', fill_value_bool='Left [x] Outer Join')
    # CC[Mt]
    CC_Mt = export_variable(input_table=CC_Mt, selected_variable='CC[Mt]')

    # Compute CC energy demand 
    # This is already accounted in the primary energy demand of the sector through energy-imp-efficiency factor.
    # This is only required to create a graph.

    # Apply CC energy efficiency levers 
    # (linked to ind energy efficiency !)
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS CC-energy-efficiency [%]  FROM INDUSTRY (we should transfer data to TECHNOLOGY)
    CC_energy_efficiency_percent = import_data(trigram='ind', variable_name='CC-energy-efficiency')
    # RCP CC-specific-energy-consumption [TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt = import_data(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')
    # Keep co2-concentration = low (for power production concentration is always low)
    CC_specific_energy_consumption_TWh_per_Mt = CC_specific_energy_consumption_TWh_per_Mt.loc[CC_specific_energy_consumption_TWh_per_Mt['co2-concentration'].isin(['low'])].copy()
    # CC-specific-energy-consumption[TWh/Mt] (replace) = CC-specific-energy-consumption[TWh/Mt] * (1 - CC-energy-efficiency[%])
    CC_specific_energy_consumption_TWh_per_Mt = mcd(input_table_1=CC_specific_energy_consumption_TWh_per_Mt, input_table_2=CC_energy_efficiency_percent, operation_selection='x * (1-y)', output_name='CC-specific-energy-consumption[TWh/Mt]')
    # Group by Country co2-concentration Years (sum)  only eletrcicity as specific consumption (other = set to 0) and we want to remove column "energy-carrier"
    CC_specific_energy_consumption_TWh_per_Mt = group_by_dimensions(df=CC_specific_energy_consumption_TWh_per_Mt, groupby_dimensions=['Country', 'co2-concentration', 'Years'], aggregation_method='Sum')

    # Emissions Capture

    # CC [Mt]
    CC_Mt_2 = use_variable(input_table=CC_Mt, selected_variable='CC[Mt]')
    # sector = refineries
    CC_Mt_2['sector'] = "refineries"
    # Keep CO2
    CC_Mt = CC_Mt.loc[CC_Mt['gaes'].isin(['CO2'])].copy()
    # primary-energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    primary_energy_demand_TWh_2 = mcd(input_table_1=CC_Mt, input_table_2=CC_specific_energy_consumption_TWh_per_Mt, operation_selection='x * y', output_name='primary-energy-demand[TWh]')
    # primary-energy-demand [TWh]  only for Carbon capture  DO NOT ADD TO  primary-energy-demand to we avoid loop to electricity sector (assumed to be low and not in the total)
    primary_energy_demand_TWh_2 = export_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')

    # CC primary energy demand

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')

    return cal_rate, out_6722_1, energy_imported_TWh_2, net_energy_production_TWh_2, MEUR, primary_energy_demand_TWh, emissions_Mt, CC_Mt_2, primary_energy_demand_TWh_2


