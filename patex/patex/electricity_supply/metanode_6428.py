import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def metanode_6428(port_01):
    # In future :
    # 
    # Account for these savings in costs (// industry)


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-production[TWh]



    module_name = 'electricity_supply'

    # HEAT Production

    # Heat production (and primary-energy-demand)

    # Apply fuel-switch levers 
    # => Change the use of different fuels :
    # Ex : 
    # solid-ff to solid-bio
    # liquid-ff to liquid-bio
    # gas-ff to gas-bio
    # waste non-res to waste-res and solid-bio
    # 
    # Note : 
    # We take into account of the efficiency ratio for primary energy demand (but not for final energy demand !).
    # Therefore ratio = 1 is used for "production" and ratio linked to efficiency is used for primary energy demand.
    # With this methology :
    # - Total heat production will not change before and after full switch
    # - Total primary fuel demand will be influenced by fuel switch and may result in different total value before and after fuel switch.

    # Correlation : Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for fuel mix HARD CODE
    out_9506_1 = pd.DataFrame(columns=['category-from', 'primary-energy-carrier-from', 'category-to', 'primary-energy-carrier-to'], data=[['fffuels', 'liquid-ff', 'biofuels', 'liquid-bio'], ['fffuels', 'solid-ff-coal', 'biofuels', 'solid-biomass'], ['fffuels', 'gaseous-ff-natural', 'biofuels', 'gaseous-bio'], ['fffuels', 'liquid-ff', 'elec-hp', 'RES-electricity-hp'], ['fffuels', 'solid-ff-coal', 'elec-hp', 'RES-electricity-hp'], ['fffuels', 'gaseous-ff-natural', 'elec-hp', 'RES-electricity-hp'], ['nonres', 'solid-waste-nonres', 'res', 'solid-waste-res'], ['nonres', 'solid-waste-nonres', 'biofuels', 'solid-biomass']])
    # ratio[-] = 1
    ratio = out_9506_1.assign(**{'ratio[-]': 1.0})

    # Heat demand (from other sectors)

    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=port_01, selected_variable='energy-demand[TWh]')
    # Keep only energy-carrier heat
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['heat'])].copy()
    # Group by  country, years and energy-carrier
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # OTS (only) ratio-self-heat-consumption [-]
    ratio_self_heat_consumption_ = import_data(trigram='elc', variable_name='ratio-self-heat-consumption', variable_type='OTS (only)')
    # Same as last available year
    ratio_self_heat_consumption_ = add_missing_years(df_data=ratio_self_heat_consumption_)
    # ratio-self-heat-consumption[%] = (replace) 1 + ratio-self-heat-consumption[%]
    ratio_self_heat_consumption_percent = ratio_self_heat_consumption_.assign(**{'ratio-self-heat-consumption[%]': 1.0+ratio_self_heat_consumption_['ratio-self-heat-consumption[%]']})
    # energy-demand[TWh] = energy-demand[TWh] * ratio-self-heat-consump.[%])
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=ratio_self_heat_consumption_percent, operation_selection='x * y', output_name='energy-demand[TWh]')

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import
    energy_net_import = import_data(trigram='elc', variable_name='energy-net-import')
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_net_import, operation_selection='x * y', output_name='energy-imported[TWh]')
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh, operation_selection='x - y', output_name='energy-producted[TWh]')

    # Formating data for other modules + Pathway Explorer

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # sector = heat
    energy_imported_TWh['sector'] = "heat"

    # CHP technology

    # Apply CHP capacity levers 
    # => determine level of heat / electricity that can be produced with CHP

    # OTS / FTS capacity [GW]
    capacity_GW = import_data(trigram='elc', variable_name='capacity')
    # Unit conversion GW to TWh
    capacity_TWh = capacity_GW.drop(columns='capacity[GW]').assign(**{'capacity[TWh]': capacity_GW['capacity[GW]'] * 8.766})
    # HTS capacity-factor [%]
    capacity_factor_percent = import_data(trigram='elc', variable_name='capacity-factor', variable_type='HTS')
    # Same as last available year
    capacity_factor_percent = add_missing_years(df_data=capacity_factor_percent)

    def helper_6124(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        return output_table
    # Join capacity and capacity-factor in one table
    out_6124_1 = helper_6124(input_table_1=capacity_TWh, input_table_2=capacity_factor_percent)
    # chp-energy-production[TWh] = capacity[TWh] * capacity-factor[%]
    chp_energy_production_TWh = mcd(input_table_1=capacity_TWh, input_table_2=capacity_factor_percent, operation_selection='x * y', output_name='chp-energy-production[TWh]')

    # Decrease capacity-factor if :
    # production with CHP > needed production of heat

    # OTS (only) chp-pth-ratio [-]
    chp_pth_ratio_ = import_data(trigram='elc', variable_name='chp-pth-ratio', variable_type='OTS (only)')
    # Same as last available year
    chp_pth_ratio_ = add_missing_years(df_data=chp_pth_ratio_)

    def helper_6698(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        return output_table
    # Join chp-pth-ratio
    out_6698_1 = helper_6698(input_table_1=out_6124_1, input_table_2=chp_pth_ratio_)
    # heat-energy-production[TWh] = chp-energy-production[TWh] / chp-pth-ratio[%]
    heat_energy_production_TWh = mcd(input_table_1=chp_energy_production_TWh, input_table_2=chp_pth_ratio_, operation_selection='x / y', output_name='heat-energy-production[TWh]')
    # If missing set to 0
    heat_energy_production_TWh = missing_value(df=heat_energy_production_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    def helper_6697(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        return output_table
    # Join energy-production, capacity and capacity-factor in one table
    out_6697_1 = helper_6697(input_table_1=out_6698_1, input_table_2=heat_energy_production_TWh)

    def helper_6696(input_table_1, input_table_2) -> pd.DataFrame:
        import numpy as np
        
        # Get usefull flow variable
        baseyear = Globals.get().base_year
        # Get usefull column names
        ## energy-produced = energy needed
        ## energy-production = energy produced with CHP
        col_energy_needed = 'energy-producted[TWh]'
        col_energy_CHP = 'heat-energy-production[TWh]'
        col_capacity = 'capacity[TWh]'
        col_capacity_factor = 'capacity-factor[%]'
        
        # Get common columns
        common_cols = [col for col in input_table_1.head() if col in input_table_2.head()]
        # Merge on common cols
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_cols)
        
        # If demand = 0 => force chp-pth ratio == 0 (no heat produced !)
        mask = (output_table[col_energy_needed] == 0)
        output_table.loc[mask, 'chp-pth-ratio[-]'] = 0
        
        # Compare energy needed with energy produced by CHP => if CHP > needed : reduced capacity-factor
        ## For years > baseyear only ! And if chp-pth ratio > 0 (no need to change capacity-factor if there is no energy produced by CHP)
        ## Recompute capacity factor
        output_table["remaining"] = output_table[col_energy_needed] - output_table[col_energy_CHP]
        mask = (output_table['Years'].astype(int) > int(baseyear)) & (output_table["remaining"] < 0) & (output_table['chp-pth-ratio[-]'] != 0)
        mask1 = mask & (output_table[col_capacity] != 0)
        mask2 = mask & (output_table[col_capacity] == 0) 
        output_table.loc[mask1, col_capacity_factor] = (output_table.loc[mask1, col_energy_needed]) / (output_table.loc[mask1, col_capacity]) * output_table.loc[mask1, 'chp-pth-ratio[-]'] 
        output_table.loc[mask2, col_capacity_factor] = 0
        
        # If capacity-factor < 0 => set to 0
        mask = (output_table[col_capacity_factor] < 0)
        output_table.loc[mask, col_capacity_factor] = 0
        return output_table
    # Decrease capacity-factor
    out_6696_1 = helper_6696(input_table_1=energy_producted_TWh, input_table_2=out_6697_1)
    # chp-pth-ratio[-]
    chp_pth_ratio = export_variable(input_table=out_6696_1, selected_variable='chp-pth-ratio[-]')
    # capacity[TWh]
    capacity_TWh = export_variable(input_table=out_6696_1, selected_variable='capacity[TWh]')
    # capacity-factor[%]
    capacity_factor_percent = export_variable(input_table=out_6696_1, selected_variable='capacity-factor[%]')
    # chp-energy-production[TWh] = capacity[TWh] * capacity-factor[%]
    chp_energy_production_TWh = mcd(input_table_1=capacity_factor_percent, input_table_2=capacity_TWh, operation_selection='x * y', output_name='chp-energy-production[TWh]')

    # Distinguish elec and heat production and apply fuel mix

    # Heat production

    # heat-energy-production[TWh] = chp-energy-production[TWh] / chp-pth-ratio[%]
    heat_energy_production_TWh = mcd(input_table_1=chp_energy_production_TWh, input_table_2=chp_pth_ratio, operation_selection='x / y', output_name='heat-energy-production[TWh]')
    # If missing set to 0
    heat_energy_production_TWh = missing_value(df=heat_energy_production_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # If < 0 Set to 0
    mask = heat_energy_production_TWh['heat-energy-production[TWh]']<0
    heat_energy_production_TWh.loc[mask, 'heat-energy-production[TWh]'] = 0
    heat_energy_production_TWh.loc[~mask, 'heat-energy-production[TWh]'] = heat_energy_production_TWh.loc[~mask, 'heat-energy-production[TWh]']

    # Heat-only technology
    # Heat only plant are used to produced heat when all heat demand can not be covered by CHP only

    # Heat production
    # If remaining heat demand after CHP production, heat is produced with heat-only plant

    # heat-energy-production[TWh] (replace) = energy-producted[TWh] - heat-energy-production[TWh]
    heat_energy_production_TWh_2 = mcd(input_table_1=energy_producted_TWh, input_table_2=heat_energy_production_TWh, operation_selection='x - y', output_name='heat-energy-production[TWh]')
    # If < 0 Set to 0
    mask = heat_energy_production_TWh_2['heat-energy-production[TWh]']<0
    heat_energy_production_TWh_2.loc[mask, 'heat-energy-production[TWh]'] = 0
    heat_energy_production_TWh_2.loc[~mask, 'heat-energy-production[TWh]'] = heat_energy_production_TWh_2.loc[~mask, 'heat-energy-production[TWh]']
    # way-of-production = heat-plant (not CHP)
    heat_energy_production_TWh_2['way-of-production'] = "heat-plant"
    # OTS (only) fuel-mix-heat [%]
    fuel_mix_heat_percent = import_data(trigram='elc', variable_name='fuel-mix-heat', variable_type='OTS (only)')
    # Same as last available year
    fuel_mix_heat_percent = add_missing_years(df_data=fuel_mix_heat_percent)
    # energy-production[TWh] (heat) = heat-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh = mcd(input_table_1=heat_energy_production_TWh_2, input_table_2=fuel_mix_heat_percent, operation_selection='x * y', output_name='energy-production[TWh]')
    # energy-production[TWh] (heat) = heat-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh_2 = mcd(input_table_1=heat_energy_production_TWh, input_table_2=fuel_mix_heat_percent, operation_selection='x * y', output_name='energy-production[TWh]')

    # Electricity production

    # energy-production[TWh] (elec) = chp-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh_3 = mcd(input_table_1=chp_energy_production_TWh, input_table_2=fuel_mix_heat_percent, operation_selection='x * y', output_name='energy-production[TWh]')
    # energy-carrier = electricity (not heat)
    energy_production_TWh_3['energy-carrier'] = "electricity"
    # Node 5876
    energy_production_TWh_2 = pd.concat([energy_production_TWh_3, energy_production_TWh_2])
    # Node 5876
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh])

    # Apply efficiency-improvement levers 
    # => determine the level of primary energy demand required to produce heat demand

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # OTS / FTS efficiency-imp-heat [%]
    efficiency_imp_heat_percent = import_data(trigram='elc', variable_name='efficiency-imp-heat')
    # OTS (only) energy-efficiency-heat  [-]
    energy_efficiency_heat_ = import_data(trigram='elc', variable_name='energy-efficiency-heat', variable_type='OTS (only)')
    # Same as last available year
    energy_efficiency_heat_ = add_missing_years(df_data=energy_efficiency_heat_)
    # efficiency-imp-heat (replace) = 1 - efficiency-imp-heat
    efficiency_imp_heat_percent['efficiency-imp-heat[%]'] = 1.0-efficiency_imp_heat_percent['efficiency-imp-heat[%]']
    # energy-efficiency[-] = energy-efficiency-heat / efficiency-imp-chp
    energy_efficiency = mcd(input_table_1=efficiency_imp_heat_percent, input_table_2=energy_efficiency_heat_, operation_selection='y / x', output_name='energy-efficiency[-]')
    # as primary-energy-carrier-to
    out_9513_1 = energy_efficiency.rename(columns={'primary-energy-carrier': 'primary-energy-carrier-to'})
    # as primary-energy-carrier-from
    out_9512_1 = energy_efficiency.rename(columns={'primary-energy-carrier': 'primary-energy-carrier-from'})
    # energy-efficiency-ratio[-] = energy-efficiency[-] (from) / energy-efficiency[-] (to)
    energy_efficiency_ratio = mcd(input_table_1=out_9512_1, input_table_2=out_9513_1, operation_selection='x / y', output_name='energy-efficiency-ratio[-]')
    # energy-efficiency-ratio [-]
    energy_efficiency_ratio = export_variable(input_table=energy_efficiency_ratio, selected_variable='energy-efficiency-ratio[-]')
    # energy-efficiency-ratio [-]
    energy_efficiency_ratio = use_variable(input_table=energy_efficiency_ratio, selected_variable='energy-efficiency-ratio[-]')
    # ratio[-] = ratio[-] * energy-efficiency-ratio[-]
    ratio_2 = mcd(input_table_1=ratio, input_table_2=energy_efficiency_ratio, operation_selection='x * y', output_name='ratio[-]')
    # primary-energy-demand[TWh] = energy-production[TWh] / energy-efficiency[-]
    primary_energy_demand_TWh = mcd(input_table_1=energy_production_TWh, input_table_2=energy_efficiency, operation_selection='x / y', output_name='primary-energy-demand[TWh]')
    # If missing set to 0
    primary_energy_demand_TWh = missing_value(df=primary_energy_demand_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by  country, years, way-of-prod,  primary-energy-carrier
    primary_energy_demand_TWh = group_by_dimensions(df=primary_energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')

    # Calibration

    # Calibration: heat-primary-energy-demand [TWh]
    heat_primary_energy_demand_TWh = import_data(trigram='elc', variable_name='heat-primary-energy-demand', variable_type='Calibration')
    # Apply Calibration on primary-energy-demand [TWh]
    primary_energy_demand_TWh, _, out_7096_3 = calibration(input_table=primary_energy_demand_TWh, cal_table=heat_primary_energy_demand_TWh, data_to_be_cal='primary-energy-demand[TWh]', data_cal='heat-primary-energy-demand[TWh]')
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = import_data(trigram='elc', variable_name='fuel-switch')
    # energy-carrier to primary-energy-carrier TO CHANGE IN INPUT DATA  INSTEAD!!!
    out_7122_1 = fuel_switch_percent.rename(columns={'energy-carrier-from': 'primary-energy-carrier-from', 'energy-carrier-to': 'primary-energy-carrier-to'})
    # Fuel Switch ff to elec-hp
    out_9519_1 = x_switch(demand_table=primary_energy_demand_TWh, switch_table=out_7122_1, correlation_table=ratio_2, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='elec-hp')
    # Fuel Switch ff to bio
    out_9520_1 = x_switch(demand_table=out_9519_1, switch_table=out_7122_1, correlation_table=ratio_2, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier')
    # Fuel Switch non-res to res (waste)
    out_7123_1 = x_switch(demand_table=out_9520_1, switch_table=out_7122_1, correlation_table=ratio_2, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')
    # Fuel Switch non-res to solid-bio (waste)
    out_7124_1 = x_switch(demand_table=out_7123_1, switch_table=out_7122_1, correlation_table=ratio_2, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres')

    # GHG emissions
    # For carbon-capture and energy required for carbon capture : apply emission factor of fossil fuels !
    # As emission factor of bio- and syn-fuels are set to 0 to represent the fact that CO2 emissions were previously captured by vegetation / H2 syn, it is not possible to determine CO2 captured based on these emissions.
    # In order to have these values : we assume bio- / syn-fuels emit the same way as there fossil fuel equivalent and apply, then, carbon capture and energy required for CC on these emissions
    # => Liquid fuels (bio / syn / ff) : liquid fossil emission factor
    # => Gaseous fuels (bio / syn / ff) : gaseous fossil emission factor
    # => Solid fuels (bio / syn / ff) : solid fossil emission factor

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=out_7124_1, selected_variable='primary-energy-demand[TWh]')
    # RCP elc-emission-factor [g/kWh]
    elc_emission_factor_g_per_kWh = import_data(trigram='elc', variable_name='elc-emission-factor', variable_type='RCP')
    # Convert Unit g/kWh to Mt/TWh (* 0.001)
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_g_per_kWh.drop(columns='elc-emission-factor[g/kWh]').assign(**{'elc-emission-factor[Mt/TWh]': elc_emission_factor_g_per_kWh['elc-emission-factor[g/kWh]'] * 0.001})
    elc_emission_factor_Mt_per_TWh_2 = elc_emission_factor_Mt_per_TWh.loc[~elc_emission_factor_Mt_per_TWh['primary-energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'liquid-bio'])].copy()

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
    out_8017_1 = helper_8017(input_table=elc_emission_factor_Mt_per_TWh_2)
    # insitu-emissions[Mt] = primary-energy-demand[TWh] * elc-emission-factor [Mt/TWh]
    insitu_emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh, input_table_2=out_8017_1, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier
    insitu_emissions_Mt = group_by_dimensions(df=insitu_emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')
    # emissions[Mt] = primary-energy-demand[TWh] * emission-factor[Mt/TWh]
    emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh, input_table_2=elc_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  country, years, gaes, way-of-prod
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'gaes'], aggregation_method='Sum')

    # Calibration

    # Calibration: heat-emissions-Mt
    heat_emissions_Mt = import_data(trigram='elc', variable_name='heat-emissions-Mt', variable_type='Calibration')
    # Apply Calibration on emissions [Mt]
    emissions_Mt, out_7099_2, out_7099_3 = calibration(input_table=emissions_Mt, cal_table=heat_emissions_Mt, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')

    # Carbon capture (and associated energy consumption)

    # Apply CC levers 
    # => determine the quantity of CO2 emissions than can be capture
    # => decrease CO2 emissions
    # 
    # We assum that CC share is the same as elec.

    # OTS / FTS : CCS-ratio-elec
    CCS_ratio_elec = import_data(trigram='elc', variable_name='CCS-ratio-elec')
    # Apply cal-rate on insitu-emissions[Mt] 
    insitu_emissions_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=out_7099_2, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Keep only CO2
    insitu_emissions_Mt = insitu_emissions_Mt.loc[insitu_emissions_Mt['gaes'].isin(['CO2'])].copy()
    # CC[Mt] = insitu-emissions[Mt] * CCS-ratio-elec[%]  if missing CCS-ratio-elec set to 0
    CC_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=CCS_ratio_elec, operation_selection='x * y', output_name='CC[Mt]', fill_value_bool='Left [x] Outer Join')
    # Group by Country, Years, way-of-production
    CC_Mt_2 = group_by_dimensions(df=CC_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # RCP CC-specific-energy-consumption [TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt = import_data(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')
    # Keep co2-concentration = low (for power production concentration is always low)
    CC_specific_energy_consumption_TWh_per_Mt = CC_specific_energy_consumption_TWh_per_Mt.loc[CC_specific_energy_consumption_TWh_per_Mt['co2-concentration'].isin(['low'])].copy()
    # primary-energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    primary_energy_demand_TWh_2 = mcd(input_table_1=CC_Mt_2, input_table_2=CC_specific_energy_consumption_TWh_per_Mt, operation_selection='x * y', output_name='primary-energy-demand[TWh]')

    # Apply CC energy efficiency levers 
    # (linked to ind energy efficiency !)
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS CC-energy-efficiency [%]  FROM INDUSTRY (we should transfer data to TECHNOLOGY)
    CC_energy_efficiency_percent = import_data(trigram='ind', variable_name='CC-energy-efficiency')
    # primary-energy-demand[TWh] (replace) = primary-energy-demand[TWh] * (1 - CC-energy-efficiency[%])
    primary_energy_demand_TWh_2 = mcd(input_table_1=primary_energy_demand_TWh_2, input_table_2=CC_energy_efficiency_percent, operation_selection='x * (1-y)', output_name='primary-energy-demand[TWh]')
    # energy-carrier as primary-energy-carrier
    out_9452_1 = primary_energy_demand_TWh_2.rename(columns={'energy-carrier': 'primary-energy-carrier'})
    # primary-energy-demand [TWh] for : - Heat production - Carbon capture
    out_9451_1 = pd.concat([primary_energy_demand_TWh, out_9452_1])
    # Group by  Country, Years, way-of-production primary-energy-carrier (sum)
    out_9451_1 = group_by_dimensions(df=out_9451_1, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')
    # primary-energy-demand [TWh]  for : - Heat production - Carbon capture
    primary_energy_demand_TWh = export_variable(input_table=out_9451_1, selected_variable='primary-energy-demand[TWh]')

    # Primary energy demand for heat production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # sector = heat
    primary_energy_demand_TWh['sector'] = "heat"

    # Energy demand for CC
    # This is already accounted in the primary energy demand of the sector.
    # This is only required to create a graph.

    # primary-energy-demand [TWh]  only for Carbon capture
    primary_energy_demand_TWh_2 = export_variable(input_table=out_9452_1, selected_variable='primary-energy-demand[TWh]')

    # CC primary energy demand

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')
    # sector = heat
    primary_energy_demand_TWh_2['sector'] = "heat"

    # Emissions Capture

    # CC [Mt]
    CC_Mt = use_variable(input_table=CC_Mt, selected_variable='CC[Mt]')
    # sector = heat
    CC_Mt['sector'] = "heat"

    # Emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # sector = heat
    emissions_Mt['sector'] = "heat"

    # Keep calibration rate

    # cal rate for emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out_7099_3, selected_variable='cal_rate_emissions[Mt]')
    # cal rate for primary-energy-demand[TWh]
    cal_rate_primary_energy_demand_TWh = use_variable(input_table=out_7096_3, selected_variable='cal_rate_primary-energy-demand[TWh]')
    cal_rate = pd.concat([cal_rate_primary_energy_demand_TWh, cal_rate_emissions_Mt])

    # Calibration

    # If missing txt set to ""
    cal_rate = missing_value(df=cal_rate, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # Fuel Switch ff to elec-hp
    out_9517_1 = x_switch(demand_table=energy_production_TWh, switch_table=out_7122_1, correlation_table=ratio, col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='elec-hp')
    # Fuel Switch ff to bio
    out_9518_1 = x_switch(demand_table=out_9517_1, switch_table=out_7122_1, correlation_table=ratio, col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier')
    # Fuel Switch non-res to res (waste)
    out_7130_1 = x_switch(demand_table=out_9518_1, switch_table=out_7122_1, correlation_table=ratio, col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')
    # Fuel Switch non-res to solid-bio (waste)
    out_7131_1 = x_switch(demand_table=out_7130_1, switch_table=out_7122_1, correlation_table=ratio, col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres')

    # Costs CAPEX / OPEX
    # Note : Only consider heat-plant for the moment (because no CAPEX/OPEX values for CHP)

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=out_7131_1, selected_variable='energy-production[TWh]')
    # Unit conversion TWh to kW
    energy_production_kW = energy_production_TWh.drop(columns='energy-production[TWh]').assign(**{'energy-production[kW]': energy_production_TWh['energy-production[TWh]'] * 114077.116130504})
    # Group by  country, years, primary-energy-carrier
    energy_production_kW = group_by_dimensions(df=energy_production_kW, groupby_dimensions=['Country', 'Years', 'primary-energy-carrier'], aggregation_method='Sum')
    # energy-production [kW]
    energy_production_kW_2 = export_variable(input_table=energy_production_kW, selected_variable='energy-production[kW]')
    # RCP costs-for-energy-by-energy-carrier [MEUR/kW] from TEC
    costs_for_energy_by_energy_carrier_MEUR_per_kW = import_data(trigram='tec', variable_name='costs-for-energy-by-energy-carrier', variable_type='RCP')
    # RCP price-indices [-] from TEC
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')
    # OTS/FTS wacc [%] from TEC
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Compute opex for heat energy-production[kW]
    out_9523_1 = compute_costs(df_activity=energy_production_kW, df_unit_costs=costs_for_energy_by_energy_carrier_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='energy-production[kW]', cost_type='OPEX')
    # RCP energy-production-cost-user [-]
    energy_production_cost_user_ = import_data(trigram='elc', variable_name='energy-production-cost-user', variable_type='RCP')
    # opex[MEUR] = opex[MEUR] * energy-production-cost-user[-]
    opex_MEUR = mcd(input_table_1=out_9523_1, input_table_2=energy_production_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    # Group by  Country, Years (sum)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    # Lag  energy-production[kW]
    out_7114_1, out_7114_2 = lag_variable(df=energy_production_kW, in_var='energy-production[kW]')
    # energy-production -lagged [kW]
    energy_production_lagged_kW = export_variable(input_table=out_7114_1, selected_variable='energy-production_lagged[kW]')
    # new-capacities[kW] = energy-production[kW] - energy-production-lagged[kW]
    new_capacities_kW = mcd(input_table_1=energy_production_kW_2, input_table_2=energy_production_lagged_kW, operation_selection='x - y', output_name='new-capacities[kW]')
    # if < 0 then 0
    mask = new_capacities_kW['new-capacities[kW]'] < 0
    new_capacities_kW.loc[mask, 'new-capacities[kW]'] =  0
    new_capacities_kW.loc[~mask, 'new-capacities[kW]'] =  new_capacities_kW.loc[~mask, 'new-capacities[kW]']
    # Timestep
    Timestep = export_variable(input_table=out_7114_2, selected_variable='Timestep')
    # annual-new-capacities[kW] = new-capacities[kW] / Timstep
    annual_new_capacities_kW = mcd(input_table_1=new_capacities_kW, input_table_2=Timestep, operation_selection='x / y', output_name='annual-new-capacities[kW]')
    # Compute capex for new heat capacities[kW]
    out_9527_1 = compute_costs(df_activity=annual_new_capacities_kW, df_unit_costs=costs_for_energy_by_energy_carrier_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='annual-new-capacities[kW]')
    # capex[MEUR] = capex[MEUR] * energy-production-cost-user[-]
    capex_MEUR = mcd(input_table_1=energy_production_cost_user_, input_table_2=out_9527_1, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by  Country, Years (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'cost-user', 'Years'], aggregation_method='Sum')
    MEUR = pd.concat([opex_MEUR, capex_MEUR])

    # Capex / Opex

    # All Capex/Opex
    MEUR = column_filter(df=MEUR, pattern='^.*$')
    # sector = heat
    MEUR['sector'] = "heat"
    # sector = heat
    energy_production_TWh['sector'] = "heat"

    # Energy - production

    # Group by  country, years, sector, way-of-prod, energy-carrier, primary-energy-carrier
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier', 'sector'], aggregation_method='Sum')

    return energy_imported_TWh, energy_production_TWh, MEUR, primary_energy_demand_TWh, emissions_Mt, cal_rate, CC_Mt, primary_energy_demand_TWh_2


