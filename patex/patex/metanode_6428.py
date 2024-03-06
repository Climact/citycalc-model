import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


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
    out_9506_1 = TableCreatorNode(df=pd.DataFrame(columns=['category-from', 'primary-energy-carrier-from', 'category-to', 'primary-energy-carrier-to'], data=[['fffuels', 'liquid-ff', 'biofuels', 'liquid-bio'], ['fffuels', 'solid-ff-coal', 'biofuels', 'solid-biomass'], ['fffuels', 'gaseous-ff-natural', 'biofuels', 'gaseous-bio'], ['fffuels', 'liquid-ff', 'elec-hp', 'RES-electricity-hp'], ['fffuels', 'solid-ff-coal', 'elec-hp', 'RES-electricity-hp'], ['fffuels', 'gaseous-ff-natural', 'elec-hp', 'RES-electricity-hp'], ['nonres', 'solid-waste-nonres', 'res', 'solid-waste-res'], ['nonres', 'solid-waste-nonres', 'biofuels', 'solid-biomass']]))()
    # ratio[-] = 1
    ratio = out_9506_1.assign(**{'ratio[-]': 1.0})

    # Heat demand (from other sectors)

    # energy-demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)
    # Keep only energy-carrier heat
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['heat'])].copy()
    # Group by  country, years and energy-carrier
    energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')(df=energy_demand_TWh)
    # OTS (only) ratio-self-heat-consumption [-]
    ratio_self_heat_consumption_ = ImportDataNode(trigram='elc', variable_name='ratio-self-heat-consumption', variable_type='OTS (only)')()
    # Same as last available year
    ratio_self_heat_consumption_ = AddMissingYearsNode()(df_data=ratio_self_heat_consumption_)
    # ratio-self-heat-consumption[%] = (replace) 1 + ratio-self-heat-consumption[%]
    ratio_self_heat_consumption_percent = ratio_self_heat_consumption_.assign(**{'ratio-self-heat-consumption[%]': 1.0+ratio_self_heat_consumption_['ratio-self-heat-consumption[%]']})
    # energy-demand[TWh] = energy-demand[TWh] * ratio-self-heat-consump.[%])
    energy_demand_TWh = MCDNode(operation_selection='x * y', output_name='energy-demand[TWh]')(input_table_1=energy_demand_TWh, input_table_2=ratio_self_heat_consumption_percent)

    # Apply net-import levers 
    # => determine level of production required to fill the demand

    # OTS / FTS energy-net-import
    energy_net_import = ImportDataNode(trigram='elc', variable_name='energy-net-import')()
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = MCDNode(operation_selection='x * y', output_name='energy-imported[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_net_import)
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = MCDNode(operation_selection='x - y', output_name='energy-producted[TWh]')(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh)

    # Formating data for other modules + Pathway Explorer

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh = UseVariableNode(selected_variable='energy-imported[TWh]')(input_table=energy_imported_TWh)
    # sector = heat
    energy_imported_TWh['sector'] = "heat"

    # CHP technology

    # Apply CHP capacity levers 
    # => determine level of heat / electricity that can be produced with CHP

    # OTS / FTS capacity [GW]
    capacity_GW = ImportDataNode(trigram='elc', variable_name='capacity')()
    # Unit conversion GW to TWh
    capacity_TWh = capacity_GW.drop(columns='capacity[GW]').assign(**{'capacity[TWh]': capacity_GW['capacity[GW]'] * 8.766})
    # HTS capacity-factor [%]
    capacity_factor_percent = ImportDataNode(trigram='elc', variable_name='capacity-factor', variable_type='HTS')()
    # Same as last available year
    capacity_factor_percent = AddMissingYearsNode()(df_data=capacity_factor_percent)

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
    chp_energy_production_TWh = MCDNode(operation_selection='x * y', output_name='chp-energy-production[TWh]')(input_table_1=capacity_TWh, input_table_2=capacity_factor_percent)

    # Decrease capacity-factor if :
    # production with CHP > needed production of heat

    # OTS (only) chp-pth-ratio [-]
    chp_pth_ratio_ = ImportDataNode(trigram='elc', variable_name='chp-pth-ratio', variable_type='OTS (only)')()
    # Same as last available year
    chp_pth_ratio_ = AddMissingYearsNode()(df_data=chp_pth_ratio_)

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
    heat_energy_production_TWh = MCDNode(operation_selection='x / y', output_name='heat-energy-production[TWh]')(input_table_1=chp_energy_production_TWh, input_table_2=chp_pth_ratio_)
    # If missing set to 0
    heat_energy_production_TWh = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=heat_energy_production_TWh)

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
    chp_pth_ratio = ExportVariableNode(selected_variable='chp-pth-ratio[-]')(input_table=out_6696_1)
    # capacity[TWh]
    capacity_TWh = ExportVariableNode(selected_variable='capacity[TWh]')(input_table=out_6696_1)
    # capacity-factor[%]
    capacity_factor_percent = ExportVariableNode(selected_variable='capacity-factor[%]')(input_table=out_6696_1)
    # chp-energy-production[TWh] = capacity[TWh] * capacity-factor[%]
    chp_energy_production_TWh = MCDNode(operation_selection='x * y', output_name='chp-energy-production[TWh]')(input_table_1=capacity_factor_percent, input_table_2=capacity_TWh)

    # Distinguish elec and heat production and apply fuel mix

    # Heat production

    # heat-energy-production[TWh] = chp-energy-production[TWh] / chp-pth-ratio[%]
    heat_energy_production_TWh = MCDNode(operation_selection='x / y', output_name='heat-energy-production[TWh]')(input_table_1=chp_energy_production_TWh, input_table_2=chp_pth_ratio)
    # If missing set to 0
    heat_energy_production_TWh = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=heat_energy_production_TWh)
    # If < 0 Set to 0
    mask = heat_energy_production_TWh['heat-energy-production[TWh]']<0
    heat_energy_production_TWh.loc[mask, 'heat-energy-production[TWh]'] = 0
    heat_energy_production_TWh.loc[~mask, 'heat-energy-production[TWh]'] = heat_energy_production_TWh.loc[~mask, 'heat-energy-production[TWh]']

    # Heat-only technology
    # Heat only plant are used to produced heat when all heat demand can not be covered by CHP only

    # Heat production
    # If remaining heat demand after CHP production, heat is produced with heat-only plant

    # heat-energy-production[TWh] (replace) = energy-producted[TWh] - heat-energy-production[TWh]
    heat_energy_production_TWh_2 = MCDNode(operation_selection='x - y', output_name='heat-energy-production[TWh]')(input_table_1=energy_producted_TWh, input_table_2=heat_energy_production_TWh)
    # If < 0 Set to 0
    mask = heat_energy_production_TWh_2['heat-energy-production[TWh]']<0
    heat_energy_production_TWh_2.loc[mask, 'heat-energy-production[TWh]'] = 0
    heat_energy_production_TWh_2.loc[~mask, 'heat-energy-production[TWh]'] = heat_energy_production_TWh_2.loc[~mask, 'heat-energy-production[TWh]']
    # way-of-production = heat-plant (not CHP)
    heat_energy_production_TWh_2['way-of-production'] = "heat-plant"
    # OTS (only) fuel-mix-heat [%]
    fuel_mix_heat_percent = ImportDataNode(trigram='elc', variable_name='fuel-mix-heat', variable_type='OTS (only)')()
    # Same as last available year
    fuel_mix_heat_percent = AddMissingYearsNode()(df_data=fuel_mix_heat_percent)
    # energy-production[TWh] (heat) = heat-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=heat_energy_production_TWh_2, input_table_2=fuel_mix_heat_percent)
    # energy-production[TWh] (heat) = heat-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh_2 = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=heat_energy_production_TWh, input_table_2=fuel_mix_heat_percent)

    # Electricity production

    # energy-production[TWh] (elec) = chp-energy-production[TWh] * fuel-mix-heat[%]
    energy_production_TWh_3 = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=chp_energy_production_TWh, input_table_2=fuel_mix_heat_percent)
    # energy-carrier = electricity (not heat)
    energy_production_TWh_3['energy-carrier'] = "electricity"
    # Node 5876
    energy_production_TWh_2 = pd.concat([energy_production_TWh_3, energy_production_TWh_2.set_index(energy_production_TWh_2.index.astype(str) + '_dup')])
    # Node 5876
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh.set_index(energy_production_TWh.index.astype(str) + '_dup')])

    # Apply efficiency-improvement levers 
    # => determine the level of primary energy demand required to produce heat demand

    # energy-production [TWh]
    energy_production_TWh = UseVariableNode(selected_variable='energy-production[TWh]')(input_table=energy_production_TWh)
    # OTS / FTS efficiency-imp-heat [%]
    efficiency_imp_heat_percent = ImportDataNode(trigram='elc', variable_name='efficiency-imp-heat')()
    # OTS (only) energy-efficiency-heat  [-]
    energy_efficiency_heat_ = ImportDataNode(trigram='elc', variable_name='energy-efficiency-heat', variable_type='OTS (only)')()
    # Same as last available year
    energy_efficiency_heat_ = AddMissingYearsNode()(df_data=energy_efficiency_heat_)
    # efficiency-imp-heat (replace) = 1 - efficiency-imp-heat
    efficiency_imp_heat_percent['efficiency-imp-heat[%]'] = 1.0-efficiency_imp_heat_percent['efficiency-imp-heat[%]']
    # energy-efficiency[-] = energy-efficiency-heat / efficiency-imp-chp
    energy_efficiency = MCDNode(operation_selection='y / x', output_name='energy-efficiency[-]')(input_table_1=efficiency_imp_heat_percent, input_table_2=energy_efficiency_heat_)
    # as primary-energy-carrier-to
    out_9513_1 = energy_efficiency.rename(columns={'primary-energy-carrier': 'primary-energy-carrier-to'})
    # as primary-energy-carrier-from
    out_9512_1 = energy_efficiency.rename(columns={'primary-energy-carrier': 'primary-energy-carrier-from'})
    # energy-efficiency-ratio[-] = energy-efficiency[-] (from) / energy-efficiency[-] (to)
    energy_efficiency_ratio = MCDNode(operation_selection='x / y', output_name='energy-efficiency-ratio[-]')(input_table_1=out_9512_1, input_table_2=out_9513_1)
    # energy-efficiency-ratio [-]
    energy_efficiency_ratio = ExportVariableNode(selected_variable='energy-efficiency-ratio[-]')(input_table=energy_efficiency_ratio)
    # energy-efficiency-ratio [-]
    energy_efficiency_ratio = UseVariableNode(selected_variable='energy-efficiency-ratio[-]')(input_table=energy_efficiency_ratio)
    # ratio[-] = ratio[-] * energy-efficiency-ratio[-]
    ratio_2 = MCDNode(operation_selection='x * y', output_name='ratio[-]')(input_table_1=ratio, input_table_2=energy_efficiency_ratio)
    # primary-energy-demand[TWh] = energy-production[TWh] / energy-efficiency[-]
    primary_energy_demand_TWh = MCDNode(operation_selection='x / y', output_name='primary-energy-demand[TWh]')(input_table_1=energy_production_TWh, input_table_2=energy_efficiency)
    # If missing set to 0
    primary_energy_demand_TWh = MissingValueNode(DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')(df=primary_energy_demand_TWh)
    # Group by  country, years, way-of-prod,  primary-energy-carrier
    primary_energy_demand_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')(df=primary_energy_demand_TWh)

    # Calibration

    # Calibration: heat-primary-energy-demand [TWh]
    heat_primary_energy_demand_TWh = ImportDataNode(trigram='elc', variable_name='heat-primary-energy-demand', variable_type='Calibration')()
    # Apply Calibration on primary-energy-demand [TWh]
    primary_energy_demand_TWh, _, out_7096_3 = CalibrationNode(data_to_be_cal='primary-energy-demand[TWh]', data_cal='heat-primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh, cal_table=heat_primary_energy_demand_TWh)
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = ImportDataNode(trigram='elc', variable_name='fuel-switch')()
    # energy-carrier to primary-energy-carrier TO CHANGE IN INPUT DATA  INSTEAD!!!
    out_7122_1 = fuel_switch_percent.rename(columns={'energy-carrier-from': 'primary-energy-carrier-from', 'energy-carrier-to': 'primary-energy-carrier-to'})
    # Fuel Switch ff to elec-hp
    out_9519_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='elec-hp')(demand_table=primary_energy_demand_TWh, switch_table=out_7122_1, correlation_table=ratio_2)
    # Fuel Switch ff to bio
    out_9520_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier')(demand_table=out_9519_1, switch_table=out_7122_1, correlation_table=ratio_2)
    # Fuel Switch non-res to res (waste)
    out_7123_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')(demand_table=out_9520_1, switch_table=out_7122_1, correlation_table=ratio_2)
    # Fuel Switch non-res to solid-bio (waste)
    out_7124_1 = XSwitchNode(col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres')(demand_table=out_7123_1, switch_table=out_7122_1, correlation_table=ratio_2)

    # GHG emissions
    # For carbon-capture and energy required for carbon capture : apply emission factor of fossil fuels !
    # As emission factor of bio- and syn-fuels are set to 0 to represent the fact that CO2 emissions were previously captured by vegetation / H2 syn, it is not possible to determine CO2 captured based on these emissions.
    # In order to have these values : we assume bio- / syn-fuels emit the same way as there fossil fuel equivalent and apply, then, carbon capture and energy required for CC on these emissions
    # => Liquid fuels (bio / syn / ff) : liquid fossil emission factor
    # => Gaseous fuels (bio / syn / ff) : gaseous fossil emission factor
    # => Solid fuels (bio / syn / ff) : solid fossil emission factor

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=out_7124_1)
    # RCP elc-emission-factor [g/kWh]
    elc_emission_factor_g_per_kWh = ImportDataNode(trigram='elc', variable_name='elc-emission-factor', variable_type='RCP')()
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
    insitu_emissions_Mt = MCDNode(operation_selection='x * y', output_name='insitu-emissions[Mt]')(input_table_1=primary_energy_demand_TWh, input_table_2=out_8017_1)
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier
    insitu_emissions_Mt = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')(df=insitu_emissions_Mt)
    # emissions[Mt] = primary-energy-demand[TWh] * emission-factor[Mt/TWh]
    emissions_Mt = MCDNode(operation_selection='x * y', output_name='emissions[Mt]')(input_table_1=primary_energy_demand_TWh, input_table_2=elc_emission_factor_Mt_per_TWh)
    # Group by  country, years, gaes, way-of-prod
    emissions_Mt = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'gaes'], aggregation_method='Sum')(df=emissions_Mt)

    # Calibration

    # Calibration: heat-emissions-Mt
    heat_emissions_Mt = ImportDataNode(trigram='elc', variable_name='heat-emissions-Mt', variable_type='Calibration')()
    # Apply Calibration on emissions [Mt]
    emissions_Mt, out_7099_2, out_7099_3 = CalibrationNode(data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')(input_table=emissions_Mt, cal_table=heat_emissions_Mt)

    # Carbon capture (and associated energy consumption)

    # Apply CC levers 
    # => determine the quantity of CO2 emissions than can be capture
    # => decrease CO2 emissions
    # 
    # We assum that CC share is the same as elec.

    # OTS / FTS : CCS-ratio-elec
    CCS_ratio_elec = ImportDataNode(trigram='elc', variable_name='CCS-ratio-elec')()
    # Apply cal-rate on insitu-emissions[Mt] 
    insitu_emissions_Mt = MCDNode(operation_selection='x * y', output_name='insitu-emissions[Mt]')(input_table_1=insitu_emissions_Mt, input_table_2=out_7099_2)
    # Keep only CO2
    insitu_emissions_Mt = insitu_emissions_Mt.loc[insitu_emissions_Mt['gaes'].isin(['CO2'])].copy()
    # CC[Mt] = insitu-emissions[Mt] * CCS-ratio-elec[%]  if missing CCS-ratio-elec set to 0
    CC_Mt = MCDNode(operation_selection='x * y', output_name='CC[Mt]', fill_value_bool='Left [x] Outer Join')(input_table_1=insitu_emissions_Mt, input_table_2=CCS_ratio_elec)
    # Group by Country, Years, way-of-production
    CC_Mt_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')(df=CC_Mt)
    # RCP CC-specific-energy-consumption [TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt = ImportDataNode(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')()
    # Keep co2-concentration = low (for power production concentration is always low)
    CC_specific_energy_consumption_TWh_per_Mt = CC_specific_energy_consumption_TWh_per_Mt.loc[CC_specific_energy_consumption_TWh_per_Mt['co2-concentration'].isin(['low'])].copy()
    # primary-energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x * y', output_name='primary-energy-demand[TWh]')(input_table_1=CC_Mt_2, input_table_2=CC_specific_energy_consumption_TWh_per_Mt)

    # Apply CC energy efficiency levers 
    # (linked to ind energy efficiency !)
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS CC-energy-efficiency [%]  FROM INDUSTRY (we should transfer data to TECHNOLOGY)
    CC_energy_efficiency_percent = ImportDataNode(trigram='ind', variable_name='CC-energy-efficiency')()
    # primary-energy-demand[TWh] (replace) = primary-energy-demand[TWh] * (1 - CC-energy-efficiency[%])
    primary_energy_demand_TWh_2 = MCDNode(operation_selection='x * (1-y)', output_name='primary-energy-demand[TWh]')(input_table_1=primary_energy_demand_TWh_2, input_table_2=CC_energy_efficiency_percent)
    # energy-carrier as primary-energy-carrier
    out_9452_1 = primary_energy_demand_TWh_2.rename(columns={'energy-carrier': 'primary-energy-carrier'})
    # primary-energy-demand [TWh] for : - Heat production - Carbon capture
    out_9451_1 = pd.concat([primary_energy_demand_TWh, out_9452_1.set_index(out_9452_1.index.astype(str) + '_dup')])
    # Group by  Country, Years, way-of-production primary-energy-carrier (sum)
    out_9451_1 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')(df=out_9451_1)
    # primary-energy-demand [TWh]  for : - Heat production - Carbon capture
    primary_energy_demand_TWh = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=out_9451_1)

    # Primary energy demand for heat production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh)
    # sector = heat
    primary_energy_demand_TWh['sector'] = "heat"

    # Energy demand for CC
    # This is already accounted in the primary energy demand of the sector.
    # This is only required to create a graph.

    # primary-energy-demand [TWh]  only for Carbon capture
    primary_energy_demand_TWh_2 = ExportVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=out_9452_1)

    # CC primary energy demand

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = UseVariableNode(selected_variable='primary-energy-demand[TWh]')(input_table=primary_energy_demand_TWh_2)
    # sector = heat
    primary_energy_demand_TWh_2['sector'] = "heat"

    # Emissions Capture

    # CC [Mt]
    CC_Mt = UseVariableNode(selected_variable='CC[Mt]')(input_table=CC_Mt)
    # sector = heat
    CC_Mt['sector'] = "heat"

    # Emissions

    # emissions [Mt]
    emissions_Mt = UseVariableNode(selected_variable='emissions[Mt]')(input_table=emissions_Mt)
    # sector = heat
    emissions_Mt['sector'] = "heat"

    # Keep calibration rate

    # cal rate for emissions[Mt]
    cal_rate_emissions_Mt = UseVariableNode(selected_variable='cal_rate_emissions[Mt]')(input_table=out_7099_3)
    # cal rate for primary-energy-demand[TWh]
    cal_rate_primary_energy_demand_TWh = UseVariableNode(selected_variable='cal_rate_primary-energy-demand[TWh]')(input_table=out_7096_3)
    cal_rate = pd.concat([cal_rate_primary_energy_demand_TWh, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])

    # Calibration

    # If missing txt set to ""
    cal_rate = MissingValueNode(dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')(df=cal_rate)
    # Fuel Switch ff to elec-hp
    out_9517_1 = XSwitchNode(col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='elec-hp')(demand_table=energy_production_TWh, switch_table=out_7122_1, correlation_table=ratio)
    # Fuel Switch ff to bio
    out_9518_1 = XSwitchNode(col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier')(demand_table=out_9517_1, switch_table=out_7122_1, correlation_table=ratio)
    # Fuel Switch non-res to res (waste)
    out_7130_1 = XSwitchNode(col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')(demand_table=out_9518_1, switch_table=out_7122_1, correlation_table=ratio)
    # Fuel Switch non-res to solid-bio (waste)
    out_7131_1 = XSwitchNode(col_energy='energy-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres')(demand_table=out_7130_1, switch_table=out_7122_1, correlation_table=ratio)

    # Costs CAPEX / OPEX
    # Note : Only consider heat-plant for the moment (because no CAPEX/OPEX values for CHP)

    # energy-production [TWh]
    energy_production_TWh = UseVariableNode(selected_variable='energy-production[TWh]')(input_table=out_7131_1)
    # Unit conversion TWh to kW
    energy_production_kW = energy_production_TWh.drop(columns='energy-production[TWh]').assign(**{'energy-production[kW]': energy_production_TWh['energy-production[TWh]'] * 114077.116130504})
    # Group by  country, years, primary-energy-carrier
    energy_production_kW = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'primary-energy-carrier'], aggregation_method='Sum')(df=energy_production_kW)
    # energy-production [kW]
    energy_production_kW_2 = ExportVariableNode(selected_variable='energy-production[kW]')(input_table=energy_production_kW)
    # RCP costs-for-energy-by-energy-carrier [MEUR/kW] from TEC
    costs_for_energy_by_energy_carrier_MEUR_per_kW = ImportDataNode(trigram='tec', variable_name='costs-for-energy-by-energy-carrier', variable_type='RCP')()
    # RCP price-indices [-] from TEC
    price_indices_ = ImportDataNode(trigram='tec', variable_name='price-indices', variable_type='RCP')()
    # OTS/FTS wacc [%] from TEC
    wacc_percent = ImportDataNode(trigram='tec', variable_name='wacc')()
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')(df=wacc_percent)
    # Compute opex for heat energy-production[kW]
    out_9523_1 = ComputeCosts(activity_variable='energy-production[kW]', cost_type='OPEX')(df_activity=energy_production_kW, df_unit_costs=costs_for_energy_by_energy_carrier_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # RCP energy-production-cost-user [-]
    energy_production_cost_user_ = ImportDataNode(trigram='elc', variable_name='energy-production-cost-user', variable_type='RCP')()
    # opex[MEUR] = opex[MEUR] * energy-production-cost-user[-]
    opex_MEUR = MCDNode(operation_selection='x * y', output_name='opex[MEUR]')(input_table_1=out_9523_1, input_table_2=energy_production_cost_user_)
    # Group by  Country, Years (sum)
    opex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')(df=opex_MEUR)
    # Lag  energy-production[kW]
    out_7114_1, out_7114_2 = LagVariable(in_var='energy-production[kW]')(df=energy_production_kW)
    # energy-production -lagged [kW]
    energy_production_lagged_kW = ExportVariableNode(selected_variable='energy-production_lagged[kW]')(input_table=out_7114_1)
    # new-capacities[kW] = energy-production[kW] - energy-production-lagged[kW]
    new_capacities_kW = MCDNode(operation_selection='x - y', output_name='new-capacities[kW]')(input_table_1=energy_production_kW_2, input_table_2=energy_production_lagged_kW)
    # if < 0 then 0
    mask = new_capacities_kW['new-capacities[kW]'] < 0
    new_capacities_kW.loc[mask, 'new-capacities[kW]'] =  0
    new_capacities_kW.loc[~mask, 'new-capacities[kW]'] =  new_capacities_kW.loc[~mask, 'new-capacities[kW]']
    # Timestep
    Timestep = ExportVariableNode(selected_variable='Timestep')(input_table=out_7114_2)
    # annual-new-capacities[kW] = new-capacities[kW] / Timstep
    annual_new_capacities_kW = MCDNode(operation_selection='x / y', output_name='annual-new-capacities[kW]')(input_table_1=new_capacities_kW, input_table_2=Timestep)
    # Compute capex for new heat capacities[kW]
    out_9527_1 = ComputeCosts(activity_variable='annual-new-capacities[kW]')(df_activity=annual_new_capacities_kW, df_unit_costs=costs_for_energy_by_energy_carrier_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name)
    # capex[MEUR] = capex[MEUR] * energy-production-cost-user[-]
    capex_MEUR = MCDNode(operation_selection='x * y', output_name='capex[MEUR]')(input_table_1=energy_production_cost_user_, input_table_2=out_9527_1)
    # Group by  Country, Years (sum)
    capex_MEUR = GroupByDimensions(groupby_dimensions=['Country', 'cost-user', 'Years'], aggregation_method='Sum')(df=capex_MEUR)
    MEUR = pd.concat([opex_MEUR, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])

    # Capex / Opex

    # All Capex/Opex
    MEUR = ColumnFilterNode(pattern='^.*$')(df=MEUR)
    # sector = heat
    MEUR['sector'] = "heat"
    # sector = heat
    energy_production_TWh['sector'] = "heat"

    # Energy - production

    # Group by  country, years, sector, way-of-prod, energy-carrier, primary-energy-carrier
    energy_production_TWh = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'energy-carrier', 'sector'], aggregation_method='Sum')(df=energy_production_TWh)

    return energy_imported_TWh, energy_production_TWh, MEUR, primary_energy_demand_TWh, emissions_Mt, cal_rate, CC_Mt, primary_energy_demand_TWh_2


