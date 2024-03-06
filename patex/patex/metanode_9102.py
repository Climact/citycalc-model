import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_9102(port_01, port_02, port_03, port_04, port_05, port_06):
    # Concatenate pollutant values


    # Note : les cal rate sont bien pourris !!!
    # Ilfaudrait comprendre pourquoi !


    # Distinction should be made between refineries oil and gaseous refineries !!!



    module_name = 'air_quality'

    # Activities implying degradation of air quality => emissions of pollutants (in tons)

    # Power activities : 
    # - xx

    # conversion group way of production METTRE DANS G.S. A VALIDER
    out_9575_1 = TableCreatorNode(df=pd.DataFrame(columns=['way-of-production', 'way-of-production-group', 'conversion[-]'], data=[['CHP', 'power-all', 1.0], ['RES-geothermal', 'NaN', 1.0], ['RES-hydroelectric', 'NaN', 1.0], ['RES-marine', 'NaN', 1.0], ['RES-solar-csp', 'NaN', 1.0], ['RES-solar-pv', 'NaN', 1.0], ['RES-wind-offshore', 'NaN', 1.0], ['RES-wind-onshore', 'NaN', 1.0], ['e-methanation', 'NaN', 1.0], ['e-methanol', 'NaN', 1.0], ['elec-plant-with-gas', 'power-all', 1.0], ['elec-plant-with-liquid', 'power-all', 1.0], ['elec-plant-with-nuclear', 'power-all', 1.0], ['elec-plant-with-solid-bio-waste', 'power-all', 1.0], ['elec-plant-with-solid-coal', 'power-all', 1.0], ['fischer-tropsch', 'NaN', 1.0], ['heat-plant', 'power-all', 1.0], ['hydrogen-with-electro', 'NaN', 1.0], ['hydrogen-with-steamref', 'NaN', 1.0], ['refineries-oil', 'refineries-all', 1.0]]))()

    # Industry activities : 
    # - xx

    # conversion material to material-group METTRE DANS G.S.
    out_9567_1 = TableCreatorNode(df=pd.DataFrame(columns=['material', 'material-group', 'conversion[-]'], data=[['aluminium', 'non-ferrous-group', 1.0], ['cement', 'other-industries-group', 1.0], ['ceramic-and-others', 'other-industries-group', 1.0], ['chemical-ammonia', 'chemical-group', 1.0], ['chemical-olefin', 'chemical-group', 1.0], ['chemical-other', 'chemical-group', 1.0], ['food', 'other-industries-group', 1.0], ['glass', 'other-industries-group', 1.0], ['lime', 'other-industries-group', 1.0], ['non-ferrous', 'non-ferrous-group', 1.0], ['other-industries', 'other-industries-group', 1.0], ['paper', 'other-industries-group', 1.0], ['steel', 'steel-group', 1.0], ['wood', 'other-industries-group', 1.0]]))()
    # (ELC) energy-demand [TWh]
    energy_demand_TWh = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_06)

    def helper_9653(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get synfuels and convert them to fossil fuels
        dim_name = 'energy-carrier'
        fuel_dict = {
            'gaseous-syn': 'gaseous-ff-natural',
            'liquid-syn': 'liquid-ff-oil'
        }
        for i in fuel_dict.keys():
            mask = (output_table[dim_name] == i)
            output_table.loc[mask, dim_name] = fuel_dict[i]
        return output_table
    # Convert synfuels to fffuels
    out_9653_1 = helper_9653(input_table=energy_demand_TWh)
    # Group by  energy-carrier, way-of-production (sum)
    out_9653_1 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'energy-carrier', 'way-of-production'], aggregation_method='Sum')(df=out_9653_1)
    # energy-demand[TWh] (replace) = energy-demand[TWh] * conversion[-]
    energy_demand_TWh = MCDNode(operation_selection='x * y', output_name='energy-demand[TWh]')(input_table_1=out_9653_1, input_table_2=out_9575_1)

    # Agriculture and land-use activities : 
    # - xx

    # (AGR) N-manure-quantity [kgN]
    N_manure_quantity_kgN = UseVariableNode(selected_variable='N-manure-quantity[kgN]')(input_table=port_03)
    # Convert Unit kgN to ktN
    N_manure_quantity_ktN = N_manure_quantity_kgN.drop(columns='N-manure-quantity[kgN]').assign(**{'N-manure-quantity[ktN]': N_manure_quantity_kgN['N-manure-quantity[kgN]'] * 1e-06})

    # Transport activities & agriculture energy-demand (mainly linked to tractors => HDV) : 
    # - xx

    # (AGR) energy-demand [TWh]
    energy_demand_TWh_2 = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_03)
    # vehicule-type = HDV
    energy_demand_TWh_3 = energy_demand_TWh_2.assign(**{'vehicule-type': "HDV"})
    # (AGR) livestock-population [lsu]
    livestock_population_lsu = UseVariableNode(selected_variable='livestock-population[lsu]')(input_table=port_03)
    # (AGR) amendment-application [t]
    amendment_application_t = UseVariableNode(selected_variable='amendment-application[t]')(input_table=port_03)
    # Convert Unit t to ktN (* 0.46 / 1000) we have around 46% of N in urea and nitrogen
    amendment_application_ktN = amendment_application_t.drop(columns='amendment-application[t]').assign(**{'amendment-application[ktN]': amendment_application_t['amendment-application[t]'] * 0.00046})
    # (IND) energy-demand [TWh]
    energy_demand_TWh_2 = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_05)

    def helper_9652(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get synfuels and convert them to fossil fuels
        dim_name = 'energy-carrier'
        fuel_dict = {
            'gaseous-syn': 'gaseous-ff-natural',
            'liquid-syn': 'liquid-ff-oil'
        }
        for i in fuel_dict.keys():
            mask = (output_table[dim_name] == i)
            output_table.loc[mask, dim_name] = fuel_dict[i]
        return output_table
    # Convert synfuels to fffuels
    out_9652_1 = helper_9652(input_table=energy_demand_TWh_2)
    # Group by energy-carrier, material (sum)
    out_9652_1 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'material', 'energy-carrier'], aggregation_method='Sum')(df=out_9652_1)
    # energy-demand[TWh] (replace) = energy-demand[TWh] * conversion[-]
    energy_demand_TWh_2 = MCDNode(operation_selection='x * y', output_name='energy-demand[TWh]')(input_table_1=out_9652_1, input_table_2=out_9567_1)
    # (IND) material-production [Mt]  Not considered in emission-factor : chemical-other chemical-olefin food wood (ok with that ?)
    material_production_Mt = UseVariableNode(selected_variable='material-production[Mt]')(input_table=port_05)
    # (LUS) land-management [ha]
    land_management_ha = UseVariableNode(selected_variable='land-management[ha]')(input_table=port_04)
    # (TRA) energy-demand [TWh]
    energy_demand_TWh_4 = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_02)
    # Group by energy-carrier, vehicule-type (sum)
    energy_demand_TWh_4 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier'], aggregation_method='Sum')(df=energy_demand_TWh_4)
    energy_demand_TWh_3 = pd.concat([energy_demand_TWh_4, energy_demand_TWh_3.set_index(energy_demand_TWh_3.index.astype(str) + '_dup')])

    def helper_9651(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get synfuels and convert them to fossil fuels
        dim_name = 'energy-carrier'
        fuel_dict = {
            'gaseous-syn': 'gaseous-ff-natural',
            'liquid-syn': 'liquid-ff-oil',
            'liquid-syn-kerosene': 'liquid-ff-kerosene',
            'liquid-syn-marinefueloil': 'liquid-ff-marinefueloil',
            'liquid-syn-diesel': 'liquid-ff-diesel',
            'liquid-syn-gasoline': 'liquid-ff-gasoline'
        }
        for i in fuel_dict.keys():
            mask = (output_table[dim_name] == i)
            output_table.loc[mask, dim_name] = fuel_dict[i]
        return output_table
    # Convert synfuels to fffuels
    out_9651_1 = helper_9651(input_table=energy_demand_TWh_3)
    # (TRA) final-transport-demand [vkm]
    final_transport_demand_vkm = UseVariableNode(selected_variable='final-transport-demand[vkm]')(input_table=port_02)
    # Top : vehicule-type= LDV Bottom : other veh.-types
    final_transport_demand_vkm_2 = final_transport_demand_vkm.loc[final_transport_demand_vkm['vehicule-type'].isin(['LDV'])].copy()
    final_transport_demand_vkm_excluded = final_transport_demand_vkm.loc[~final_transport_demand_vkm['vehicule-type'].isin(['LDV'])].copy()
    # Group by vehicule-type (sum)
    final_transport_demand_vkm_excluded = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')(df=final_transport_demand_vkm_excluded)
    # Group by vehicule-type, transport-user (sum)
    final_transport_demand_vkm = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'vehicule-type', 'transport-user'], aggregation_method='Sum')(df=final_transport_demand_vkm_2)

    # Building activities : 
    # - xx

    # (BLD) energy-demand [TWh]
    energy_demand_TWh_3 = UseVariableNode(selected_variable='energy-demand[TWh]')(input_table=port_01)

    def helper_7514(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get synfuels and convert them to fossil fuels
        dim_name = 'energy-carrier'
        fuel_dict = {
            'gaseous-syn': 'gaseous-ff-natural',
            'liquid-syn': 'liquid-ff-oil'
        }
        for i in fuel_dict.keys():
            mask = (output_table[dim_name] == i)
            output_table.loc[mask, dim_name] = fuel_dict[i]
        return output_table
    # Convert synfuels to fffuels
    out_7514_1_2 = helper_7514(input_table=energy_demand_TWh_3)
    # Top : end-use = heating/cooking Bottom : other end-uses
    out_7514_1 = out_7514_1_2.loc[out_7514_1_2['end-use'].isin(['heating', 'cooking'])].copy()
    out_7514_1_excluded = out_7514_1_2.loc[~out_7514_1_2['end-use'].isin(['heating', 'cooking'])].copy()
    # Keep  energy-carrier = solid-biomass / solid-ff-coal
    out_7514_1_2 = out_7514_1.loc[out_7514_1['energy-carrier'].isin(['solid-ff-coal', 'solid-biomass'])].copy()
    out_7514_1_excluded_2 = out_7514_1.loc[~out_7514_1['energy-carrier'].isin(['solid-ff-coal', 'solid-biomass'])].copy()
    out_7514_1_excluded = pd.concat([out_7514_1_excluded_2, out_7514_1_excluded.set_index(out_7514_1_excluded.index.astype(str) + '_dup')])
    # RCP bld-energy-carrier-emission-factor-by-use [t/TWh]
    bld_energy_carrier_emission_factor_by_use_t_per_TWh = ImportDataNode(trigram='air', variable_name='bld-energy-carrier-emission-factor-by-use', variable_type='RCP')()
    # emissions[t] = energy-demand[TWh] * bld-energy-carrier-emission-factor-by-use [t/TWh]
    emissions_t = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=out_7514_1_2, input_table_2=bld_energy_carrier_emission_factor_by_use_t_per_TWh)
    # Group by Country, Years, pollutants (sum)
    emissions_t = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t)
    # RCP bld-energy-carrier-emission-factor [t/TWh]
    bld_energy_carrier_emission_factor_t_per_TWh = ImportDataNode(trigram='air', variable_name='bld-energy-carrier-emission-factor', variable_type='RCP')()
    # RCP tra-energy-carrier-emission-factor [t/TWh]
    tra_energy_carrier_emission_factor_t_per_TWh = ImportDataNode(trigram='air', variable_name='tra-energy-carrier-emission-factor', variable_type='RCP')()
    # RCP vkm-emission-factor-by-vehicle-user [t/vkm]
    vkm_emission_factor_by_vehicle_user_t_per_vkm = ImportDataNode(trigram='air', variable_name='vkm-emission-factor-by-vehicle-user', variable_type='RCP')()
    # emissions[t] = energy-demand[TWh] * tra-energy-carrier-emission-factor [t/TWh]
    emissions_t_2 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=final_transport_demand_vkm, input_table_2=vkm_emission_factor_by_vehicle_user_t_per_vkm)
    # Group by Country, Years, pollutants (sum)
    emissions_t_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_2)
    # RCP vkm-emission-factor-by-vehicle [t/vkm]
    vkm_emission_factor_by_vehicle_t_per_vkm = ImportDataNode(trigram='air', variable_name='vkm-emission-factor-by-vehicle', variable_type='RCP')()
    # RCP livestock-conv-factors [lsu/num] (from agriculture)
    livestock_conv_factors_lsu_per_num = ImportDataNode(trigram='agr', variable_name='livestock-conv-factors', variable_type='RCP')()
    # livestock-population[num] = livestock-population[lsu] / livestock-conv-factors [lsu/num]
    livestock_population_num = MCDNode(operation_selection='x / y', output_name='livestock-population[num]')(input_table_1=livestock_population_lsu, input_table_2=livestock_conv_factors_lsu_per_num)
    # emissions[t] = energy-demand[TWh] * tra-energy-carrier-emission-factor [t/TWh]
    emissions_t_3 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=final_transport_demand_vkm_excluded, input_table_2=vkm_emission_factor_by_vehicle_t_per_vkm)
    # Group by Country, Years, pollutants (sum)
    emissions_t_3 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_3)
    emissions_t_2 = pd.concat([emissions_t_2, emissions_t_3.set_index(emissions_t_3.index.astype(str) + '_dup')])
    # RCP lsu-emission-factor [t/animals]
    lsu_emission_factor_t_per_animals = ImportDataNode(trigram='air', variable_name='lsu-emission-factor', variable_type='RCP')()
    # emissions[t] = livestock-population[num] * lsu-emission-factor[t/animals]
    emissions_t_3 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=livestock_population_num, input_table_2=lsu_emission_factor_t_per_animals)
    # Group by Country, Years, pollutants (sum)
    emissions_t_3 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_3)
    # RCP oth-amendment-emission-factor [t/ktN]
    oth_amendment_emission_factor_t_per_ktN = ImportDataNode(trigram='air', variable_name='oth-amendment-emission-factor', variable_type='RCP')()
    # emissions[t] = amendment-application[ktN] * oth-amendment-emission-factor [t/ktN]
    emissions_t_4 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=amendment_application_ktN, input_table_2=oth_amendment_emission_factor_t_per_ktN)
    # Group by Country, Years, pollutants (sum)
    emissions_t_4 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_4)
    # RCP lsu-amendment-emission-factor [t/ktN]
    lsu_amendment_emission_factor_t_per_ktN = ImportDataNode(trigram='air', variable_name='lsu-amendment-emission-factor', variable_type='RCP')()
    # emissions[t] = N-manure-quantity[ktN] * lsu-amendment-emission-factor [t/ktN]
    emissions_t_5 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=N_manure_quantity_ktN, input_table_2=lsu_amendment_emission_factor_t_per_ktN)
    # Group by Country, Years, pollutants (sum)
    emissions_t_5 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_5)
    # RCP land-emission-factor [t/ha]
    land_emission_factor_t_per_ha = ImportDataNode(trigram='air', variable_name='land-emission-factor', variable_type='RCP')()
    # RCP ind-energy-carrier-emission-factor [t/TWh]
    ind_energy_carrier_emission_factor_t_per_TWh = ImportDataNode(trigram='air', variable_name='ind-energy-carrier-emission-factor', variable_type='RCP')()
    # emissions[t] = energy-demand[TWh] * ind-energy-carrier-emission-factor [t/TWh]
    emissions_t_6 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=energy_demand_TWh_2, input_table_2=ind_energy_carrier_emission_factor_t_per_TWh)
    # Group by Country, Years, pollutants (sum)
    emissions_t_6 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_6)
    # RCP ind-emission-factor-by-material [t/Mt]
    ind_emission_factor_by_material_t_per_Mt = ImportDataNode(trigram='air', variable_name='ind-emission-factor-by-material', variable_type='RCP')()
    # emissions[t] = material-production[Mt] * ind-emission-factor-by-material [t/Mt]
    emissions_t_7 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=material_production_Mt, input_table_2=ind_emission_factor_by_material_t_per_Mt)
    # Group by Country, Years, pollutants (sum)
    emissions_t_7 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_7)
    # RCP ind-emission-factor-by-material-route [t/Mt]
    ind_emission_factor_by_material_route_t_per_Mt = ImportDataNode(trigram='air', variable_name='ind-emission-factor-by-material-route', variable_type='RCP')()
    # emissions[t] = material-production[Mt] * ind-emission-factor-by-material-route [t/Mt]
    emissions_t_8 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=material_production_Mt, input_table_2=ind_emission_factor_by_material_route_t_per_Mt)
    # Group by Country, Years, pollutants (sum)
    emissions_t_8 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_8)
    emissions_t_7 = pd.concat([emissions_t_7, emissions_t_8.set_index(emissions_t_8.index.astype(str) + '_dup')])
    emissions_t_6 = pd.concat([emissions_t_6, emissions_t_7.set_index(emissions_t_7.index.astype(str) + '_dup')])
    # sector = ind
    emissions_t_6['sector'] = "ind"
    # RCP elc-energy-carrier-emission-factor [t/TWh]
    elc_energy_carrier_emission_factor_t_per_TWh = ImportDataNode(trigram='air', variable_name='elc-energy-carrier-emission-factor', variable_type='RCP')()
    # emissions[t] = energy-demand[TWh] * elc-energy-carrier-emission-factor [t/TWh]
    emissions_t_7 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=energy_demand_TWh, input_table_2=elc_energy_carrier_emission_factor_t_per_TWh)
    # Group by Country, Years, pollutants (sum)
    emissions_t_7 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_7)
    # sector = elc
    emissions_t_7['sector'] = "elc"
    emissions_t_6 = pd.concat([emissions_t_6, emissions_t_7.set_index(emissions_t_7.index.astype(str) + '_dup')])
    # emissions[t] = land-management[ha] * land-emission-factor [t/ha]
    emissions_t_7 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=land_management_ha, input_table_2=land_emission_factor_t_per_ha)
    # Group by Country, Years, pollutants (sum)
    emissions_t_7 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_7)
    emissions_t_5 = pd.concat([emissions_t_5, emissions_t_7.set_index(emissions_t_7.index.astype(str) + '_dup')])
    emissions_t_4 = pd.concat([emissions_t_4, emissions_t_5.set_index(emissions_t_5.index.astype(str) + '_dup')])
    emissions_t_3 = pd.concat([emissions_t_3, emissions_t_4.set_index(emissions_t_4.index.astype(str) + '_dup')])
    # sector = agr
    emissions_t_3['sector'] = "agr"
    emissions_t_3 = pd.concat([emissions_t_3, emissions_t_6.set_index(emissions_t_6.index.astype(str) + '_dup')])
    # emissions[t] = energy-demand[TWh] * tra-energy-carrier-emission-factor [t/TWh]
    emissions_t_4 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=out_9651_1, input_table_2=tra_energy_carrier_emission_factor_t_per_TWh)
    # Group by Country, Years, pollutants (sum)
    emissions_t_4 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_4)
    emissions_t_2 = pd.concat([emissions_t_4, emissions_t_2.set_index(emissions_t_2.index.astype(str) + '_dup')])
    # sector = tra
    emissions_t_2['sector'] = "tra"
    emissions_t_2 = pd.concat([emissions_t_2, emissions_t_3.set_index(emissions_t_3.index.astype(str) + '_dup')])
    # emissions[t] = energy-demand[TWh] * bld-energy-carrier-emission-factor [t/TWh]
    emissions_t_3 = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=out_7514_1_excluded, input_table_2=bld_energy_carrier_emission_factor_t_per_TWh)
    # Group by Country, Years, pollutants (sum)
    emissions_t_3 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant'], aggregation_method='Sum')(df=emissions_t_3)
    emissions_t = pd.concat([emissions_t, emissions_t_3.set_index(emissions_t_3.index.astype(str) + '_dup')])
    # sector = bld
    emissions_t['sector'] = "bld"
    emissions_t = pd.concat([emissions_t, emissions_t_2.set_index(emissions_t_2.index.astype(str) + '_dup')])

    # Calibrate emissions

    # Calibration pollutant-emission [t]
    pollutant_emission_t = ImportDataNode(trigram='air', variable_name='pollutant-emission', variable_type='Calibration')()
    # Keep sector = tot
    pollutant_emission_t_2 = pollutant_emission_t.loc[pollutant_emission_t['sector'].isin(['tot'])].copy()
    # Group by  all dimensions excluding sector (sum)
    pollutant_emission_t_2 = GroupByDimensions(groupby_dimensions=['Years', 'Country', 'pollutant'], aggregation_method='Sum')(df=pollutant_emission_t_2)
    # Group by  aal dimensions (sum)
    emissions_t = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'pollutant', 'sector'], aggregation_method='Sum')(df=emissions_t)

    # Calibration

    # Apply Calibration on emissions[t] detailled
    emissions_t, _, out_9608_3 = CalibrationNode(data_to_be_cal='emissions[t]', data_cal='pollutant-emission[t]')(input_table=emissions_t, cal_table=pollutant_emission_t)

    # Calibration RATES

    # Cal_rate for water-withdrawal [m3]

    # cal-rate for emissions[t] (detailled)
    cal_rate_emissions_t = UseVariableNode(selected_variable='cal_rate_emissions[t]')(input_table=out_9608_3)
    # emissions[t] to pollutant-emission [t]
    out_9650_1 = cal_rate_emissions_t.rename(columns={'cal_rate_emissions[t]': 'cal_rate_pollutant-emission[t]'})
    # Group by  all dimensions excluding sector (sum)
    emissions_t_2 = GroupByDimensions(groupby_dimensions=['Years', 'Country', 'pollutant'], aggregation_method='Sum')(df=emissions_t)

    # Calibration

    # Apply Calibration on emissions[t] total
    _, out_9613_2, _ = CalibrationNode(data_to_be_cal='emissions[t]', data_cal='pollutant-emission[t]')(input_table=emissions_t_2, cal_table=pollutant_emission_t_2)
    # Apply cal rate on detailled emissions
    emissions_t = MCDNode(operation_selection='x * y', output_name='emissions[t]')(input_table_1=emissions_t, input_table_2=out_9613_2)
    # emissions [t]
    emissions_t = ExportVariableNode(selected_variable='emissions[t]')(input_table=emissions_t)

    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer
    # 
    # - Emissions [t]

    # emissions [t]
    emissions_t = UseVariableNode(selected_variable='emissions[t]')(input_table=emissions_t)

    # Dispersion and population exposure
    # We convert t of pollutant to ug/m3

    # RCP air-dispersion [ug/m3/t of pollutant]
    air_dispersion_ug_per_m3_per_t_of_pollutant = ImportDataNode(trigram='air', variable_name='air-dispersion', variable_type='RCP')()
    # exposure[ug/m3] = emissions[t]  x gwp[ug/m3/t of polluant]
    exposure_ug_per_m3 = MCDNode(operation_selection='x * y', output_name='exposure[ug/m3]')(input_table_1=emissions_t, input_table_2=air_dispersion_ug_per_m3_per_t_of_pollutant)
    # CP air_pollutant-conversion-factor
    air_pollutant_conversion_factor = ImportDataNode(trigram='air', variable_name='air_pollutant-conversion-factor', variable_type='CP')()
    # Group by  pollutant (sum)
    air_pollutant_conversion_factor = GroupByDimensions(groupby_dimensions=['pollutant'], aggregation_method='Sum')(df=air_pollutant_conversion_factor)
    # exposure[ugPM2eq/m3] = exposure[ug/m3]  x weight_pollutant_to_PM
    exposure_ugPM2eq_per_m3 = MCDNode(operation_selection='x * y', output_name='exposure[ugPM2eq/m3]')(input_table_1=exposure_ug_per_m3, input_table_2=air_pollutant_conversion_factor)
    # RCP dispersion-scale-factor [-]  Usefull for cities : allows to take into account  difference in scales
    dispersion_scale_factor_ = ImportDataNode(trigram='air', variable_name='dispersion-scale-factor', variable_type='RCP')()
    # exposure[ugPM2eq/m3] (replace) = exposure[ugPM2eq/m3] * dispersion-scale-factor[-]
    exposure_ugPM2eq_per_m3 = MCDNode(operation_selection='x * y', output_name='exposure[ugPM2eq/m3]')(input_table_1=exposure_ugPM2eq_per_m3, input_table_2=dispersion_scale_factor_)
    # Group by  Country, Years (sum)
    exposure_ugPM2eq_per_m3 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=exposure_ugPM2eq_per_m3)
    # exposure [ugPM2eq/m3]
    exposure_ugPM2eq_per_m3 = ExportVariableNode(selected_variable='exposure[ugPM2eq/m3]')(input_table=exposure_ugPM2eq_per_m3)

    # For : Pathway Explorer
    # 
    # - Exposure (ug/m3)

    # exposure [ugPM2eq/m3]
    exposure_ugPM2eq_per_m3 = UseVariableNode(selected_variable='exposure[ugPM2eq/m3]')(input_table=exposure_ugPM2eq_per_m3)
    out_9649_1 = pd.concat([exposure_ugPM2eq_per_m3, emissions_t.set_index(emissions_t.index.astype(str) + '_dup')])

    # Health impact and health costs

    # Calibration mortality [num] (from lifestyle)
    mortality_num_2 = ImportDataNode(trigram='lfs', variable_name='mortality', variable_type='Calibration')()
    # Keep Years <= baseyear
    mortality_num, _ = FilterDimension(dimension='Years', operation_selection='≤', value_years=Globals.get().base_year)(df=mortality_num_2)
    # Keep Years = baseyear
    mortality_num_2, _ = FilterDimension(dimension='Years', operation_selection='=', value_years=Globals.get().base_year)(df=mortality_num_2)
    # OTS/FTS population [num] (from lifestyle)
    population_num = ImportDataNode(trigram='lfs', variable_name='population')()
    # baseline-mortality [death/pop] = mortality[num] / population[num]
    baseline_mortality_death_per_pop = MCDNode(operation_selection='x / y', output_name='baseline-mortality[death/pop]')(input_table_1=mortality_num_2, input_table_2=population_num)
    # By Country (sum)
    baseline_mortality_death_per_pop = GroupByDimensions(groupby_dimensions=['Country'], aggregation_method='Sum')(df=baseline_mortality_death_per_pop)
    # mortality[num] = baseline-mortality [death/pop] * population[num]
    mortality_num_2 = MCDNode(operation_selection='x * y', output_name='mortality[num]')(input_table_1=baseline_mortality_death_per_pop, input_table_2=population_num)
    # Keep Years > baseyear
    mortality_num_2, _ = FilterDimension(dimension='Years', operation_selection='>', value_years=Globals.get().base_year)(df=mortality_num_2)
    mortality_num = pd.concat([mortality_num, mortality_num_2.set_index(mortality_num_2.index.astype(str) + '_dup')])
    # By Country, Years (sum)
    mortality_num = GroupByDimensions(groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')(df=mortality_num)
    # relative-risk[-] = 1.062 ^ (exposure[ugPM2eq/m3]/10)
    relative_risk = exposure_ugPM2eq_per_m3.assign(**{'relative-risk[-]': 1.062**(exposure_ugPM2eq_per_m3['exposure[ugPM2eq/m3]']/10.0)})
    # attribuable-fraction[-] = (relative-risk[-] - 1) / relative-risk[-]
    attribuable_fraction = relative_risk.assign(**{'attribuable-fraction[-]': (relative_risk['relative-risk[-]']-1.0)/relative_risk['relative-risk[-]']})
    # attribuable-fraction [-]
    attribuable_fraction = UseVariableNode(selected_variable='attribuable-fraction[-]')(input_table=attribuable_fraction)
    # mortality[num] (replace) = attribuable-fraction[-] * mortality[num]
    mortality_num = MCDNode(operation_selection='x * y', output_name='mortality[num]')(input_table_1=attribuable_fraction, input_table_2=mortality_num)
    # RCP statistical-life-value [MEUR/deaths]
    statistical_life_value_MEUR_per_deaths = ImportDataNode(trigram='air', variable_name='statistical-life-value', variable_type='RCP')()
    # costs[MEUR] = mortality[num] * statistical-life-value [MEUR/deaths]
    costs_MEUR = MCDNode(operation_selection='x * y', output_name='costs[MEUR]')(input_table_1=mortality_num, input_table_2=statistical_life_value_MEUR_per_deaths)

    # For : Pathway Explorer
    # 
    # - Water exploitation index [%] (and mark lines)

    # costs [MEUR]
    costs_MEUR = UseVariableNode(selected_variable='costs[MEUR]')(input_table=costs_MEUR)
    out_9492_1 = pd.concat([out_9649_1, costs_MEUR.set_index(costs_MEUR.index.astype(str) + '_dup')])
    out_9493_1 = AddTrigram()(module_name=module_name, df=out_9492_1)

    return out_9493_1, out_9650_1


