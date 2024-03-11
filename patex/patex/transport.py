import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


# Transport module
def transport(lifestyle):
    # Passenger


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[MJ]


    # Hard codé ici ==> à terme, introduire dans une google sheet !


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-transport-demand[vkm]


    # Copy/Paste values for
    # PHEV(CE)*diesel
    # in
    # PHEV(CE)*elec


    # Disable electricity calibration (Based on a switch)
    # (ELIA calibration was previously done here)


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : energy-demand[MJ]


    # Parameters


    # Freight


    # Apply here MCE 
    # => based on eco_added-value-industry


    # Passenger


    # new-veh-fleet : 
    # remove Nan value for 1970 => ? Pas implémenté ici... car pas de Nan


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : vehicule-fleet[number]


    # Lifetime : copier ce qui est fait avec vehicule efficiency => même logique !
    # Envisager des noeuds qui font ce qui est fait ici ??


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : vehicule-fleet[number]


    # If negative, set to 0


    # veh-fleet / timestep


    # Freight


    # Copy/Paste values for : PHEV(CE)*diesel
    # in : PHEV(CE)*elec


    # Years > baseyear


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : vehicule-fleet[number]


    # Concatenate of a same variable
    # coming from differents sources
    # Here : total-veh-fleet[number]


    # If positive set to 0


    # MERGER : to create
    # Keep one metric if not Nan, else set the other one


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-new-veh-fleet[number]




    # Years <= baseyear


    # New vehicule fleet = 0
    # (when historic data)


    # Copy/Paste values for : PHEV(CE)*diesel
    # in : PHEV(CE)*elec


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : final-veh-fleet-remaining[number]


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : renewal-rate[%]


    # Historical = same as remaining




    # Remaining-factor =
    # (1-$$CURRENT_COLUMN$$)^$Timestep$


    # Quality Check
    # 
    # For technology-share 
    # => sum by veh-type should be = 1
    # 
    # No need to be converted in converter


    # Apply here MCE => based on eco_policies


    # Note : "fleet-age" could be removed from technology-share-mix (not really used here)


    # Avoir noeud + générique ??
    # Ou revoir les calculs ci-dessous ? Pas besoin de garder fleet-age et co ?


    # Note : 
    # 1) fleet-age = mettre new partout (quand missing) ? Ou on s'en fout de cette dimension ?
    # 
    # 2) Missing motor-type / energy-carrier for mix-type :
    # a) ZEV => walk / bike
    # b) ICE => maritime  / aviation + walk /bike
    # c) BEV => maritime  / aviation
    # 
    # => Python : do the job for marine + aviation
    # BUT SHOULD BE IN GOOGLE SHEET (not here !)


    # Use of variables


    # Concatenate of a same variable
    # 
    # Here : final-energy-demand[km]


    # Bottom part of the last filter should be empty.


    # Are not send anymore to Pathway Explorer
    # - tra_(technology | modal)-share.*
    # - tra_(...)-emissions.*
    # - tra_total-emissions.*
    # - tra_distance.*
    # - tra_domestic.*
    # - tra_avg-.*
    # - tra_occupancy.*
    # - tra_owned-cars.*
    # - tra_new-veh.*
    # - tra_length
    # - tra_biofuels.*
    # - tra_efuels.*
    # - tra_energy-demand_(total | by-fuel).*
    # - tra_(passenger | freight)_(energy-demand_by-fuel | veh-eff | (...)-emissions).*
    # - tra_freight_avg-.*
    # - tra_passenger_aver.*-car.*
    # - tra_passenger_public.*share.*


    # TEMP > delete when previous has been migrated


    # Select variables


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : modal-share[%]



    module_name = 'transport'

    # Apply fuel-switch levers (switch)
    # => Change fuel used for each transport (switch for fossil fuel to bio fuels)

    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for fuel switch
    out_9540_1 = pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['fffuels', 'liquid-ff-diesel', 'biofuels', 'liquid-bio-diesel'], ['fffuels', 'liquid-ff-gasoline', 'biofuels', 'liquid-bio-gasoline'], ['fffuels', 'gaseous-ff-natural', 'biofuels', 'gaseous-bio'], ['fffuels', 'liquid-ff-kerosene', 'biofuels', 'liquid-bio-kerosene'], ['fffuels', 'liquid-ff-marinefueloil', 'biofuels', 'liquid-bio-marinefueloil'], ['fffuels', 'liquid-ff-diesel', 'synfuels', 'liquid-syn-diesel'], ['fffuels', 'liquid-ff-gasoline', 'synfuels', 'liquid-syn-gasoline'], ['fffuels', 'gaseous-ff-natural', 'synfuels', 'gaseous-syn'], ['fffuels', 'liquid-ff-kerosene', 'synfuels', 'liquid-syn-kerosene'], ['fffuels', 'liquid-ff-marinefueloil', 'synfuels', 'liquid-syn-marinefueloil']])
    # ratio[-] = 1 (final energy ; no need to take into account differences  in fuel efficieny)
    ratio = out_9540_1.assign(**{'ratio[-]': 1.0})

    # Product / material / resources DEMAND

    # Vehicle demand

    # Fleet logic : recompute lifetime -- and -- technology-share  -- and -- veh-efficiency

    # Apply retrofit-switch
    # => Switch a part of the remaining historical fleet from ICE to BEV. It is done to cope with the limitation of the current stock logic (renewal rate applied to all the fleet, making the historical fleet harder to be replaced) while also being a relevent lever.
    # To use the X-switch lever, we have to create a new category (ie BEVnew) and to convert it afterwards to BEV (and convert its energy to electricity). 
    # These retrofitted vehicles are also sent to IND for production.

    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for retrofit-switch
    out_9472_1 = pd.DataFrame(columns=['category-from', 'motor-type-from', 'category-to', 'motor-type-to'], data=[['thermic', 'ICE', 'electric', 'BEVnew']])
    # ratio[-] = 1
    ratio_2 = out_9472_1.assign(**{'ratio[-]': 1.0})

    # Infrastructure demand

    # Infrastructures

    # mult-factor 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # mult-factor per material
    out_5173_1 = pd.DataFrame(columns=['mult-factor', 'material'], data=[[0.05, 'roads'], [0.05, 'rails'], [0.1, 'trolley-cables']])

    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for material
    out_5133_1 = pd.DataFrame(columns=['vehicule-type', 'material'], data=[['LDV', 'roads'], ['HDV', 'roads'], ['bus', 'roads'], ['rail', 'rails']])

    # Input data 
    # + inject module name (flow variable)

    # Transport demand

    # Passenger

    # Select variables from Lifestyle module

    # (LFS) distance-traveled (pkm)
    distance_traveled_pkm = use_variable(input_table=lifestyle, selected_variable='distance-traveled[pkm]')
    # (LFS) transport-demand aviation (pkm)
    transport_demand_pkm_2 = use_variable(input_table=lifestyle, selected_variable='transport-demand[pkm]')

    # KPI's (require extra computation)

    # Transport Demand

    # (LFS) capita (cap)
    population_cap = use_variable(input_table=lifestyle, selected_variable='population[cap]')

    # Renewal-rate : aggregated by transport-user + vehicle-type
    # Renewal-need
    # 
    # Utilité d'avoir un détail pour + que transport-user et vehicle-type pour lifetime ? ==> utilisé + loin ?
    # Ne concerne que passenger : LDV / bus - et - freight : LDV / HDV

    # OTS only

    # OTS (only) vehicle-lifetime-km [km]
    vehicle_lifetime_km_km = import_data(trigram='tra', variable_name='vehicle-lifetime-km', variable_type='OTS (only)')
    # Same as last available year
    vehicle_lifetime_km_km = add_missing_years(df_data=vehicle_lifetime_km_km)
    # Group by  transport-user, vehicule-type (max)
    vehicle_lifetime_km_km = group_by_dimensions(df=vehicle_lifetime_km_km, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Maximum')

    # Apply utilization-rate levers (improve)
    # => Determine the % of use of a vehicle (for non-public vehicle)

    # OTS / FTS utilization-rate
    utilization_rate = import_data(trigram='tra', variable_name='utilization-rate')
    # renewal-rate[%] = utilization-rate / lifetime
    renewal_rate_percent = mcd(input_table_1=vehicle_lifetime_km_km, input_table_2=utilization_rate, operation_selection='y / x', output_name='renewal-rate[%]')

    def helper_4996(input_table) -> pd.DataFrame:
        # Copy input to output => keep only years
        output_table = input_table.copy()
        output_table = output_table[["Years"]]
        
        # Remove duplicates
        output_table = output_table.drop_duplicates()
        # Sort on years
        output_table["year_i"] = output_table["Years"].astype(int)
        output_table = output_table.sort_values(by=['year_i'])
        output_table = output_table[["year_i"]]
        # Timestep
        output_table["Timestep"] = output_table.diff(axis=0)
        # Reset Years
        output_table["Years"] = output_table["year_i"].astype(int)
        output_table = output_table[["Years","Timestep"]]
        #Fill na with 0value
        output_table = output_table.fillna(0)
        return output_table
    # Timestep
    out_4996_1 = helper_4996(input_table=utilization_rate)
    # OTS (only) renewal-rate [%]
    renewal_rate_percent_2 = import_data(trigram='tra', variable_name='renewal-rate', variable_type='OTS (only)')
    # Same as last available year
    renewal_rate_percent_2 = add_missing_years(df_data=renewal_rate_percent_2)
    renewal_rate_percent = pd.concat([renewal_rate_percent, renewal_rate_percent_2.set_index(renewal_rate_percent_2.index.astype(str) + '_dup')])

    # Renewal-fleet

    # Select variables

    # renewal-rate [%]
    renewal_rate_percent_2 = use_variable(input_table=renewal_rate_percent, selected_variable='renewal-rate[%]')

    # Formating data for other modules + Pathway Explorer

    # For : Minerals

    # Keep all (renewal-rate)
    renewal_rate_percent_3 = column_filter(df=renewal_rate_percent, columns_to_drop=[])

    def helper_6019(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_freight_HDV = (output_table["transport-user"] == "freight") & (output_table["vehicule-type"] == "HDV")
        mask_freight_LDV = (output_table["transport-user"] == "freight") & (output_table["vehicule-type"] == "LDV")
        
        output_table.loc[mask_freight_HDV, "vehicule-type"] = "HDVH"
        output_table.loc[mask_freight_LDV, "vehicule-type"] = "HDVHL"
        return output_table
    # For freight : vehicule-type : HDV and LDV to HDVH and HDVL
    out_6019_1 = helper_6019(input_table=renewal_rate_percent_3)

    # Pivot

    out_6009_1, _, _ = pivoting(df=out_6019_1, agg_dict={'renewal-rate[%]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Years', 'Country'], list_pivots=['transport-user', 'vehicule-type'])
    out_6002_1 = missing_value_column_filter(df=out_6009_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Remove Aggregation Name
    out_6002_1 = column_rename_regex(df=out_6002_1, search_string='(.*)\\+.*', replace_string='$1')
    # Same as in Pathway Explorer
    out_6002_1 = column_rename_regex(df=out_6002_1, search_string='(.*)_(.*)', replace_string='tra_$1_renewal-rate_$2[%]')
    # Filter out aviation + WW +  marine + rail  + metro-tram
    out_6002_1 = column_filter(df=out_6002_1, pattern='^((?!.*aviation|IWW|marine|rail|metro-tram.*).)*$')

    # Add dummy columns (not modelized in XCalc)

    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_6004_1 = out_6002_1.copy()
    out_6004_1['tra_freight_renewal-rate_HDVM[%]'] = 0.0

    # Renewal-need =
    # 1-(1-$$CURRENT_COLUMN$$)^$Timestep$

    # Inner join
    out_4997_1 = joiner(df_left=renewal_rate_percent, df_right=out_4996_1, joiner='inner', left_input=['Years'], right_input=['Years'])
    # renewal-need[%] = 1-(1-renewal-rate[%])^timestep
    renewal_need_percent = out_4997_1.assign(**{'renewal-need[%]': 1.0-(1.0-out_4997_1['renewal-rate[%]'])**out_4997_1['Timestep']})
    # renewal-need [%]
    renewal_need_percent = export_variable(input_table=renewal_need_percent, selected_variable='renewal-need[%]')
    # renewal-need [%]
    renewal_need_percent = use_variable(input_table=renewal_need_percent, selected_variable='renewal-need[%]')

    # Freight

    # OTS only

    # OTS (only) national-share [%]
    national_share_percent = import_data(trigram='tra', variable_name='national-share', variable_type='OTS (only)')
    # Same as last available year
    national_share_percent = add_missing_years(df_data=national_share_percent)
    # OTS (only) avg-number-vehicles-tkm [veh/tkm]
    avg_number_vehicles_tkm_veh_per_tkm = import_data(trigram='tra', variable_name='avg-number-vehicles-tkm', variable_type='OTS (only)')
    # Same as last available year
    avg_number_vehicles_tkm_veh_per_tkm = add_missing_years(df_data=avg_number_vehicles_tkm_veh_per_tkm)

    # Passenger

    # OTS only

    # OTS (only) avg-number-vehicles-pkm [veh/bn_pkm]
    avg_number_vehicles_pkm_veh_per_bn_pkm = import_data(trigram='tra', variable_name='avg-number-vehicles-pkm', variable_type='OTS (only)')
    # Same as last available year
    avg_number_vehicles_pkm_veh_per_bn_pkm = add_missing_years(df_data=avg_number_vehicles_pkm_veh_per_bn_pkm)


    # Apply inland-demand levers (shift)
    # => determine the type of inland km traveled (transit, long distance, ...)

    # OTS/FTS demand-share-inland [%]
    demand_share_inland_percent = import_data(trigram='tra', variable_name='demand-share-inland')

    # Apply demand levers (avoid)
    # => determine the km demand for freight other than inland (eg. marine, aviation)

    # OTS/FTS international-freight-demand [bn_tkm]
    international_freight_demand_bn_tkm = import_data(trigram='tra', variable_name='international-freight-demand')

    # Modal-share levels are applied later in the flow

    # OTS/FTS modal-share [%]
    modal_share_percent = import_data(trigram='tra', variable_name='modal-share')

    # Calculation for post-calibration

    # keep only OTS (years <= baseyear)
    modal_share_percent_2, _ = filter_dimension(df=modal_share_percent, dimension='Years', operation_selection='≤', value_years=Globals.get().base_year)
    # Same as last available year
    modal_share_percent_2 = add_missing_years(df_data=modal_share_percent_2)
    # transport-demand[pkm] = modal-share[%]  * distance-traveled[pkm]
    transport_demand_pkm = mcd(input_table_1=modal_share_percent_2, input_table_2=distance_traveled_pkm, operation_selection='x * y', output_name='transport-demand[pkm]')

    # Transport-demand
    # By Country/Years/Vehicule-type

    # Group by  transport-user, vehicule-type (sum)
    transport_demand_pkm = group_by_dimensions(df=transport_demand_pkm, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    transport_demand_pkm_3 = pd.concat([transport_demand_pkm, transport_demand_pkm_2.set_index(transport_demand_pkm_2.index.astype(str) + '_dup')])
    # transport-demand[pkm] = modal-share[%]  * distance-traveled[pkm]
    transport_demand_pkm = mcd(input_table_1=modal_share_percent, input_table_2=distance_traveled_pkm, operation_selection='x * y', output_name='transport-demand[pkm]')
    # Group by  transport-user, vehicule-type (sum)
    transport_demand_pkm_4 = group_by_dimensions(df=transport_demand_pkm, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    transport_demand_pkm_2 = pd.concat([transport_demand_pkm_4, transport_demand_pkm_2.set_index(transport_demand_pkm_2.index.astype(str) + '_dup')])

    # Calibration

    # Calibration: transport-demand [pkm]
    transport_demand_pkm_pkm = import_data(trigram='tra', variable_name='transport-demand-pkm', variable_type='Calibration')
    # Apply Calibration on transport-demand[pkm]
    transport_demand_pkm_2, _, out_7099_3 = calibration(input_table=transport_demand_pkm_2, cal_table=transport_demand_pkm_pkm, data_to_be_cal='transport-demand[pkm]', data_cal='transport-demand[pkm]')

    # Calibration RATES

    # Cal_rate for transport-demand : bn_tkm and pkm

    # Cal rate for  transport-demand[pkm]
    cal_rate_transport_demand_pkm = use_variable(input_table=out_7099_3, selected_variable='cal_rate_transport-demand[pkm]')
    # bottom : all except aviation
    transport_demand_pkm_excluded = transport_demand_pkm_2.loc[transport_demand_pkm_2['vehicule-type'].isin(['bike', 'walk', '2W', 'LDV', 'bus', 'rail'])].copy()
    transport_demand_pkm_2 = transport_demand_pkm_2.loc[~transport_demand_pkm_2['vehicule-type'].isin(['bike', 'walk', '2W', 'LDV', 'bus', 'rail'])].copy()

    # Post-Calibration
    # To ensure that the total transport demand (pkm) is not affected by the modal-share lever (indeed if modes does not have the same CF, the total will change if the modal shares change). The post-CF are computed by calibrating the total (with the modal share given by the levers) to the total with the modal share of the base year (applied on all FTS years). For verification : These post-CF needs to be close to 1.

    # Group by  transport-user (sum)
    transport_demand_pkm_excluded_2 = group_by_dimensions(df=transport_demand_pkm_excluded, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # Apply Calibration on transport-demand[pkm]
    transport_demand_pkm_3, _, _ = calibration(input_table=transport_demand_pkm_3, cal_table=transport_demand_pkm_pkm, data_to_be_cal='transport-demand[pkm]', data_cal='transport-demand[pkm]')
    # bottom : all except aviation
    transport_demand_pkm_excluded_3 = transport_demand_pkm_3.loc[transport_demand_pkm_3['vehicule-type'].isin(['bike', 'walk', '2W', 'LDV', 'bus', 'rail'])].copy()
    # Group by  transport-user (sum)
    transport_demand_pkm_excluded_3 = group_by_dimensions(df=transport_demand_pkm_excluded_3, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # Calculate  Post-Calibration factors
    _, out_9522_2, _ = calibration(input_table=transport_demand_pkm_excluded_2, cal_table=transport_demand_pkm_excluded_3, data_to_be_cal='transport-demand[pkm]', data_cal='transport-demand[pkm]')
    # transport-demand[pkm] (replace) = transport-demand[pkm]  * cal_rate (post-calibration)
    transport_demand_pkm_3 = mcd(input_table_1=transport_demand_pkm_excluded, input_table_2=out_9522_2, operation_selection='x * y', output_name='transport-demand[pkm]')
    transport_demand_pkm_2 = pd.concat([transport_demand_pkm_2, transport_demand_pkm_3.set_index(transport_demand_pkm_3.index.astype(str) + '_dup')])

    # Final transport-demand

    # transport-demand[pkm]
    transport_demand_pkm_3 = export_variable(input_table=transport_demand_pkm_2, selected_variable='transport-demand[pkm]')

    # Select variables from transport demand

    # transport-demand [pkm]
    transport_demand_pkm_2 = use_variable(input_table=transport_demand_pkm_3, selected_variable='transport-demand[pkm]')
    # Convert Unit from pkm to bn_pkm
    transport_demand_bn_pkm = transport_demand_pkm_2.drop(columns='transport-demand[pkm]').assign(**{'transport-demand[bn_pkm]': transport_demand_pkm_2['transport-demand[pkm]'] * 1e-09})
    # veh-fleet-total[number] = veh-transport-demand[bn_pkm] * avg-number-vehicles-pkm[veh/bn_pkm]
    veh_fleet_total_number_2 = mcd(input_table_1=transport_demand_bn_pkm, input_table_2=avg_number_vehicles_pkm_veh_per_bn_pkm, operation_selection='x * y', output_name='veh-fleet-total[number]')
    # Pins (all selected)
    transport_demand_pkm_3 = column_filter(df=transport_demand_pkm_3, pattern='^.*$')

    # Final transport demand

    # Use of variables

    # transport-demand [pkm]
    transport_demand_pkm_3 = use_variable(input_table=transport_demand_pkm_3, selected_variable='transport-demand[pkm]')

    # Modal share of LDV passenger

    # transport-demand [pkm] (including bike and walk)
    transport_demand_pkm_4 = use_variable(input_table=transport_demand_pkm_3, selected_variable='transport-demand[pkm]')
    transport_demand_pkm_5 = transport_demand_pkm_4.loc[~transport_demand_pkm_4['vehicule-type'].isin(['aviation'])].copy()
    # Top: LDV
    transport_demand_pkm_6 = transport_demand_pkm_5.loc[transport_demand_pkm_5['vehicule-type'].isin(['LDV'])].copy()
    # Group by  year
    transport_demand_pkm_5 = group_by_dimensions(df=transport_demand_pkm_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # modal-share-passenger-LDV[pkm%] = transport-demand-per-cap (LDV) [pkm/cap] / transport-demand-per-cap (all) [pkm/cap]
    modal_share_passenger_LDV_percent_pkm = mcd(input_table_1=transport_demand_pkm_6, input_table_2=transport_demand_pkm_5, operation_selection='x / y', output_name='modal-share-passenger-LDV[%pkm]')
    # modal-share-passenger-LDV[pkm%] 
    modal_share_passenger_LDV_percent_pkm = export_variable(input_table=modal_share_passenger_LDV_percent_pkm, selected_variable='modal-share-passenger-LDV[%pkm]')

    # For : Pathway Explorer

    # Group by  dimensions
    transport_demand_pkm_4 = group_by_dimensions(df=transport_demand_pkm_4, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')

    # Apply occupancy levers (avoid)
    # => determine the nb of passenger per mode

    # OTS/FTS occupancy [pkm/vkm]
    occupancy_pkm_per_vkm = import_data(trigram='tra', variable_name='occupancy')

    # Apply automation levers (avoid)
    # => For LDV : determine % of automation through two levers :
    # - automation-share
    # - add-empty-km

    # OTS/FTS automation-add-empty-km [%]
    automation_add_empty_km_percent = import_data(trigram='tra', variable_name='automation-add-empty-km')
    # OTS/FTS automation-share [%]
    automation_share_percent = import_data(trigram='tra', variable_name='automation-share')
    # automation-factor[%] = 1 + automation-add-empty-km[%] * automation-share[%]
    automation_factor_percent = mcd(input_table_1=automation_add_empty_km_percent, input_table_2=automation_share_percent, operation_selection='1 + x * y', output_name='automation-factor[%]')

    # For : ALL RESULTS (allows visualization outside the module node)

    # Group by  transport-user (sum)
    transport_demand_pkm_5 = group_by_dimensions(df=transport_demand_pkm_2, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # Add total prefix
    transport_demand_pkm_5 = column_rename_regex(df=transport_demand_pkm_5, search_string='(.*\\[.*)', replace_string='total-$1')
    # modal-share[%] = transport-demand[pkm] / total-transport-demand[pkm]
    modal_share_percent_2 = mcd(input_table_1=transport_demand_pkm_5, input_table_2=transport_demand_pkm_2, operation_selection='y / x', output_name='modal-share[%]')
    # veh-transport-demand[vkm] = transport-demand[pkm] / occupancy[pkm/vkm]
    veh_transport_demand_vkm = mcd(input_table_1=transport_demand_pkm_2, input_table_2=occupancy_pkm_per_vkm, operation_selection='x / y', output_name='veh-transport-demand[vkm]')
    # veh-transport-demand[vkm] = (replace) veh-transport-demand[vkm] x automation-factor[%]  Fill missing factor with 1
    veh_transport_demand_vkm_2 = mcd(input_table_1=veh_transport_demand_vkm, input_table_2=automation_factor_percent, operation_selection='x * y', output_name='veh-transport-demand[vkm]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Pins (all selected)
    veh_transport_demand_vkm = column_filter(df=veh_transport_demand_vkm_2, pattern='^.*$')
    # veh-transport-demand [vkm] from pkm
    veh_transport_demand_vkm = use_variable(input_table=veh_transport_demand_vkm, selected_variable='veh-transport-demand[vkm]')

    # Apply utilization-rate levers (improve)
    # => Determine the % of use of a vehicle (for non-public vehicle)

    # veh-fleet-total[number] = veh-transport-demand[vkm] / utilization-rate[vkm/veh]
    veh_fleet_total_number = mcd(input_table_1=veh_transport_demand_vkm_2, input_table_2=utilization_rate, operation_selection='x / y', output_name='veh-fleet-total[number]')
    veh_fleet_total_number = pd.concat([veh_fleet_total_number_2, veh_fleet_total_number.set_index(veh_fleet_total_number.index.astype(str) + '_dup')])

    # Transport demand in urban area and fossil fuel consumption in urban area

    # transport-demand[pkm]
    transport_demand_pkm_5 = use_variable(input_table=transport_demand_pkm, selected_variable='transport-demand[pkm]')

    # Apply occupancy levers (avoid)
    # => determine the nb of passenger per mode

    # veh-transport-demand[vkm] = transport-demand[pkm] / occupancy[pkm/vkm]
    veh_transport_demand_vkm_2 = mcd(input_table_1=transport_demand_pkm_5, input_table_2=occupancy_pkm_per_vkm, operation_selection='x / y', output_name='veh-transport-demand[vkm]')
    # Top: only urban
    veh_transport_demand_vkm_3 = veh_transport_demand_vkm_2.loc[veh_transport_demand_vkm_2['distance-type'].isin(['urban'])].copy()
    # Remove distance-type
    veh_transport_demand_vkm_3 = group_by_dimensions(df=veh_transport_demand_vkm_3, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    # Remove distance-type
    veh_transport_demand_vkm_2 = group_by_dimensions(df=veh_transport_demand_vkm_2, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    # urban-share[%vkm] = transport-demand[vkm] (only urban) / transport-demand[vkm ] (total)
    urban_share_percent_vkm = mcd(input_table_1=veh_transport_demand_vkm_3, input_table_2=veh_transport_demand_vkm_2, operation_selection='x / y', output_name='urban-share[%vkm]')
    # Remove distance-type
    transport_demand_pkm = group_by_dimensions(df=transport_demand_pkm_5, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    # Top: only urban
    transport_demand_pkm_5 = transport_demand_pkm_5.loc[transport_demand_pkm_5['distance-type'].isin(['urban'])].copy()
    # Remove distance-type
    transport_demand_pkm_5 = group_by_dimensions(df=transport_demand_pkm_5, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    # urban-share[%pkm] = transport-demand[pkm] (only urban) / transport-demand[pkm ] (total)
    urban_share_percent_pkm = mcd(input_table_1=transport_demand_pkm_5, input_table_2=transport_demand_pkm, operation_selection='x / y', output_name='urban-share[%pkm]')
    # transport-demand-urban[pkm] = urban-share[%pkm] * transport-demand[pkm] (total)
    transport_demand_urban_pkm = mcd(input_table_1=urban_share_percent_pkm, input_table_2=transport_demand_pkm_2, operation_selection='x * y', output_name='transport-demand-urban[pkm]')
    # transport-demand-urban[pkm]
    transport_demand_urban_pkm_2 = export_variable(input_table=transport_demand_urban_pkm, selected_variable='transport-demand-urban[pkm]')
    # GroupBy transport-user
    transport_demand_urban_pkm_3 = group_by_dimensions(df=transport_demand_urban_pkm, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # urban-transport-share[%pkm] = transport-demand-urban[pkm] (per veh-type) / transport-demand-urban[pkm] (total)
    urban_transport_share_percent_pkm = mcd(input_table_1=transport_demand_urban_pkm, input_table_2=transport_demand_urban_pkm_3, operation_selection='x / y', output_name='urban-transport-share[%pkm]')
    # urban-transport-share[%pkm]
    urban_transport_share_percent_pkm = export_variable(input_table=urban_transport_share_percent_pkm, selected_variable='urban-transport-share[%pkm]')
    # KPI
    transport_pkm = pd.concat([transport_demand_urban_pkm_2, urban_transport_share_percent_pkm.set_index(urban_transport_share_percent_pkm.index.astype(str) + '_dup')])

    # Technology-share

    # Apply technology-share-new levers (improve)
    # => Determine the share of technology for new vehicles

    # OTS / FTS technology-share-new [%]
    technology_share_new_percent = import_data(trigram='tra', variable_name='technology-share-new')

    # Recompute the technology-share
    # ZEV / LEV / CEV = in column "mix-type"
    # % ZEV = % ZEV
    # % LEV = % LEV * (1 - % ZEV)
    # % ICE = (to be created) 1- %ZEV - % LEV (roads)
    # % ICE =  (to be created) 1- %BEV (marine / aviation / IWW)


    def helper_4985(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get dimension list
        dimensions = []
        for name in output_table.select_dtypes(include=['int32', 'int64', 'object']):  # object => refers to string
            dimensions.append(name)
        
        # Mask on ZEV and LEV
        mask_ZEV = (output_table["mix-type"] == "ZEV")
        mask_LEV = (output_table["mix-type"] == "LEV")
        mask_BEV = (output_table["mix-type"] == "BEV")
        
        # ZEV : stay like this ==> no need to do something
        
        # BEV : stay like this ==> no need to do something
        
        # LEV : LEV * (1 - ZEV)
        lev_table_generic = output_table.loc[mask_LEV,:].copy()
        # get values from ZEV
        zev_table = output_table.loc[mask_ZEV,:].copy()
        zev_table["technology-share-temp[%]"] = 1 - zev_table["technology-share-new[%]"]
        # keep only dimensions and "technology-share-temp[%]" from zev
        col_to_keep = dimensions.copy()
        col_to_keep.append("technology-share-temp[%]")
        zev_table_for_lev = zev_table[col_to_keep].copy()
        zev_table["technology-share-temp[%]"] = zev_table["technology-share-new[%]"]
        zev_table_for_ice = zev_table[col_to_keep].copy()
        # set ZEV to LEV to allow match
        zev_table_for_lev["mix-type"] = "LEV"
        # merge with ZEv with LEV
        lev_table = pd.merge(lev_table_generic, zev_table_for_lev, how="inner", on=dimensions)
        lev_table["technology-share-tot[%]"] = lev_table["technology-share-new[%]"] * lev_table["technology-share-temp[%]"]
        # drop unused columns
        del lev_table["technology-share-new[%]"]
        del lev_table["technology-share-temp[%]"]
        lev_table_for_ice = lev_table.copy()
        lev_table_for_ice["technology-share-temp2[%]"] = lev_table_for_ice["technology-share-tot[%]"]
        del lev_table_for_ice["technology-share-tot[%]"]
        del lev_table_for_ice["mix-type"] # Drop Mix-type => it will be keep from ZEV !
        
        # ICE : 1 - ZEV - LEV
        dimensions_ice = dimensions.copy()
        dimensions_ice.remove("mix-type")
        ice_table = pd.merge(lev_table_for_ice, zev_table_for_ice, how="outer", on=dimensions_ice) # Outer : to allow ZEV value with no LEV value
        # if Nan for ZEV or LEV ==> set 0
        mask_nan_values = (ice_table["technology-share-temp[%]"].isna())
        ice_table.loc[mask_nan_values, "technology-share-temp[%]"] = 0
        mask_nan_values = (ice_table["technology-share-temp2[%]"].isna())
        ice_table.loc[mask_nan_values, "technology-share-temp2[%]"] = 0
        # do 1 - ZEV - LEV
        ice_table["technology-share-tot[%]"] = 1 - ice_table["technology-share-temp[%]"] - ice_table["technology-share-temp2[%]"]
        # set "mix-type" = ICE
        ice_table["mix-type"] = "ICE"
        # drop unused columns
        del ice_table["technology-share-temp2[%]"]
        del ice_table["technology-share-temp[%]"]
        
        
        # ICE : 1 - BEV
        bev_table_generic = output_table.loc[mask_BEV,:].copy()
        # get values from BEV
        bev_table = output_table.loc[mask_BEV,:].copy()
        bev_table["technology-share-tot[%]"] = 1 - bev_table["technology-share-new[%]"]
        # set "mix-type" = ICE
        bev_table["mix-type"] = "ICE"
        # drop unused columns
        del bev_table["technology-share-new[%]"]
        
        
        # Concatenate ICE and LEV
        temp_table = pd.concat([ice_table, lev_table, bev_table])
        
        ## Merge all tables together ==> outer to keep ICE
        output_table = pd.merge(output_table, temp_table, how="outer", on=dimensions)
        mask_nan = (output_table["technology-share-tot[%]"].isna())
        output_table.loc[mask_nan,"technology-share-tot[%]"] = output_table.loc[mask_nan,"technology-share-new[%]"]
        return output_table
    out_4985_1 = helper_4985(input_table=technology_share_new_percent)
    # technology-share-tot [%] 
    technology_share_tot_percent = export_variable(input_table=out_4985_1, selected_variable='technology-share-tot[%]')

    # Apply technology-share-mix levers (improve)
    # => Determine the share of technology

    # OTS / FTS technology-share-mix [%]
    technology_share_mix_percent = import_data(trigram='tra', variable_name='technology-share-mix')
    # Same as last available year (some of the dimensions are OTS only and need to be completed up to 2050)
    technology_share_mix_percent = add_missing_years(df_data=technology_share_mix_percent)
    # technology-share[%] = technology-share-tot[%] x technology-share-mix[%]  Fill techno-share-mix missing with 1
    technology_share_percent = mcd(input_table_1=technology_share_tot_percent, input_table_2=technology_share_mix_percent, operation_selection='x * y', output_name='technology-share[%]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_5210(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Mask aviation
        mask_aviation = (output_table["vehicule-type"] == "aviation")
        mask_aviation_ice = (output_table["vehicule-type"] == "aviation") & (output_table["mix-type"] == "ICE")
        mask_aviation_bev = (output_table["vehicule-type"] == "aviation") & (output_table["mix-type"] == "BEV")
        # apply value
        output_table.loc[mask_aviation, "fleet-age"] = "new"
        output_table.loc[mask_aviation_ice, "energy-carrier"] = "liquid-ff-kerosene"
        output_table.loc[mask_aviation_ice, "motor-type"] = "ICE"
        output_table.loc[mask_aviation_bev, "energy-carrier"] = "electricity"
        output_table.loc[mask_aviation_bev, "motor-type"] = "BEV"
        
        # Mask mask_marine
        mask_marine = (output_table["vehicule-type"] == "marine")
        mask_marine_ice = (output_table["vehicule-type"] == "marine") & (output_table["mix-type"] == "ICE")
        mask_marine_bev = (output_table["vehicule-type"] == "marine") & (output_table["mix-type"] == "BEV")
        # apply value
        output_table.loc[mask_marine, "fleet-age"] = "new"
        output_table.loc[mask_marine_ice, "energy-carrier"] = "liquid-ff-marinefueloil"
        output_table.loc[mask_marine_ice, "motor-type"] = "ICE"
        output_table.loc[mask_marine_bev, "energy-carrier"] = "electricity"
        output_table.loc[mask_marine_bev, "motor-type"] = "BEV"
        
        # Mask mask_IWW
        mask_marine = (output_table["vehicule-type"] == "IWW")
        mask_marine_ice = (output_table["vehicule-type"] == "IWW") & (output_table["mix-type"] == "ICE")
        mask_marine_bev = (output_table["vehicule-type"] == "IWW") & (output_table["mix-type"] == "BEV")
        # apply value
        output_table.loc[mask_marine, "fleet-age"] = "new"
        output_table.loc[mask_marine_ice, "energy-carrier"] = "liquid-ff-marinefueloil"
        output_table.loc[mask_marine_ice, "motor-type"] = "ICE"
        output_table.loc[mask_marine_bev, "energy-carrier"] = "electricity"
        output_table.loc[mask_marine_bev, "motor-type"] = "BEV"
        
        # Years as int
        output_table["Years"] = output_table["Years"].astype(int)
        return output_table
    # For maritime / aviation and IWW set value for motor-type energy-carrier fleet-age
    out_5210_1 = helper_5210(input_table=technology_share_percent)
    # technology-share [%]
    technology_share_percent = export_variable(input_table=out_5210_1, selected_variable='technology-share[%]')

    # Select variables

    # technology-share [%]
    technology_share_percent = use_variable(input_table=technology_share_percent, selected_variable='technology-share[%]')
    # Only keep ZEV
    technology_share_percent_2 = technology_share_percent.loc[technology_share_percent['mix-type'].isin(['ZEV'])].copy()
    # Filter on  transport-user: Top: passenger bottom: freight
    technology_share_percent_3 = technology_share_percent_2.loc[technology_share_percent_2['transport-user'].isin(['passenger'])].copy()
    technology_share_percent_excluded = technology_share_percent_2.loc[~technology_share_percent_2['transport-user'].isin(['passenger'])].copy()
    # Filter on veh-type: Top: HDV
    technology_share_percent_excluded = technology_share_percent_excluded.loc[technology_share_percent_excluded['vehicule-type'].isin(['HDV'])].copy()
    # Filter on veh-type: Top: LDV and bus
    technology_share_percent_2 = technology_share_percent_3.loc[technology_share_percent_3['vehicule-type'].isin(['LDV', 'bus'])].copy()
    # Join freight and transport
    technology_share_percent_2 = pd.concat([technology_share_percent_2, technology_share_percent_excluded.set_index(technology_share_percent_excluded.index.astype(str) + '_dup')])

    # Share of ZEV in new sales (Passenger: LDV and BUS, Freight: HDV)

    # Group by en-carr, motor-type (sum)
    technology_share_percent_2 = group_by_dimensions(df=technology_share_percent_2, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'mix-type', 'fleet-age'], aggregation_method='Sum')
    # technology-share [%] to  technology-share-new [%]
    technology_share_percent_2 = column_rename_regex(df=technology_share_percent_2, search_string='technology-share', replace_string='technology-share-new')
    # technology-share-new [%]
    technology_share_new_percent = export_variable(input_table=technology_share_percent_2, selected_variable='technology-share-new[%]')
    # OTS (only) technology-share-fleet [%]
    technology_share_fleet_percent = import_data(trigram='tra', variable_name='technology-share-fleet', variable_type='OTS (only)')
    # Same as last available year
    technology_share_fleet_percent = add_missing_years(df_data=technology_share_fleet_percent)

    # Values for Years <= baseyear

    # technology-share-fleet [%]
    technology_share_fleet_percent = use_variable(input_table=technology_share_fleet_percent, selected_variable='technology-share-fleet[%]')
    # technology-share-fleet[%] to technology-share[%]
    technology_share_fleet_percent_2 = column_rename_regex(df=technology_share_fleet_percent, search_string='(technology-share)-fleet(\\[.*)', replace_string='$1$2')

    def helper_5321(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) <= int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years <= baseyears
    out_5321_1 = helper_5321(input_table=technology_share_fleet_percent_2)

    # Apply demand-inland levers (avoid)
    # => determine the km demand for freight for inland

    # OTS/FTS demand-inland-tkm [bn_tkm]
    demand_inland_tkm_bn_tkm = import_data(trigram='tra', variable_name='demand-inland-tkm')
    # distance-traveled[bn_tkm] = demand-inland[bn_tkm] x demand-share-inland[%]
    distance_traveled_bn_tkm = mcd(input_table_1=demand_inland_tkm_bn_tkm, input_table_2=demand_share_inland_percent, operation_selection='x * y', output_name='distance-traveled[bn_tkm]')

    # Apply modal-share levers (shift)
    # => determine the mode used to travel used to reach the km demand

    # transport-demand[bn_tkm] = modal-share[%] x distance-traveled[bn_tkm]
    transport_demand_bn_tkm = mcd(input_table_1=modal_share_percent, input_table_2=distance_traveled_bn_tkm, operation_selection='x * y', output_name='transport-demand[bn_tkm]')
    # Group by  transport-user, vehicule-type (sum)
    transport_demand_bn_tkm = group_by_dimensions(df=transport_demand_bn_tkm, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    demand_bn_tkm = pd.concat([transport_demand_bn_tkm, international_freight_demand_bn_tkm.set_index(international_freight_demand_bn_tkm.index.astype(str) + '_dup')])

    # Calibration

    # Calibration: transport-demand [bn_tkm]
    transport_demand_tkm_bn_tkm = import_data(trigram='tra', variable_name='transport-demand-tkm', variable_type='Calibration')
    # Apply Calibration on transport-demand[Gtkm]
    _, out_7100_2, out_7100_3 = calibration(input_table=demand_bn_tkm, cal_table=transport_demand_tkm_bn_tkm, data_to_be_cal='transport-demand[bn_tkm]', data_cal='transport-demand[bn_tkm]')

    # For the moment, LDV is not calibrated in XCalc, but this will be changed in the future...
    # For the moment, cal_rate = 1 for LDV
    # 
    # Idem for marine => set to 1


    def helper_9233(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        cal_col = 'cal_rate_transport-demand[bn_tkm]'
        
        mask_veh = (output_table["vehicule-type"] == "LDV") | (output_table["vehicule-type"] == "marine")
        output_table.loc[mask_veh, cal_col] = 1
        return output_table
    # If LDV / marine set to 1
    out_9233_1 = helper_9233(input_table=out_7100_3)
    # Cal rate for  transport-demand[tkm]
    cal_rate_transport_demand_bn_tkm = use_variable(input_table=out_9233_1, selected_variable='cal_rate_transport-demand[bn_tkm]')
    cal_rate_transport_demand = pd.concat([cal_rate_transport_demand_pkm, cal_rate_transport_demand_bn_tkm.set_index(cal_rate_transport_demand_bn_tkm.index.astype(str) + '_dup')])

    def helper_5779(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        cal_col = 'cal_rate_transport-demand[bn_tkm]'
        
        mask_veh = (output_table["vehicule-type"] == "LDV") | (output_table["vehicule-type"] == "marine")
        output_table.loc[mask_veh, cal_col] = 1
        return output_table
    # If LDV / marine set to 1
    out_5779_1 = helper_5779(input_table=out_7100_2)
    # transport-demand[Gtkm] = transport-demand[bn-tkm] x cal_rate  Fill missing cal_rate with 1
    transport_demand_bn_tkm = mcd(input_table_1=demand_bn_tkm, input_table_2=out_5779_1, operation_selection='x * y', output_name='transport-demand[bn_tkm]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Convert Unit from Gtkm to tkm
    transport_demand_tkm = transport_demand_bn_tkm.drop(columns='transport-demand[bn_tkm]').assign(**{'transport-demand[tkm]': transport_demand_bn_tkm['transport-demand[bn_tkm]'] * 1000000000.0})
    # transport-demand[tkm]
    transport_demand_tkm_2 = export_variable(input_table=transport_demand_tkm, selected_variable='transport-demand[tkm]')
    # Pins (all selected)
    transport_demand_tkm = column_filter(df=transport_demand_tkm_2, pattern='^.*$')
    # transport-demand [tkm]
    transport_demand_tkm = use_variable(input_table=transport_demand_tkm, selected_variable='transport-demand[tkm]')

    # Select variables

    # transport-demand[tkm]
    transport_demand_tkm_3 = use_variable(input_table=transport_demand_tkm, selected_variable='transport-demand[tkm]')
    # Group by  transport-user, vehicule-type
    transport_demand_tkm_3 = group_by_dimensions(df=transport_demand_tkm_3, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    transport_demand = pd.concat([transport_demand_tkm_3, transport_demand_pkm_4.set_index(transport_demand_pkm_4.index.astype(str) + '_dup')])

    # Apply load-factor levers (avoid)
    # => Determine...

    # OTS/FTS load-factor [tkm/vkm]
    load_factor_tkm_per_vkm = import_data(trigram='tra', variable_name='load-factor')

    # Select variables

    # transport-demand [tkm]
    transport_demand_tkm_2 = use_variable(input_table=transport_demand_tkm_2, selected_variable='transport-demand[tkm]')
    # Group by  transport-user (sum)
    transport_demand_tkm_3 = group_by_dimensions(df=transport_demand_tkm_2, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # Add total prefix
    transport_demand_tkm_3 = column_rename_regex(df=transport_demand_tkm_3, search_string='(.*\\[.*)', replace_string='total-$1')
    # modal-share[%] = transport-demand[tkm] / total-transport-demand[tkm]
    modal_share_percent = mcd(input_table_1=transport_demand_tkm_3, input_table_2=transport_demand_tkm_2, operation_selection='y / x', output_name='modal-share[%]')
    modal_share_percent = pd.concat([modal_share_percent_2, modal_share_percent.set_index(modal_share_percent.index.astype(str) + '_dup')])
    # modal-share[%]
    modal_share_percent = export_variable(input_table=modal_share_percent, selected_variable='modal-share[%]')
    # Group by  dimensions
    modal_share_percent = group_by_dimensions(df=modal_share_percent, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type'], aggregation_method='Sum')
    # veh-transport-demand[vkm] = transport-demand[tkm] / load-factor[tkm/vkm]  Missing load-factor : set to 1
    veh_transport_demand_vkm_2 = mcd(input_table_1=transport_demand_tkm_2, input_table_2=load_factor_tkm_per_vkm, operation_selection='x / y', output_name='veh-transport-demand[vkm]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # veh-fleet-total[number] = veh-transport-demand[vkm] / utilization-rate[vkm/veh]
    veh_fleet_total_number_2 = mcd(input_table_1=utilization_rate, input_table_2=veh_transport_demand_vkm_2, operation_selection='y / x', output_name='veh-fleet-total[number]')
    # veh-fleet-total[number] = veh-transport-demand[tkm] * avg-number-vehicles-tkm[veh/tkm]
    veh_fleet_total_number_3 = mcd(input_table_1=veh_transport_demand_vkm_2, input_table_2=avg_number_vehicles_tkm_veh_per_tkm, operation_selection='x * y', output_name='veh-fleet-total[number]')
    # veh-fleet-total[number] = (replace) total[number] x national-share  Missing value national-share set to 1
    veh_fleet_total_number_3 = mcd(input_table_1=veh_fleet_total_number_3, input_table_2=national_share_percent, operation_selection='x * y', output_name='veh-fleet-total[number]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    veh_fleet_total_number_2 = pd.concat([veh_fleet_total_number_2, veh_fleet_total_number_3.set_index(veh_fleet_total_number_3.index.astype(str) + '_dup')])

    # Historical vehicle fleet

    # CAL vehicle-fleet-historical [number]
    vehicle_fleet_historical_number = import_data(trigram='tra', variable_name='vehicle-fleet-historical', variable_type='Calibration')
    veh_fleet_total_number = pd.concat([veh_fleet_total_number, veh_fleet_total_number_2.set_index(veh_fleet_total_number_2.index.astype(str) + '_dup')])

    # Futur vehicle fleet

    # veh-fleet-total [number]
    veh_fleet_total_number = export_variable(input_table=veh_fleet_total_number, selected_variable='veh-fleet-total[number]')

    # Calibration

    # Apply Calibration on transport-demand[pkm]
    veh_fleet_total_number, _, _ = calibration(input_table=veh_fleet_total_number, cal_table=vehicle_fleet_historical_number, data_to_be_cal='veh-fleet-total[number]', data_cal='vehicle-fleet-historical[number]')
    # veh-fleet-total [number]
    veh_fleet_total_number = export_variable(input_table=veh_fleet_total_number, selected_variable='veh-fleet-total[number]')
    # Lag  veh-fleet-total[number]
    out_7157_1, _ = lag_variable(df=veh_fleet_total_number, in_var='veh-fleet-total[number]')
    # veh-fleet-total-lagged [number]
    veh_fleet_total_lagged_number = use_variable(input_table=out_7157_1, selected_variable='veh-fleet-total_lagged[number]')
    # veh-fleet-renewal[number] = veh-fleet-total-lagged[number] * renewal-need[%]
    veh_fleet_renewal_number = mcd(input_table_1=veh_fleet_total_lagged_number, input_table_2=renewal_need_percent, operation_selection='x * y', output_name='veh-fleet-renewal[number]')
    # veh-fleet-remaining[number] = veh-fleet-total-lagged[number] - veh-fleet-renewal[number]
    veh_fleet_remaining_number = mcd(input_table_1=veh_fleet_total_lagged_number, input_table_2=veh_fleet_renewal_number, operation_selection='x - y', output_name='veh-fleet-remaining[number]')
    # veh-fleet-total [number]
    veh_fleet_total_number = use_variable(input_table=veh_fleet_total_number, selected_variable='veh-fleet-total[number]')
    # veh-fleet-historical[number] = veh-fleet-total[number] x technology-share-fleet[%]
    veh_fleet_historical_number = mcd(input_table_1=technology_share_fleet_percent, input_table_2=veh_fleet_total_number, operation_selection='x * y', output_name='veh-fleet-historical[number]')

    # Final vehicule fleet : total, remaining, new

    # Values for Years <= baseyear

    # Use of variables

    # veh-fleet-historical [number]
    veh_fleet_historical_number = export_variable(input_table=veh_fleet_historical_number, selected_variable='veh-fleet-historical[number]')

    def helper_5573(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_PHEV = (output_table["motor-type"] == "PHEV") | (output_table["motor-type"] == "PHEVCE")
        
        output_table_elec = output_table.loc[mask_PHEV,:]
        output_table_elec["energy-carrier"] = "electricity"
        
        output_table = pd.concat([output_table, output_table_elec], ignore_index = True)
        return output_table
    # Copy/Paste values for motor-type = PHEV(CE) from diesel to elec
    out_5573_1 = helper_5573(input_table=veh_fleet_historical_number)

    def helper_5569(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) <= int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years <= baseyears
    out_5569_1 = helper_5569(input_table=out_5573_1)
    # historical = remaining
    out_5583_1 = out_5569_1.rename(columns={'veh-fleet-historical[number]': 'final-veh-fleet-remaining[number]'})

    # Energy DEMAND

    # Vehicule-efficiency

    # Apply vehicle efficiency levers (improve)
    # => Determine the efficiency of each vehicle and then the energy-demand for each vehicle

    # OTS / FTS veh-efficiency-tkm [MJ/tkm]
    veh_efficiency_tkm_MJ_per_tkm = import_data(trigram='tra', variable_name='veh-efficiency-tkm')
    # Keep new
    veh_efficiency_tkm_MJ_per_tkm_2 = veh_efficiency_tkm_MJ_per_tkm.loc[veh_efficiency_tkm_MJ_per_tkm['fleet-age'].isin(['new'])].copy()
    veh_efficiency_tkm_MJ_per_tkm_excluded = veh_efficiency_tkm_MJ_per_tkm.loc[~veh_efficiency_tkm_MJ_per_tkm['fleet-age'].isin(['new'])].copy()
    # OTS / FTS veh-efficiency-pkm [MJ/pkm]
    veh_efficiency_pkm_MJ_per_pkm = import_data(trigram='tra', variable_name='veh-efficiency-pkm')
    # Keep new
    veh_efficiency_pkm_MJ_per_pkm_2 = veh_efficiency_pkm_MJ_per_pkm.loc[veh_efficiency_pkm_MJ_per_pkm['fleet-age'].isin(['new'])].copy()
    veh_efficiency_pkm_MJ_per_pkm_excluded = veh_efficiency_pkm_MJ_per_pkm.loc[~veh_efficiency_pkm_MJ_per_pkm['fleet-age'].isin(['new'])].copy()
    veh_efficiency_MJ_per_excluded = pd.concat([veh_efficiency_tkm_MJ_per_tkm_excluded, veh_efficiency_pkm_MJ_per_pkm_excluded.set_index(veh_efficiency_pkm_MJ_per_pkm_excluded.index.astype(str) + '_dup')])
    veh_efficiency_MJ_per = pd.concat([veh_efficiency_tkm_MJ_per_tkm_2, veh_efficiency_pkm_MJ_per_pkm_2.set_index(veh_efficiency_pkm_MJ_per_pkm_2.index.astype(str) + '_dup')])
    # OTS / FTS veh-efficiency-km [MJ/km]
    veh_efficiency_km_MJ_per_km = import_data(trigram='tra', variable_name='veh-efficiency-km')
    # Keep new
    veh_efficiency_km_MJ_per_km_2 = veh_efficiency_km_MJ_per_km.loc[veh_efficiency_km_MJ_per_km['fleet-age'].isin(['new'])].copy()
    veh_efficiency_km_MJ_per_km_excluded = veh_efficiency_km_MJ_per_km.loc[~veh_efficiency_km_MJ_per_km['fleet-age'].isin(['new'])].copy()
    veh_efficiency_MJ_per_excluded = pd.concat([veh_efficiency_MJ_per_excluded, veh_efficiency_km_MJ_per_km_excluded.set_index(veh_efficiency_km_MJ_per_km_excluded.index.astype(str) + '_dup')])

    # Vehicule-efficiency : actual fleet vehicule

    # Drop fleet-age
    veh_efficiency_MJ_per_excluded = column_filter(df=veh_efficiency_MJ_per_excluded, columns_to_drop=['fleet-age'])
    # Add fleet suffix to veh-eff
    veh_efficiency_MJ_per_excluded = column_rename_regex(df=veh_efficiency_MJ_per_excluded, search_string='(veh-eff.*)', replace_string='fleet-$1')
    veh_efficiency_MJ_per = pd.concat([veh_efficiency_MJ_per, veh_efficiency_km_MJ_per_km_2.set_index(veh_efficiency_km_MJ_per_km_2.index.astype(str) + '_dup')])

    # Vehicule-efficiency : new vehicule

    # Drop fleet-age
    veh_efficiency_MJ_per = column_filter(df=veh_efficiency_MJ_per, columns_to_drop=['fleet-age'])
    # Add new suffix to veh-eff
    veh_efficiency_MJ_per = column_rename_regex(df=veh_efficiency_MJ_per, search_string='(veh-eff.*)', replace_string='new-$1')
    # Outer Join : get fleet and new veh-efficiency values
    out_5365_1 = joiner(df_left=veh_efficiency_MJ_per, df_right=veh_efficiency_MJ_per_excluded, joiner='outer', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])

    # Average new vehicules GHG emissions (Passenger: LDV, 2W and BUS, Freight: HDV)

    # veh-efficiency-km[MJ/km]
    veh_efficiency_km_MJ_per_km = use_variable(input_table=veh_efficiency_km_MJ_per_km, selected_variable='veh-efficiency-km[MJ/km]')
    # veh-efficiency-km-avg[MJ/km] = veh-efficiency-km[MJ/km] * technology-share [%]
    veh_efficiency_km_avg_MJ_per_km = mcd(input_table_1=veh_efficiency_km_MJ_per_km, input_table_2=technology_share_percent, operation_selection='x * y', output_name='veh-efficiency-km-avg[MJ/km]')
    # FOR INDUSTRY : veh-fleet-renewal-ind[number] = veh-fleet[number] * renewal-rate[%]
    veh_fleet_renewal_ind_number = mcd(input_table_1=renewal_rate_percent_2, input_table_2=veh_fleet_total_number, operation_selection='x * y', output_name='veh-fleet-renewal-ind[number]')
    # veh-fleet-need[number] = veh-fleet-total[number] - veh-fleet-total-lagged[number]
    veh_fleet_need_number = mcd(input_table_1=veh_fleet_total_number, input_table_2=veh_fleet_total_lagged_number, operation_selection='x - y', output_name='veh-fleet-need[number]')
    # FOR INDUSTRY : veh-fleet-need-ind[number] = veh-fleet-need[number] if < 0 => the set to 0
    veh_fleet_need_number_2 = veh_fleet_need_number.copy()
    mask = veh_fleet_need_number_2['veh-fleet-need[number]']<0
    veh_fleet_need_number_2.loc[mask, 'veh-fleet-need[number]'] =  0
    veh_fleet_need_number_2.loc[~mask, 'veh-fleet-need[number]'] =  veh_fleet_need_number_2.loc[~mask, 'veh-fleet-need[number]']

    def helper_5286(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get timestep
        temp_table = output_table.copy()
        temp_table = temp_table[['Years']]
        temp_table = temp_table.sort_values(by=['Years'])
        temp_table = temp_table.drop_duplicates()
        temp_table["Timestep"] = abs(temp_table.diff(periods=1))
        temp_table = temp_table.fillna(0)
        
        # Join timstep to initial table
        output_table = pd.merge(output_table, temp_table, on=["Years"], how="inner")
        
        # Devide input_table['veh-fleet-need[number]'] by timestep
        output_table['veh-fleet-need[number]'] = output_table['veh-fleet-need[number]'] / output_table['Timestep']
        # Remove timestep
        del temp_table["Timestep"]
        return output_table
    out_5286_1 = helper_5286(input_table=veh_fleet_need_number_2)
    # new-veh-fleet-ind[number] = veh-fleet-renewal-ind[number] + veh-fleet-need-ind[number]
    new_veh_fleet_ind_number = mcd(input_table_1=veh_fleet_renewal_ind_number, input_table_2=out_5286_1, operation_selection='x + y', output_name='new-veh-fleet-ind[number]', fill_value_bool='Inner Join')
    # set to 0
    new_veh_fleet_ind_number = missing_value(df=new_veh_fleet_ind_number, DTS_DT_O=[['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # new-veh-fleet-ind [number]
    new_veh_fleet_ind_number = use_variable(input_table=new_veh_fleet_ind_number, selected_variable='new-veh-fleet-ind[number]')
    # final-veh-fleet-ind[number] = new-veh-fleet-ind[number] x technology-share[%] sent to IND
    final_veh_fleet_ind_number = mcd(input_table_1=new_veh_fleet_ind_number, input_table_2=technology_share_percent, operation_selection='x * y', output_name='final-veh-fleet-ind[number]')
    # Group by  vehicule-type, motor-type, energy-carrier (sum)
    final_veh_fleet_ind_number_2 = group_by_dimensions(df=final_veh_fleet_ind_number, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')

    # Costs

    # final-veh-fleet-ind [number]
    final_veh_fleet_ind_number_2 = use_variable(input_table=final_veh_fleet_ind_number_2, selected_variable='final-veh-fleet-ind[number]')
    # RCP costs-by-vehicle [MEUR/number] From TECH
    costs_by_vehicle_MEUR_per_number = import_data(trigram='tec', variable_name='costs-by-vehicle', variable_type='RCP')

    # For : Industry

    # Select variables

    # final-veh-fleet-ind[number]
    final_veh_fleet_ind_number = use_variable(input_table=final_veh_fleet_ind_number, selected_variable='final-veh-fleet-ind[number]')

    def helper_5410(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        veh_type_with_same_motor = ["rail", "marine", "IWW", "aviation"]
        
        for veh in veh_type_with_same_motor:
            mask_veh = (output_table["vehicule-type"] == veh)
            output_table.loc[mask_veh, "motor-type"] = "same"
        
        
        mask_PHEVCE = (output_table["motor-type"] == "PHEVCE")
        output_table.loc[mask_PHEVCE, "motor-type"] = "PHEV"
        mask_HEV = (output_table["motor-type"] == "HEV")
        output_table.loc[mask_HEV, "motor-type"] = "PHEV"
        return output_table
    # For vehicule-type = rail /marine / IWW / aviation motor-type not needed (set as "same")  AND  PHEVCE => PHEV HEV => PHEV
    out_5410_1 = helper_5410(input_table=final_veh_fleet_ind_number)
    # Group by  vehicule-type, motor-type (sum)
    out_5410_1_2 = group_by_dimensions(df=out_5410_1, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'motor-type'], aggregation_method='Sum')

    # For : Minerals

    # Keep all
    out_5410_1 = column_filter(df=out_5410_1, pattern='^.*$')

    def helper_5457(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_freight_HDV = (output_table["transport-user"] == "freight") & (output_table["vehicule-type"] == "HDV")
        mask_freight_LDV = (output_table["transport-user"] == "freight") & (output_table["vehicule-type"] == "LDV")
        
        output_table.loc[mask_freight_HDV, "vehicule-type"] = "HDVH"
        output_table.loc[mask_freight_LDV, "vehicule-type"] = "HDVL"
        return output_table
    # For freight : vehicule-type : HDV and LDV to HDVH and HDVL
    out_5457_1 = helper_5457(input_table=out_5410_1)
    # Group by  vehicule-type, motor-type (sum)
    out_5457_1 = group_by_dimensions(df=out_5457_1, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'motor-type'], aggregation_method='Sum')

    # Pivot

    out_5459_1, _, _ = pivoting(df=out_5457_1, agg_dict={'final-veh-fleet-ind[number]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Years', 'Country'], list_pivots=['vehicule-type', 'motor-type'])
    out_5460_1 = missing_value_column_filter(df=out_5459_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Remove Aggregation Name
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='(.*)\\+.*', replace_string='$1')
    # Same as in Pathway Explorer
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='(.*)_(.*)', replace_string='tra_$1_$2[number]')
    # BEV to EV
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='_BEV\\[', replace_string='_EV[')
    # aviation to planes
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='aviation', replace_string='planes')
    # rail to trains
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='rail', replace_string='trains')
    # remove _same
    out_5460_1 = column_rename_regex(df=out_5460_1, search_string='_same\\[', replace_string='[')
    # ships = marine + IWW
    tra_ships_number = out_5460_1.assign(**{'tra_ships[number]': out_5460_1['tra_IWW[number]']+out_5460_1['tra_marine[number]']})
    # Filter out marine + IWW
    tra_ships_number = column_filter(df=tra_ships_number, columns_to_drop=['tra_IWW[number]', 'tra_marine[number]'])

    # Add dummy columns (not modelized in XCalc)

    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4567_1 = tra_ships_number.copy()
    out_4567_1['tra_2W_PHEV[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4568_1 = out_4567_1.copy()
    out_4568_1['tra_2W_FCEV[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4569_1 = out_4568_1.copy()
    out_4569_1['tra_HDVM_ICE[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4570_1 = out_4569_1.copy()
    out_4570_1['tra_HDVM_EV[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4571_1 = out_4570_1.copy()
    out_4571_1['tra_HDVM_PHEV[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4572_1 = out_4571_1.copy()
    out_4572_1['tra_HDVM_FCEV[number]'] = 0.0
    # Add dummy 0 values for columns not modelled in XCalc and asked by Minerals
    out_4573_1 = out_4572_1.copy()
    out_4573_1['tra_subways[number]'] = 0.0
    # Join data for minerals
    out_6021_1 = joiner(df_left=out_6004_1, df_right=out_4573_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # new-veh-fleet[number] = veh-fleet-need[number] + veh-fleet-renewal[number]
    new_veh_fleet_number = mcd(input_table_1=veh_fleet_need_number, input_table_2=veh_fleet_renewal_number, operation_selection='x + y', output_name='new-veh-fleet[number]')
    # if new-veh-fleet[number] < 0 then set to 0
    new_veh_fleet_number_2 = new_veh_fleet_number.copy()
    mask = new_veh_fleet_number_2['new-veh-fleet[number]'] < 0
    new_veh_fleet_number_2.loc[mask, 'new-veh-fleet[number]'] =  0
    new_veh_fleet_number_2.loc[~mask, 'new-veh-fleet[number]'] =  new_veh_fleet_number_2.loc[~mask, 'new-veh-fleet[number]']
    # new-veh-fleet [number]
    new_veh_fleet_number_2 = use_variable(input_table=new_veh_fleet_number_2, selected_variable='new-veh-fleet[number]')
    # final-new-veh-fleet[number] = new-veh-fleet[number] x technology-share[%]
    final_new_veh_fleet_number = mcd(input_table_1=new_veh_fleet_number_2, input_table_2=technology_share_percent, operation_selection='x * y', output_name='final-new-veh-fleet[number]')
    # Group by  transport-user, vehicule-type, energy-carrier, motor-type (sum)
    final_new_veh_fleet_number = group_by_dimensions(df=final_new_veh_fleet_number, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')
    # if new-veh-fleet[number] > 0 then set to 0
    mask = new_veh_fleet_number['new-veh-fleet[number]'] > 0
    new_veh_fleet_number.loc[mask, 'new-veh-fleet[number]'] =  0
    new_veh_fleet_number.loc[~mask, 'new-veh-fleet[number]'] =  new_veh_fleet_number.loc[~mask, 'new-veh-fleet[number]']

    # Substract negative values from new-veh-fleet to remaining fleet

    # tot-veh-fleet-remaining[number] = veh-fleet-remaining + new-veh-fleet[number] (negative values only)
    tot_veh_fleet_remaining_number = mcd(input_table_1=new_veh_fleet_number, input_table_2=veh_fleet_remaining_number, operation_selection='x + y', output_name='tot-veh-fleet-remaining[number]')
    # tot-veh-fleet-remaining [number]
    tot_veh_fleet_remaining_number = use_variable(input_table=tot_veh_fleet_remaining_number, selected_variable='tot-veh-fleet-remaining[number]')
    # final-veh-fleet-remaining[number] = tot-veh-fleet-remaining[number] x technology-share-fleet[%]
    final_veh_fleet_remaining_number = mcd(input_table_1=tot_veh_fleet_remaining_number, input_table_2=technology_share_fleet_percent, operation_selection='x * y', output_name='final-veh-fleet-remaining[number]')
    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    final_veh_fleet_remaining_number = group_by_dimensions(df=final_veh_fleet_remaining_number, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')
    # Conactenate to have all available mix
    final_veh_fleet_number = pd.concat([final_new_veh_fleet_number, final_veh_fleet_remaining_number.set_index(final_veh_fleet_remaining_number.index.astype(str) + '_dup')])

    # Values for Years > baseyear

    # MDC : FILL MISSING VALUES WITH CONSTANT  ?
    # ==> SINON : AVOIR DES FLUX SEPARES ??
    # ==> + Replace column name ??!!

    # set to 0 if missing (no vehicule)
    final_veh_fleet_number = missing_value(df=final_veh_fleet_number, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # total-veh-fleet[number] = final-new-veh-fleet[number] + final-veh-fleet-remaining[number]
    total_veh_fleet_number = final_veh_fleet_number.assign(**{'total-veh-fleet[number]': final_veh_fleet_number['final-new-veh-fleet[number]']+final_veh_fleet_number['final-veh-fleet-remaining[number]']})

    def helper_5314(input_table) -> pd.DataFrame:
        # Import pandas
        
        
        # Measures columns
        tot_veh_fleet = 'total-veh-fleet[number]'
        tot_veh_fleet_agg = 'total-veh-fleet-agg[number]'
        veh_fleet_remaining = 'final-veh-fleet-remaining[number]'
        veh_fleet_remaining_agg = 'final-veh-fleet-remaining-agg[number]'
        
        # Group by on :
        group_by_col = ["Years", "Country", "transport-user", "vehicule-type"]
        group_by_col_metrics = group_by_col + [tot_veh_fleet, veh_fleet_remaining]
        
        # Copy input to output
        output_table = input_table.copy()
        
        # Dimensions columns => we group all values on these dimensions
        dimensions = []
        for name in output_table.select_dtypes(include=['int32', 'int64', 'object']):  # object => refers to string
            dimensions.append(name)
        
        # Group by dimensions => sum
        output_table = output_table.groupby(dimensions, as_index=False).sum()
        
        # Group by group_by_col => sum
        output_table_agg = output_table.copy()
        output_table_agg = output_table_agg[group_by_col_metrics] # Keep only desired columns
        output_table_agg = output_table_agg.groupby(group_by_col, as_index=False).sum()
        output_table_agg = output_table_agg.rename(columns = {tot_veh_fleet:tot_veh_fleet_agg, veh_fleet_remaining:veh_fleet_remaining_agg})
        output_table_agg = output_table_agg.drop_duplicates()
        
        # Add group by values to initial table
        output_table = pd.merge(output_table, output_table_agg, on = group_by_col, how = "inner")
        return output_table
    # total-veh-fleet-agg[number] and total-veh-fleet-remaining-agg[number] =  groupby (Country, Years, transport-user, vehicule-type) SUM of  total-veh-fleet[number] and of final-veh-fleet-remaining[number]
    out_5314_1 = helper_5314(input_table=total_veh_fleet_number)
    # Left Outer Join Add technology-share to fleet data
    out_5319_1 = joiner(df_left=out_5314_1, df_right=technology_share_fleet_percent, joiner='left', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])
    # set to 0 if missing (no vehicule)
    out_5319_1 = missing_value(df=out_5319_1, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    def helper_5315(input_table) -> pd.DataFrame:
        #Import pandas
        
        
        # Copy input to output
        output_table = input_table.copy()
        
        # Define years
        baseyear = int(Globals.get().base_year)
        endyear = 2050
        
        # Measures columns
        tot_veh_fleet_agg = 'total-veh-fleet-agg[number]'
        tot_veh_fleet = 'total-veh-fleet[number]'
        veh_fleet_remaining_agg = 'final-veh-fleet-remaining-agg[number]'
        veh_fleet_remaining = 'final-veh-fleet-remaining[number]'
        veh_fleet_new = 'final-new-veh-fleet[number]'
        techno_share_old = 'technology-share-fleet[%]'
        techno_share_new = 'technology-share[%]'
        
        # Get dimension list
        dimensions = []
        for name in output_table.select_dtypes(include=['int32', 'object']):  # object => refers to string
            dimensions.append(name)
        
        # Timestep
        temp_table = output_table.copy()
        temp_table = temp_table[['Years']]
        temp_table = temp_table.sort_values(by=['Years'])
        temp_table = temp_table.drop_duplicates()
        temp_table["Timestep"] = abs(temp_table.diff(periods=1))
        temp_table = temp_table.fillna(0)
        # Merge timestep with output_table
        output_table = pd.merge(output_table, temp_table, on=["Years"], how="inner")
        
        # Get possible years
        year_values = []
        for year in output_table["Years"].values:
            if year not in year_values and year >= baseyear:
                year_values.append(int(year))
        
        # By default : techno_share_new = techno_share_old
        output_table[techno_share_new] = output_table[techno_share_old]
        
        # For year in baseyear => endyear
        for i in range(1, len(year_values)):
        
            # Mask for years
            year = year_values[i]
            year_prev = year_values[i - 1]
            mask_y = (output_table["Years"] == year)
            mask_prev = (output_table["Years"] == year_prev)
        
            # Table for year i
            output_table_y = output_table.loc[mask_y, :].copy()
            # Table for year i - 1 => Keep only technology-share[%]
            output_table_prev = output_table.loc[mask_prev, :].copy()
            output_table_prev["Years"] = year
            columns_to_keep = dimensions + [techno_share_new]
            output_table_prev = output_table_prev[columns_to_keep]
        
            # Recompute remaining_temp = techno-share form prev * agg veh fleet from y
            del output_table_y[techno_share_new]
            # print(output_table_prev)
            # print(output_table_y)
            output_table_tot = pd.merge(output_table_y, output_table_prev, on=dimensions, how="inner")
            # print(output_table_tot.shape)
            if i > 1:
                output_table_tot[veh_fleet_remaining] = output_table_tot[veh_fleet_remaining_agg] * output_table_tot[
                    techno_share_new]
            # Recompute technology-share :
            output_table_tot[tot_veh_fleet] = output_table_tot[veh_fleet_new] + output_table_tot[veh_fleet_remaining]
            # If tot_veh_fleet > 0 : technology-share = sum(new + remaining) / sub-cat
            mask_gt_0 = (output_table_tot[tot_veh_fleet] > 0.000000001) & (output_table_tot[tot_veh_fleet_agg] > 0)
            output_table_tot.loc[mask_gt_0, techno_share_new] = output_table_tot.loc[mask_gt_0, tot_veh_fleet] / \
                                                                output_table_tot.loc[mask_gt_0, tot_veh_fleet_agg]
            # Else (tot_veh_fleet > 0) : keep previous value
            # Else (tot_veh_fleet_agg = 0) => keep previous value
        
            # From output_table : remove year i and concat with this new values
            output_table = output_table.loc[~mask_y, :]
            output_table = pd.concat([output_table, output_table_tot], ignore_index=True)
        return output_table
    # For years > 2015 : technology-share[%][year] = tot-veh-fleet[number][year] by transport-user / veh-type / motor-type / energy-carrier / tot-veh-sub-cat[number][year] by transport-user / veh-type   isNan() => set previous values
    out_5315_1 = helper_5315(input_table=out_5319_1)
    # other columns  to remerge  after X Switch
    out_5315_1_2 = column_filter(df=out_5315_1, columns_to_drop=['final-veh-fleet-remaining[number]', 'final-veh-fleet-remaining-agg[number]', 'technology-share-fleet[%]', 'Timestep'])
    # OTS / FTS retrofit-switch [%]
    retrofit_switch_percent = import_data(trigram='tra', variable_name='retrofit-switch')
    # only  final-veh-remaining-fleet done because X-switch node bugs if too many  columns
    out_5315_1 = column_filter(df=out_5315_1, columns_to_drop=['final-new-veh-fleet[number]', 'total-veh-fleet[number]', 'total-veh-fleet-agg[number]', 'final-veh-fleet-remaining-agg[number]', 'technology-share-fleet[%]', 'Timestep', 'technology-share[%]'])
    # Retrofit Switch thermic to electric
    out_9471_1 = x_switch(demand_table=out_5315_1, switch_table=retrofit_switch_percent, correlation_table=ratio_2, col_energy='final-veh-fleet-remaining[number]', col_energy_carrier='motor-type', category_from_selected='thermic', category_to_selected='electric')
    # top : rest bottom : BEVnew
    out_9471_1_excluded = out_9471_1.loc[out_9471_1['motor-type'].isin(['BEVnew'])].copy()
    out_9471_1 = out_9471_1.loc[~out_9471_1['motor-type'].isin(['BEVnew'])].copy()
    # rename BEVnew to BEV
    out_9471_1_excluded['motor-type'] = "BEV"
    # rename [in energy-carrier] to electricity sent to IND
    out_9471_1_excluded['energy-carrier'] = "electricity"

    # WARNING
    # Retrofitted vehicles are currently equivalent to new vehicle demand. (hence the same name to be easily summed)
    # This is a limitation that increase the new vehicles to be produced by IND, it should be fixed by creating new product "LDV-retrofitted" and "HDV-retrofitted" with different subproducts demand (eg 0.2 LDV-BEV + 1 battery). It hasn't be done yet by lack of time.

    # final-veh-fleet-remaining[number] (only retrofit veh)
    final_veh_fleet_remaining_number = use_variable(input_table=out_9471_1_excluded, selected_variable='final-veh-fleet-remaining[number]')
    # rename column final-veh-fleet-remaining[number] to final-veh-fleet-ind[number]
    out_9478_1 = final_veh_fleet_remaining_number.rename(columns={'final-veh-fleet-remaining[number]': 'final-veh-fleet-ind[number]'})
    # Group by  vehicule-type, motor-type (sum)
    out_9478_1 = group_by_dimensions(df=out_9478_1, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'motor-type'], aggregation_method='Sum')
    out_1 = pd.concat([out_5410_1_2, out_9478_1.set_index(out_9478_1.index.astype(str) + '_dup')])
    # Group by  vehicule-type, motor-type (sum)
    out_1 = group_by_dimensions(df=out_1, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'motor-type'], aggregation_method='Sum')
    # transport-demand for pathway explorer
    out_9206_1 = pd.concat([out_1, transport_demand.set_index(transport_demand.index.astype(str) + '_dup')])
    out_9471_1 = pd.concat([out_9471_1, out_9471_1_excluded.set_index(out_9471_1_excluded.index.astype(str) + '_dup')])
    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    out_9471_1 = group_by_dimensions(df=out_9471_1, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')
    # Remerge
    out_9492_1 = joiner(df_left=out_5315_1_2, df_right=out_9471_1, joiner='left', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])
    # total-veh-fleet[number] (replace) = final-new-veh-fleet[number] + final-veh-fleet-remaining[number]
    total_veh_fleet_number = out_9492_1.assign(**{'total-veh-fleet[number]': out_9492_1['final-new-veh-fleet[number]']+out_9492_1['final-veh-fleet-remaining[number]']})
    # technology-share[%] (replace) = total-veh-fleet[number] / total-veh-fleet-agg[number]
    technology_share_percent = total_veh_fleet_number.assign(**{'technology-share[%]': total_veh_fleet_number['total-veh-fleet[number]']/total_veh_fleet_number['total-veh-fleet-agg[number]']})
    # Fill with 0
    technology_share_percent = missing_value(df=technology_share_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    def helper_5320(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        mask = (output_table["Years"] > int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years > baseyears
    out_5320_1 = helper_5320(input_table=technology_share_percent)
    # Conactenate to have all available mix
    out_1_2 = pd.concat([out_5320_1, out_5321_1.set_index(out_5321_1.index.astype(str) + '_dup')])

    # Technology-share[%]

    # technology-share [%]
    technology_share_percent_2 = export_variable(input_table=out_1_2, selected_variable='technology-share[%]')

    # For : ALL RESULTS (allows visualization outside the module node)

    # Select variables

    # technology-share [%]
    technology_share_percent_2 = use_variable(input_table=technology_share_percent_2, selected_variable='technology-share[%]')
    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    technology_share_percent_3 = group_by_dimensions(df=technology_share_percent_2, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')
    share_percent = pd.concat([modal_share_percent, technology_share_percent_3.set_index(technology_share_percent_3.index.astype(str) + '_dup')])


    # Keep all (for all results)
    share_percent = column_filter(df=share_percent, pattern='^.*$')
    # final-transport-demand[vkm] = veh-transport-demand[vkm] * technology-share[%]
    final_transport_demand_vkm = mcd(input_table_1=technology_share_percent_2, input_table_2=veh_transport_demand_vkm, operation_selection='x * y', output_name='final-transport-demand[vkm]', fill_value_bool='Inner Join')

    def helper_5272(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get values
        mask_PHEV_diesel = (output_table["motor-type"].str.contains("PHEV")) & (output_table["energy-carrier"] == "liquid-ff-diesel")
        # Table with diesel => change it into elec
        table_temp = output_table.loc[mask_PHEV_diesel, :]
        table_temp["energy-carrier"] = "electricity"
        # Concatenate with output_table
        output_table = pd.concat([output_table, table_temp], ignore_index = True)
        return output_table
    # Copy PHEV(CE)*diesel in new columns PHEV(CE)*elec
    out_5272_1 = helper_5272(input_table=final_transport_demand_vkm)
    # transport-demand[tkm] = transport-demand[tkm] * technology-share[%]
    final_transport_demand_tkm = mcd(input_table_1=technology_share_percent_2, input_table_2=transport_demand_tkm, operation_selection='x * y', output_name='final-transport-demand[tkm]')
    # final-transport-demand [tkm]
    final_transport_demand_tkm_2 = export_variable(input_table=final_transport_demand_tkm, selected_variable='final-transport-demand[tkm]')

    # HDV share in freight

    # final-transport-demand [tkm]
    final_transport_demand_tkm = use_variable(input_table=final_transport_demand_tkm_2, selected_variable='final-transport-demand[tkm]')
    # Group by year
    final_transport_demand_tkm_3 = group_by_dimensions(df=final_transport_demand_tkm, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Top: HDV
    final_transport_demand_tkm_4 = final_transport_demand_tkm.loc[final_transport_demand_tkm['vehicule-type'].isin(['HDV'])].copy()
    # Group by year
    final_transport_demand_tkm_4 = group_by_dimensions(df=final_transport_demand_tkm_4, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # modal-share-HDV[%tkm] = final-transport-demand (HDV) [tkm] / final-transport-demand (all) [tkm]
    modal_share_HDV_percent_tkm = mcd(input_table_1=final_transport_demand_tkm_4, input_table_2=final_transport_demand_tkm_3, operation_selection='x / y', output_name='modal-share-HDV[%tkm]')
    # modal-share-HDV[%tkm]
    modal_share_HDV_percent_tkm = export_variable(input_table=modal_share_HDV_percent_tkm, selected_variable='modal-share-HDV[%tkm]')
    # transport-demand-per-cap [tkm/cap] = transport_demand [tkm] / Capita [cap]
    transport_demand_per_cap_tkm_per_cap = mcd(input_table_1=final_transport_demand_tkm_3, input_table_2=population_cap, operation_selection='x / y', output_name='transport-demand-per-cap[tkm/cap]')
    # Convert Unit from tkm to km => x1
    final_transport_demand_km_2 = final_transport_demand_tkm.drop(columns='final-transport-demand[tkm]').assign(**{'final-transport-demand[km]': final_transport_demand_tkm['final-transport-demand[tkm]'] * 1.0})
    # transport-demand[pkm] = veh-transport-demand[pkm] * technology-share[%]
    final_transport_demand_pkm = mcd(input_table_1=technology_share_percent_2, input_table_2=transport_demand_pkm_3, operation_selection='x * y', output_name='final-transport-demand[pkm]')

    # Final transport demand

    # final-transport-demand [pkm]
    final_transport_demand_pkm = export_variable(input_table=final_transport_demand_pkm, selected_variable='final-transport-demand[pkm]')
    # Final transport demand (pkm)
    final_transport_demand_pkm_2 = use_variable(input_table=final_transport_demand_pkm, selected_variable='final-transport-demand[pkm]')
    # Group by  transport-user, vehicule-type, motor-type (sum)
    final_transport_demand_pkm_3 = group_by_dimensions(df=final_transport_demand_pkm_2, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # transport-demand-per-cap [pkm/cap] = transport_demand [pkm] / Capita [cap]
    transport_demand_per_cap_pkm_per_cap = mcd(input_table_1=population_cap, input_table_2=final_transport_demand_pkm_3, operation_selection='y / x', output_name='transport-demand-per-cap[pkm/cap]')
    # transport-demand-per-cap [pkm/cap]
    transport_demand_per_cap_pkm_per_cap = export_variable(input_table=transport_demand_per_cap_pkm_per_cap, selected_variable='transport-demand-per-cap[pkm/cap]')
    # Convert Unit from pkm to km => x1
    final_transport_demand_km = final_transport_demand_pkm_2.drop(columns='final-transport-demand[pkm]').assign(**{'final-transport-demand[km]': final_transport_demand_pkm_2['final-transport-demand[pkm]'] * 1.0})
    final_transport_demand_km = pd.concat([final_transport_demand_km_2, final_transport_demand_km.set_index(final_transport_demand_km.index.astype(str) + '_dup')])
    out_5134_1 = joiner(df_left=final_transport_demand_km, df_right=out_5133_1, joiner='inner', left_input=['vehicule-type'], right_input=['vehicule-type'])

    def helper_5178(input_table) -> pd.DataFrame:
        # Import pandas
        
        
        # Copy input to output
        output_table = input_table.copy()
        
        # Copy rails => to tram
        mask_rails = (output_table["material"] == "rails")
        trolley_table = output_table.loc[mask_rails, :]
        trolley_table["material"] = "trolley-cables"
        
        # Concat tables
        output_table = pd.concat([output_table, trolley_table], ignore_index=True)
        return output_table
    out_5178_1 = helper_5178(input_table=out_5134_1)
    # Group by  material (sum)
    out_5178_1 = group_by_dimensions(df=out_5178_1, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Divide by baseyear
    out_7159_1 = divide_year(df=out_5178_1, in_var='final-transport-demand[km]', output_name='increase-rate[%]', reference_year=Globals.get().base_year)
    # Set 0 if divide by 0
    out_7159_1 = missing_value(df=out_7159_1, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # increase-rate [%]
    increase_rate_percent = export_variable(input_table=out_7159_1, selected_variable='increase-rate[%]')
    # Keep rail only
    final_transport_demand_pkm_2 = final_transport_demand_pkm_2.loc[final_transport_demand_pkm_2['vehicule-type'].isin(['rail'])].copy()

    # Corr-factor-trolley-cables
    # = demand rails CEV / demandrails (CEV + FCEV + ICE-diesel)

    # Group by  Country, Rears (sum)
    final_transport_demand_pkm_3 = group_by_dimensions(df=final_transport_demand_pkm_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # final-transport-demand to total-rail-demand
    final_transport_demand_pkm_3 = column_rename_regex(df=final_transport_demand_pkm_3, search_string='(.*demand.*)', replace_string='total-rail-demand[pkm]')
    # Keep CEV only
    final_transport_demand_pkm_2 = final_transport_demand_pkm_2.loc[final_transport_demand_pkm_2['motor-type'].isin(['CEV'])].copy()
    # corr-factor-trolley[%] = final-transport-demand : CEV / total
    corr_factor_trolley_percent = mcd(input_table_1=final_transport_demand_pkm_2, input_table_2=final_transport_demand_pkm_3, operation_selection='x / y', output_name='corr-factor-trolley[%]')

    # Add trolley-cables category
    # = copy values from rails

    # Set 0 if divide by 0
    corr_factor_trolley_percent = missing_value(df=corr_factor_trolley_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # corr-factor-trolley [%]
    corr_factor_trolley_percent = export_variable(input_table=corr_factor_trolley_percent, selected_variable='corr-factor-trolley[%]')
    # Group by  Country, Years (mean)
    corr_factor_trolley_percent = group_by_dimensions(df=corr_factor_trolley_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    def helper_5954(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["material"] = "trolley-cables"
        return output_table
    # Add Column  material = trolley-cables
    out_5954_1 = helper_5954(input_table=corr_factor_trolley_percent)

    # OTS only

    # OTS (only) length [km] (of road/rail)
    length_km = import_data(trigram='tra', variable_name='length', variable_type='OTS (only)')
    # Same as last available year
    length_km = add_missing_years(df_data=length_km)

    def helper_5179(input_table) -> pd.DataFrame:
        # Import pandas
        
        
        # Copy input to output
        output_table = input_table.copy()
        
        # Copy rails => to tram
        mask_rails = (output_table["material"] == "rails")
        trolley_table = output_table.loc[mask_rails, :]
        trolley_table["material"] = "trolley-cables"
        
        # Concat tables
        output_table = pd.concat([output_table, trolley_table], ignore_index=True)
        return output_table
    out_5179_1 = helper_5179(input_table=length_km)
    # corr-length[km] = length[km] * mult-corr
    corr_length_km = mcd(input_table_1=out_5179_1, input_table_2=out_5173_1, operation_selection='x * y', output_name='corr-length[km]')
    # corr-length[km] = (replace) corr-length[km] x corr-factor-trolley[%]  If missing corr-factor-torlley set to 1
    corr_length_km = mcd(input_table_1=corr_length_km, input_table_2=out_5954_1, operation_selection='x * y', output_name='corr-length[km]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # corr-length [km]
    corr_length_km = export_variable(input_table=corr_length_km, selected_variable='corr-length[km]')
    # material-length-demand[km] = increase-rate[%] * corr-length[km]
    material_length_demand_km = mcd(input_table_1=increase_rate_percent, input_table_2=corr_length_km, operation_selection='x * y', output_name='material-length-demand[km]')

    # Material length demand

    # material-length-demand [km]
    material_length_demand_km = export_variable(input_table=material_length_demand_km, selected_variable='material-length-demand[km]')
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = import_data(trigram='tra', variable_name='fuel-switch')
    # Fuel Switch fffuels to biofuels
    out_9542_1 = x_switch(demand_table=veh_efficiency_km_avg_MJ_per_km, switch_table=fuel_switch_percent, correlation_table=ratio, col_energy='veh-efficiency-km-avg[MJ/km]')
    # Fuel Switch fffuels to efuels
    out_9543_1 = x_switch(demand_table=out_9542_1, switch_table=fuel_switch_percent, correlation_table=ratio, col_energy='veh-efficiency-km-avg[MJ/km]', category_to_selected='synfuels')
    # Convert Unit from MJ/km to TWh/km
    veh_efficiency_km_avg_TWh_per_km = out_9543_1.drop(columns='veh-efficiency-km-avg[MJ/km]').assign(**{'veh-efficiency-km-avg[TWh/km]': out_9543_1['veh-efficiency-km-avg[MJ/km]'] * 2.77777777777778e-10})
    # CP tra-combustion-emission-factor [Mt/TWh]
    tra_combustion_emission_factor_Mt_per_TWh = import_data(trigram='tra', variable_name='tra-combustion-emission-factor', variable_type='RCP')
    # emissions-veh[Mt/km][Mt/km] = veh-efficiency-km-avg[TWh/km] * tra-combustion-emission-factor [Mt/TWh]
    emissions_veh_Mt_per_km = mcd(input_table_1=veh_efficiency_km_avg_TWh_per_km, input_table_2=tra_combustion_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions-veh[Mt/km]')
    # Convert Unit from Mt/km to g/km
    emissions_veh_g_per_km = emissions_veh_Mt_per_km.drop(columns='emissions-veh[Mt/km]').assign(**{'emissions-veh[g/km]': emissions_veh_Mt_per_km['emissions-veh[Mt/km]'] * 1000000000000.0})
    # Group by year
    emissions_veh_g_per_km = group_by_dimensions(df=emissions_veh_g_per_km, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'fleet-age', 'gaes'], aggregation_method='Sum')
    # Top: only new
    emissions_veh_g_per_km = emissions_veh_g_per_km.loc[emissions_veh_g_per_km['fleet-age'].isin(['new'])].copy()
    # Top: passenger
    emissions_veh_g_per_km_2 = emissions_veh_g_per_km.loc[emissions_veh_g_per_km['transport-user'].isin(['passenger'])].copy()
    emissions_veh_g_per_km_excluded = emissions_veh_g_per_km.loc[~emissions_veh_g_per_km['transport-user'].isin(['passenger'])].copy()
    # Top: HDV
    emissions_veh_g_per_km_excluded = emissions_veh_g_per_km_excluded.loc[emissions_veh_g_per_km_excluded['vehicule-type'].isin(['HDV'])].copy()
    # Top: LDV & bus & 2W
    emissions_veh_g_per_km = emissions_veh_g_per_km_2.loc[emissions_veh_g_per_km_2['vehicule-type'].isin(['LDV', 'bus', '2W'])].copy()
    # Freight and Passenger
    emissions_veh_g_per_km = pd.concat([emissions_veh_g_per_km, emissions_veh_g_per_km_excluded.set_index(emissions_veh_g_per_km_excluded.index.astype(str) + '_dup')])
    # Top: gas=CO2
    emissions_veh_g_per_km = emissions_veh_g_per_km.loc[emissions_veh_g_per_km['gaes'].isin(['CO2'])].copy()
    # emissions-veh[g/km]
    emissions_veh_g_per_km = export_variable(input_table=emissions_veh_g_per_km, selected_variable='emissions-veh[g/km]')

    # For : industry

    # Select variables

    # material-lenght-demand[km]
    material_length_demand_km = use_variable(input_table=material_length_demand_km, selected_variable='material-length-demand[km]')
    out_7164_1 = pd.concat([out_1, material_length_demand_km.set_index(material_length_demand_km.index.astype(str) + '_dup')])
    # All for : Industry
    out_7164_1 = column_filter(df=out_7164_1, pattern='^.*$')

    # For : Minerals

    # Pivot

    out_5401_1, _, _ = pivoting(df=material_length_demand_km, agg_dict={'material-length-demand[km]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Years', 'Country'], list_pivots=['material'])
    out_5402_1 = missing_value_column_filter(df=out_5401_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as used in Industry
    out_5402_1 = column_rename_regex(df=out_5402_1, search_string='(.*)\\+.*(\\[.*)', replace_string='tra_length_$1$2')
    # remove s from rails
    out_5402_1 = column_rename_regex(df=out_5402_1, search_string='rails', replace_string='rail')
    # remove s from roads
    out_5402_1 = column_rename_regex(df=out_5402_1, search_string='roads', replace_string='road')

    # Rename

    # Keep all
    out_5402_1 = column_filter(df=out_5402_1, pattern='^.*$')
    # Remove length
    out_5402_1 = column_rename_regex(df=out_5402_1, search_string='length_', replace_string='')
    # Join data for minerals
    out_5466_1 = joiner(df_left=out_5402_1, df_right=out_6021_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # All for : Minerals
    out_5466_1 = column_filter(df=out_5466_1, pattern='^.*$')
    # Years
    out_6036_1 = out_5466_1.assign(Years=out_5466_1['Years'].astype(str))
    # RCP costs-by-transport-infra [MEUR/km] From TECH
    costs_by_transport_infra_MEUR_per_km = import_data(trigram='tec', variable_name='costs-by-transport-infra', variable_type='RCP')
    # RCP price-indices [-] From TECH
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')
    # OTS/FTS wacc [%] From TECH
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = tra
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['tra'])].copy()
    # Group by all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Compute capex for transport infrastructure [km] 
    out_9430_1 = compute_costs(df_activity=material_length_demand_km, df_unit_costs=costs_by_transport_infra_MEUR_per_km, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='material-length-demand[km]')
    # RCP transport-infra-cost-user [-]
    transport_infra_cost_user_ = import_data(trigram='tra', variable_name='transport-infra-cost-user', variable_type='RCP')
    # capex[MEUR] = capex[MEUR] * vehicle-cost-user[-]
    capex_MEUR_2 = mcd(input_table_1=out_9430_1, input_table_2=transport_infra_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by  Country, Years, material (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR_2, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # capex-by-infrastructure [MEUR]
    out_9433_1 = capex_MEUR.rename(columns={'capex[MEUR]': 'capex-by-infrastructure[MEUR]'})
    # Compute capex for final-veh-fleet-in[number] (NEW FLEET =>CAPEX)
    out_9401_1 = compute_costs(df_activity=final_veh_fleet_ind_number_2, df_unit_costs=costs_by_vehicle_MEUR_per_number, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='final-veh-fleet-ind[number]')
    # RCP vehicle-cost-user [-]
    vehicle_cost_user_ = import_data(trigram='tra', variable_name='vehicle-cost-user', variable_type='RCP')
    # capex[MEUR] = capex[MEUR] * vehicle-cost-user[-]
    capex_MEUR = mcd(input_table_1=out_9401_1, input_table_2=vehicle_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    capex_MEUR_2 = pd.concat([capex_MEUR, capex_MEUR_2.set_index(capex_MEUR_2.index.astype(str) + '_dup')])

    # Cost by user

    # Group by  Country, Years, cost-user (sum)
    capex_MEUR_2 = group_by_dimensions(df=capex_MEUR_2, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    # Exclude vehicle-type = LDV
    capex_MEUR_excluded = capex_MEUR.loc[capex_MEUR['vehicule-type'].isin(['LDV'])].copy()
    capex_MEUR = capex_MEUR.loc[~capex_MEUR['vehicule-type'].isin(['LDV'])].copy()
    # Group by  Country, Years, vehicle-type, energy-carrier, motor-type (sum)
    capex_MEUR_excluded = group_by_dimensions(df=capex_MEUR_excluded, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier', 'motor-type'], aggregation_method='Sum')
    # capex-by-vehicle-motor [MEUR]
    out_9410_1 = capex_MEUR_excluded.rename(columns={'capex[MEUR]': 'capex-by-vehicle-motor[MEUR]'})
    # Group by  Country, Years, vehicle-type (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # capex-by-vehicle [MEUR]
    out_9409_1 = capex_MEUR.rename(columns={'capex[MEUR]': 'capex-by-vehicle[MEUR]'})
    out_1 = pd.concat([out_9409_1, out_9410_1.set_index(out_9410_1.index.astype(str) + '_dup')])

    # Values for Years > baseyear

    # Use of variables

    # final-veh-fleet-remaining [number]
    final_veh_fleet_remaining_number = export_variable(input_table=technology_share_percent, selected_variable='final-veh-fleet-remaining[number]')

    def helper_5562(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_PHEV = (output_table["motor-type"] == "PHEV") | (output_table["motor-type"] == "PHEVCE")
        
        output_table_elec = output_table.loc[mask_PHEV,:]
        output_table_elec["energy-carrier"] = "electricity"
        
        output_table = pd.concat([output_table, output_table_elec], ignore_index = True)
        return output_table
    # Copy/Paste values for motor-type = PHEV(CE) from diesel to elec
    out_5562_1 = helper_5562(input_table=final_veh_fleet_remaining_number)

    def helper_5575(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) > int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years > baseyears
    out_5575_1 = helper_5575(input_table=out_5562_1)
    out_1_2 = pd.concat([out_5575_1, out_5583_1.set_index(out_5583_1.index.astype(str) + '_dup')])

    # Vehicule-fleet 
    # [number]

    # final-veh-fleet-remaining [number]
    final_veh_fleet_remaining_number = export_variable(input_table=out_1_2, selected_variable='final-veh-fleet-remaining[number]')
    # total-veh-fleet [number]
    total_veh_fleet_number = export_variable(input_table=technology_share_percent, selected_variable='total-veh-fleet[number]')

    def helper_5375(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_PHEV = (output_table["motor-type"] == "PHEV") | (output_table["motor-type"] == "PHEVCE")
        
        output_table_elec = output_table.loc[mask_PHEV,:]
        output_table_elec["energy-carrier"] = "electricity"
        
        output_table = pd.concat([output_table, output_table_elec], ignore_index = True)
        return output_table
    # Copy/Paste values for motor-type = PHEV(CE) from diesel to elec
    out_5375_1 = helper_5375(input_table=total_veh_fleet_number)

    def helper_5560(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) > int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years > baseyears
    out_5560_1 = helper_5560(input_table=out_5375_1)
    # final-new-veh-fleet [number]
    final_new_veh_fleet_number = export_variable(input_table=technology_share_percent, selected_variable='final-new-veh-fleet[number]')

    def helper_5561(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask_PHEV = (output_table["motor-type"] == "PHEV") | (output_table["motor-type"] == "PHEVCE")
        
        output_table_elec = output_table.loc[mask_PHEV,:]
        output_table_elec["energy-carrier"] = "electricity"
        
        output_table = pd.concat([output_table, output_table_elec], ignore_index = True)
        return output_table
    # Copy/Paste values for motor-type = PHEV(CE) from diesel to elec
    out_5561_1 = helper_5561(input_table=final_new_veh_fleet_number)

    def helper_5574(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) > int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years > baseyears
    out_5574_1 = helper_5574(input_table=out_5561_1)

    def helper_5568(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        mask = (output_table["Years"].astype(int) <= int(Globals.get().base_year))
        output_table = output_table.loc[mask, :]
        return output_table
    # Keep years <= baseyears
    out_5568_1 = helper_5568(input_table=out_5561_1)
    # Set all to 0
    final_new_veh_fleet_number = out_5568_1.assign(**{'final-new-veh-fleet[number]': 0.0})
    # total-veh-fleet[number] = final-new-veh-fleet[number] + final-veh-fleet-remaining[number]
    total_veh_fleet_number = mcd(input_table_1=final_new_veh_fleet_number, input_table_2=out_5583_1, operation_selection='x + y', output_name='total-veh-fleet[number]')
    out_5576_1 = pd.concat([out_5560_1, total_veh_fleet_number.set_index(total_veh_fleet_number.index.astype(str) + '_dup')])
    # total-veh-fleet [number]
    total_veh_fleet_number = export_variable(input_table=out_5576_1, selected_variable='total-veh-fleet[number]')

    # Number of passenger LDV

    # total-veh-fleet [number]
    total_veh_fleet_number_2 = use_variable(input_table=total_veh_fleet_number, selected_variable='total-veh-fleet[number]')
    # Top: Passenger
    total_veh_fleet_number_3 = total_veh_fleet_number_2.loc[total_veh_fleet_number_2['transport-user'].isin(['passenger'])].copy()
    # Top: LDV
    total_veh_fleet_number_3 = total_veh_fleet_number_3.loc[total_veh_fleet_number_3['vehicule-type'].isin(['LDV'])].copy()
    # Split motor-type
    total_veh_fleet_number_excluded = total_veh_fleet_number_3.loc[total_veh_fleet_number_3['motor-type'].isin(['PHEV', 'PHEVCE'])].copy()
    total_veh_fleet_number_3 = total_veh_fleet_number_3.loc[~total_veh_fleet_number_3['motor-type'].isin(['PHEV', 'PHEVCE'])].copy()
    total_veh_fleet_number_excluded = total_veh_fleet_number_excluded.loc[~total_veh_fleet_number_excluded['energy-carrier'].isin(['electricity'])].copy()
    total_veh_fleet_number_3 = pd.concat([total_veh_fleet_number_3, total_veh_fleet_number_excluded.set_index(total_veh_fleet_number_excluded.index.astype(str) + '_dup')])
    # Group by  year
    total_veh_fleet_number_3 = group_by_dimensions(df=total_veh_fleet_number_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # total-veh-fleet[number] to  total-veh-fleet-passenger-LDV[number]
    total_veh_fleet_number_3 = column_rename_regex(df=total_veh_fleet_number_3, search_string='total-veh-fleet', replace_string='total-veh-fleet-passenger-LDV')
    # total-veh-fleet-passenger-LDV[number]
    total_veh_fleet_passenger_LDV_number = export_variable(input_table=total_veh_fleet_number_3, selected_variable='total-veh-fleet-passenger-LDV[number]')

    # For : Pathway Explorer

    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    total_veh_fleet_number_3 = group_by_dimensions(df=total_veh_fleet_number_2, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')
    # Split motor-type
    total_veh_fleet_number_excluded = total_veh_fleet_number_3.loc[total_veh_fleet_number_3['motor-type'].isin(['PHEV', 'PHEVCE'])].copy()
    total_veh_fleet_number_3 = total_veh_fleet_number_3.loc[~total_veh_fleet_number_3['motor-type'].isin(['PHEV', 'PHEVCE'])].copy()
    total_veh_fleet_number_excluded = total_veh_fleet_number_excluded.loc[~total_veh_fleet_number_excluded['energy-carrier'].isin(['electricity'])].copy()
    total_veh_fleet_number_3 = pd.concat([total_veh_fleet_number_3, total_veh_fleet_number_excluded.set_index(total_veh_fleet_number_excluded.index.astype(str) + '_dup')])
    # Concat outputs for Pathway  explorer
    out_9207_1 = pd.concat([out_9206_1, total_veh_fleet_number_3.set_index(total_veh_fleet_number_3.index.astype(str) + '_dup')])
    # Keep  bus (opex given in number of vehicle)
    total_veh_fleet_number_2 = total_veh_fleet_number_2.loc[total_veh_fleet_number_2['vehicule-type'].isin(['bus'])].copy()
    # Compute opex for final-fleet-demand[number] (Total vehicle (number) => OPEX)
    out_9422_1 = compute_costs(df_activity=total_veh_fleet_number_2, df_unit_costs=costs_by_vehicle_MEUR_per_number, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='total-veh-fleet[number]', cost_type='OPEX')
    # opex[MEUR] = opex[MEUR] * vehicle-cost-user[-]
    opex_MEUR = mcd(input_table_1=out_9422_1, input_table_2=vehicle_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    out_5577_1 = pd.concat([out_5574_1, final_new_veh_fleet_number.set_index(final_new_veh_fleet_number.index.astype(str) + '_dup')])
    # final-new-veh-fleet [number]
    final_new_veh_fleet_number = export_variable(input_table=out_5577_1, selected_variable='final-new-veh-fleet[number]')
    out_5330_1 = joiner(df_left=total_veh_fleet_number, df_right=final_new_veh_fleet_number, joiner='inner', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])
    out_5331_1 = joiner(df_left=out_5330_1, df_right=final_veh_fleet_remaining_number, joiner='inner', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])
    # Left Join
    out_5367_1 = joiner(df_left=out_5331_1, df_right=out_5365_1, joiner='left', left_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], right_input=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'])

    def helper_5350(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        output_table["Years"] = output_table["Years"].astype(int)
        
        # Years
        baseyear = int(Globals.get().base_year)
        
        # Columns names
        ## Efficiency : actual fleet + new fleet
        fleet_veh_eff_tkm = 'fleet-veh-efficiency-tkm[MJ/tkm]'
        fleet_veh_eff_pkm = 'fleet-veh-efficiency-pkm[MJ/pkm]'
        fleet_veh_eff_km = 'fleet-veh-efficiency-km[MJ/km]'
        new_veh_eff_tkm = 'new-veh-efficiency-tkm[MJ/tkm]'
        new_veh_eff_pkm = 'new-veh-efficiency-pkm[MJ/pkm]'
        new_veh_eff_km = 'new-veh-efficiency-km[MJ/km]'
        veh_eff_tkm = 'veh-efficiency-tkm[MJ/tkm]'
        veh_eff_pkm = 'veh-efficiency-pkm[MJ/pkm]'
        veh_eff_km = 'veh-efficiency-km[MJ/km]'
        ## Fleet : remaining, new and total
        remaining_fleet = 'final-veh-fleet-remaining[number]'
        new_fleet = 'final-new-veh-fleet[number]'
        total_fleet = 'total-veh-fleet[number]'
        
        # Get dimension list
        dimensions = []
        for name in output_table.select_dtypes(include=['int', 'object']):  # object => refers to string
            dimensions.append(name)
        
        # Get possible years
        year_values = []
        for year in output_table["Years"].values:
            if year not in year_values and year >= baseyear:
                year_values.append(year)
        year_values.sort()
        
        # For year in baseyear => endyear
        for i in range(1, len(year_values)):
            # Mask for years
            year = int(year_values[i])
            year_prev = int(year_values[i - 1])
            mask_y = (output_table["Years"] == year)
            mask_prev = (output_table["Years"] == year_prev)
            # Get veh efficiency from previous years => keep only dimensions (except years) + fleet_veh_eff columns
            output_table_prev = output_table.loc[mask_prev, :]
            output_table_prev = output_table_prev[dimensions + [fleet_veh_eff_tkm, fleet_veh_eff_pkm, fleet_veh_eff_km]]
            output_table_prev["Years"] = int(year)
            # Get all others values from actual years
            output_table_y = output_table.loc[mask_y, :]
            del output_table_y[fleet_veh_eff_tkm]
            del output_table_y[fleet_veh_eff_pkm]
            del output_table_y[fleet_veh_eff_km]
            # Join both table => calculate new value for fleet_veh_eff)
            output_table_tot = pd.merge(output_table_y, output_table_prev, on=dimensions, how="inner")
            output_table_tot[fleet_veh_eff_tkm + "temp"] = (output_table_tot[fleet_veh_eff_tkm] * output_table_tot[
                remaining_fleet] + output_table_tot[new_veh_eff_tkm] * output_table_tot[new_fleet]) / output_table_tot[
                                                               total_fleet]
            output_table_tot[fleet_veh_eff_pkm + "temp"] = (output_table_tot[fleet_veh_eff_pkm] * output_table_tot[
                remaining_fleet] + output_table_tot[new_veh_eff_pkm] * output_table_tot[new_fleet]) / output_table_tot[
                                                               total_fleet]
            output_table_tot[fleet_veh_eff_km + "temp"] = (output_table_tot[fleet_veh_eff_km] * output_table_tot[
                remaining_fleet] + output_table_tot[new_veh_eff_km] * output_table_tot[new_fleet]) / output_table_tot[
                                                              total_fleet]
            # If value = NaN (when divided by 0) => fleet_veh_eff = previous values
            for eff_type in [fleet_veh_eff_tkm, fleet_veh_eff_pkm, fleet_veh_eff_km]:
                mask_nan = (output_table_tot[eff_type + "temp"].isna())
                output_table_tot.loc[mask_nan, eff_type + "temp"] = output_table_tot.loc[mask_nan, eff_type]
                del output_table_tot[eff_type]
                output_table_tot = output_table_tot.rename(columns={eff_type + "temp": eff_type}, inplace=False)
            # From output_table : remove year i and concat with this new values
            output_table = pd.concat([output_table, output_table_tot], ignore_index=True)
            
        
        # Rename columns => fleet-veh-efficiency = veh-efficiency
        output_table = output_table.rename(
            columns={fleet_veh_eff_tkm: veh_eff_tkm, fleet_veh_eff_pkm: veh_eff_pkm, fleet_veh_eff_km: veh_eff_km},
            inplace=False)
        return output_table
    # fleet veh efficiency = (last fleet veh eff * remaining fleet + new veh eff * new fleet) / (remaining + new fleet)
    out_5350_1 = helper_5350(input_table=out_5367_1)

    # veh-efficiency

    # veh-efficiency-pkm [MJ/pkm]
    veh_efficiency_pkm_MJ_per_pkm = export_variable(input_table=out_5350_1, selected_variable='veh-efficiency-pkm[MJ/pkm]')

    def helper_5369(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # mask_nan
        mask_nan = (output_table['veh-efficiency-pkm[MJ/pkm]'].isna())
        output_table = output_table.loc[~mask_nan, :]
        return output_table
    # remove  NaN values
    out_5369_1 = helper_5369(input_table=veh_efficiency_pkm_MJ_per_pkm)

    # Energy demand

    # Select variables

    # veh-efficiency [MJ/pkm]
    veh_efficiency_pkm_MJ_per_pkm = use_variable(input_table=out_5369_1, selected_variable='veh-efficiency-pkm[MJ/pkm]')
    # energy-demand[MJ] = final-transport-demand[pkm] * vehicle-efficiency[MJ/pkm]
    energy_demand_MJ = mcd(input_table_1=final_transport_demand_pkm, input_table_2=veh_efficiency_pkm_MJ_per_pkm, operation_selection='x * y', output_name='energy-demand[MJ]')
    # veh-efficiency-tkm [MJ/tkm]
    veh_efficiency_tkm_MJ_per_tkm = export_variable(input_table=out_5350_1, selected_variable='veh-efficiency-tkm[MJ/tkm]')

    def helper_5368(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # mask_nan
        mask_nan = (output_table['veh-efficiency-tkm[MJ/tkm]'].isna())
        output_table = output_table.loc[~mask_nan, :]
        return output_table
    # remove  NaN values
    out_5368_1 = helper_5368(input_table=veh_efficiency_tkm_MJ_per_tkm)
    # veh-efficiency [MJ/tkm]
    veh_efficiency_tkm_MJ_per_tkm = use_variable(input_table=out_5368_1, selected_variable='veh-efficiency-tkm[MJ/tkm]')
    # energy-demand[MJ] = final-transport-demand[tkm] * vehicle-efficiency[MJ/tkm]
    energy_demand_MJ_3 = mcd(input_table_1=final_transport_demand_tkm_2, input_table_2=veh_efficiency_tkm_MJ_per_tkm, operation_selection='x * y', output_name='energy-demand[MJ]')
    # veh-efficiency-km [MJ/km]
    veh_efficiency_km_MJ_per_km = export_variable(input_table=out_5350_1, selected_variable='veh-efficiency-km[MJ/km]')

    def helper_5370(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # mask_nan
        mask_nan = (output_table['veh-efficiency-km[MJ/km]'].isna())
        output_table = output_table.loc[~mask_nan, :]
        return output_table
    # remove  NaN values
    out_5370_1 = helper_5370(input_table=veh_efficiency_km_MJ_per_km)

    # For km veh-efficiency => mormalize values
    # 
    # Concerns : PASSENGER (LDV/bus) / FREIGHT (LDV/HDV) - PHEV
    # From diesel
    # efficiency-normalized = efficiency / max(all efficiency)
    # From elec
    # efficiency = max(all efficiency elec) * efficiency-normalized


    def helper_5274(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        veh_efficiency_col = "veh-efficiency-km[MJ/km]"
        
        # Mask
        # Get output_table for diesel
        mask_diesel = (output_table["energy-carrier"] == "liquid-ff-diesel") & (output_table["motor-type"].str.contains("PHEV"))
        output_table_diesel = output_table.copy()
        output_table_diesel = output_table_diesel.loc[mask_diesel, :]
        # Get output_table for diesel
        mask_elec = (output_table["energy-carrier"] == "electricity") & (output_table["motor-type"].str.contains("PHEV"))
        output_table_elec = output_table.copy()
        output_table_elec = output_table_elec.loc[mask_elec, :]
        # Get output_table for not diesel / elec
        mask_others = ((output_table["energy-carrier"] == "liquid-ff-diesel") | (output_table["energy-carrier"] == "electricity")) & (output_table["motor-type"].str.contains("PHEV"))
        output_table_others = output_table.copy()
        output_table_others = output_table_others.loc[~mask_others, :]
        
        
        # Dimensions except Years and energy-carrier
        dims = output_table.select_dtypes(include = "object") # object = refers to string
        dimensions = []
        for col in dims.head():
            if col != "Years":
                dimensions.append(col)
        
        
        # Get max value by group dimensions
        ## Diesel
        max_values_diesel = output_table_diesel.copy()
        max_values_diesel = max_values_diesel.groupby(dimensions, as_index=False).max()
        del max_values_diesel["Years"]
        ## Elec
        max_values_elec = output_table_elec.copy()
        max_values_elec = max_values_elec.groupby(dimensions, as_index=False).max()
        del max_values_elec["Years"]
        
        # Get only diesel => normalized = col / col_max
        ## a. Get max value
        max_values_diesel = max_values_diesel.rename(columns = {veh_efficiency_col: "max_value_diesel"})
        # b. Apply max_values 
        ratio_diesel = pd.merge(left=output_table_diesel, right=max_values_diesel, how='inner', on=dimensions)
        ratio_diesel["ratio_diesel"] = ratio_diesel[veh_efficiency_col] / ratio_diesel["max_value_diesel"]
        ratio_diesel = ratio_diesel.fillna(0)  # If division by 0 leading to na value, it's because efficiency is already 0. Setting back 0.
        del ratio_diesel[veh_efficiency_col]
        del ratio_diesel["max_value_diesel"]
        del ratio_diesel["energy-carrier"] # Delete energy-carrier as we want to associate diesel to elec
        
        # Get only elec
        ## Dimension reduced = all dimensions except energy-carrier (and years)
        dimensions_reduced = dimensions.copy()
        dimensions_reduced.remove("energy-carrier")
        ## Get max value
        max_values_elec = max_values_elec.rename(columns = {veh_efficiency_col: "max_value_elec"})
        ## Apply ratio from diesel to elec to max value from elec
        ratio_elec = pd.merge(left=max_values_elec, right=ratio_diesel, how='inner', on=dimensions_reduced)
        ratio_elec["new_elec_value"] = ratio_elec["max_value_elec"] * ratio_elec["ratio_diesel"]
        # Change initial efficiency values:
        dimensions.append("Years")
        output_table_elec = pd.merge(left=output_table_elec, right=ratio_elec, how='inner', on=dimensions)
        del output_table_elec[veh_efficiency_col]
        output_table_elec = output_table_elec.rename(columns = {"new_elec_value": veh_efficiency_col})
        
        # Join max_values (diesel / elec) with output_table => if Nan : set to 1
        
        output_table = pd.concat([output_table_others, output_table_elec, output_table_diesel], ignore_index = True)
        return output_table
    out_5274_1 = helper_5274(input_table=out_5370_1)
    # veh-efficiency [MJ/km]
    veh_efficiency_km_MJ_per_km = use_variable(input_table=out_5274_1, selected_variable='veh-efficiency-km[MJ/km]')
    # Pins (all selected)
    veh_transport_demand_vkm = column_filter(df=veh_transport_demand_vkm_2, pattern='^.*$')
    # veh-transport-demand [vkm] from tkm
    veh_transport_demand_vkm = use_variable(input_table=veh_transport_demand_vkm, selected_variable='veh-transport-demand[vkm]')
    # transport-demand[vkm] = veh-transport-demand[vkm] * technology-share[%]
    final_transport_demand_vkm = mcd(input_table_1=technology_share_percent_2, input_table_2=veh_transport_demand_vkm, operation_selection='x * y', output_name='final-transport-demand[vkm]')

    def helper_5273(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Get values
        mask_PHEV_diesel = (output_table["motor-type"].str.contains("PHEV")) & (output_table["energy-carrier"] == "liquid-ff-diesel")
        # Table with diesel => change it into elec
        table_temp = output_table.loc[mask_PHEV_diesel, :]
        table_temp["energy-carrier"] = "electricity"
        # Concatenate with output_table
        output_table = pd.concat([output_table, table_temp], ignore_index = True)
        return output_table
    # Copy PHEV(CE)*diesel in new columns PHEV(CE)*elec
    out_5273_1 = helper_5273(input_table=final_transport_demand_vkm)
    out_1_2 = pd.concat([out_5272_1, out_5273_1.set_index(out_5273_1.index.astype(str) + '_dup')])
    # final-transport-demand [vkm]
    final_transport_demand_vkm_2 = export_variable(input_table=out_1_2, selected_variable='final-transport-demand[vkm]')
    # final-transport-demand[vkm] 
    final_transport_demand_vkm = use_variable(input_table=final_transport_demand_vkm_2, selected_variable='final-transport-demand[vkm]')
    # Include LDV, HDV, and bus
    final_transport_demand_vkm_3 = final_transport_demand_vkm.loc[final_transport_demand_vkm['vehicule-type'].isin(['LDV', 'bus', 'HDV'])].copy()
    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    final_transport_demand_vkm_3 = group_by_dimensions(df=final_transport_demand_vkm_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # final-transport-demand[vkm] to  final-transport-demand-total[vkm]
    final_transport_demand_vkm_3 = column_rename_regex(df=final_transport_demand_vkm_3, search_string='final-transport-demand', replace_string='final-transport-demand-total')
    # final-transport-demand-total[vkm]
    final_transport_demand_total_vkm = export_variable(input_table=final_transport_demand_vkm_3, selected_variable='final-transport-demand-total[vkm]')
    # KPI
    transport_demand = pd.concat([transport_demand_per_cap_pkm_per_cap, final_transport_demand_total_vkm.set_index(final_transport_demand_total_vkm.index.astype(str) + '_dup')])
    # KPI
    transport_demand = pd.concat([transport_demand_per_cap_tkm_per_cap, transport_demand.set_index(transport_demand.index.astype(str) + '_dup')])
    # Top: HDV
    final_transport_demand_vkm_4 = final_transport_demand_vkm.loc[final_transport_demand_vkm['vehicule-type'].isin(['HDV'])].copy()

    # Electricity share as energy-carrier in HDV freight

    # Group by year
    final_transport_demand_vkm_3 = group_by_dimensions(df=final_transport_demand_vkm_4, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # Top: energy-carrier=elec
    final_transport_demand_vkm_4 = final_transport_demand_vkm_4.loc[final_transport_demand_vkm_4['energy-carrier'].isin(['electricity'])].copy()
    # Group by year
    final_transport_demand_vkm_4 = group_by_dimensions(df=final_transport_demand_vkm_4, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # elec-share[%vkm] = final-transport-demand  (HDV, energy-carrier=elec) [vkm] / final-transport-demand  (HDV, energy-carrier=all) [vkm]
    elec_share_percent_vkm = mcd(input_table_1=final_transport_demand_vkm_4, input_table_2=final_transport_demand_vkm_3, operation_selection='x / y', output_name='elec-share[%vkm]')
    # elec-share[%tkm]
    elec_share_percent_vkm = export_variable(input_table=elec_share_percent_vkm, selected_variable='elec-share[%vkm]')
    # Top: LDV
    final_transport_demand_vkm_4 = final_transport_demand_vkm.loc[final_transport_demand_vkm['vehicule-type'].isin(['LDV'])].copy()

    # Electricity share as enrgy-carrier in LDV

    # Group by year
    final_transport_demand_vkm_3 = group_by_dimensions(df=final_transport_demand_vkm_4, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # Top: energy-carrier=elec
    final_transport_demand_vkm_4 = final_transport_demand_vkm_4.loc[final_transport_demand_vkm_4['energy-carrier'].isin(['electricity'])].copy()
    # Group by year
    final_transport_demand_vkm_4 = group_by_dimensions(df=final_transport_demand_vkm_4, groupby_dimensions=['Country', 'Years', 'vehicule-type'], aggregation_method='Sum')
    # elec-share[%vkm] = final-transport-demand  (LDV, energy-carrier=elec) [vkm] / final-transport-demand  (LDV, energy-carrier=all) [vkm]
    elec_share_percent_vkm_2 = mcd(input_table_1=final_transport_demand_vkm_4, input_table_2=final_transport_demand_vkm_3, operation_selection='x / y', output_name='elec-share[%vkm]')
    # elec-share[%vkm]
    elec_share_percent_vkm_2 = export_variable(input_table=elec_share_percent_vkm_2, selected_variable='elec-share[%vkm]')
    # energy-demand[MJ] = final-transport-demand[vkm] * vehicle-efficiency[MJ/km]
    energy_demand_MJ_2 = mcd(input_table_1=final_transport_demand_vkm_2, input_table_2=veh_efficiency_km_MJ_per_km, operation_selection='x * y', output_name='energy-demand[MJ]')
    energy_demand_MJ_2 = pd.concat([energy_demand_MJ_2, energy_demand_MJ_3.set_index(energy_demand_MJ_3.index.astype(str) + '_dup')])
    energy_demand_MJ = pd.concat([energy_demand_MJ_2, energy_demand_MJ.set_index(energy_demand_MJ.index.astype(str) + '_dup')])
    # Convert Unit from MJ to TWh
    energy_demand_TWh = energy_demand_MJ.drop(columns='energy-demand[MJ]').assign(**{'energy-demand[TWh]': energy_demand_MJ['energy-demand[MJ]'] * 2.77777777777778e-10})
    # Group by  transport-user, vehicule-type, motor-type, energy-carrier (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'transport-user', 'vehicule-type', 'motor-type', 'energy-carrier'], aggregation_method='Sum')

    # Energy-demand
    # before fuel-mix and fuel-switch

    # energy-demand [TWh] Before calibration and fuel mix / switch
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # Fuel Mix

    # OTS only

    # OTS (only) fuel-mix [%]
    fuel_mix_percent = import_data(trigram='tra', variable_name='fuel-mix', variable_type='OTS (only)')
    # Same as last available year
    fuel_mix_percent = add_missing_years(df_data=fuel_mix_percent)
    final_transport_demand_vkm_2 = final_transport_demand_vkm.loc[~final_transport_demand_vkm['vehicule-type'].isin(['bus'])].copy()
    # Compute opex for final-transport-demand[vkm] (Total demand (based on km) => OPEX)
    out_9418_1 = compute_costs(df_activity=final_transport_demand_vkm_2, df_unit_costs=costs_by_vehicle_MEUR_per_number, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, cost_type='OPEX')
    # opex[MEUR] = opex[MEUR] * vehicle-cost-user[-]
    opex_MEUR_2 = mcd(input_table_1=out_9418_1, input_table_2=vehicle_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    opex_MEUR_3 = pd.concat([opex_MEUR, opex_MEUR_2.set_index(opex_MEUR_2.index.astype(str) + '_dup')])
    # Group by  Country, Years, cost-user (sum)
    opex_MEUR_3 = group_by_dimensions(df=opex_MEUR_3, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    MEUR = pd.concat([capex_MEUR_2, opex_MEUR_3.set_index(opex_MEUR_3.index.astype(str) + '_dup')])
    opex_MEUR = pd.concat([opex_MEUR_2, opex_MEUR.set_index(opex_MEUR.index.astype(str) + '_dup')])
    # Group by  Country, Years, vehicle-type, energy-carrier, motor-type (sum)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier', 'motor-type'], aggregation_method='Sum')
    # opex-by-vehicle-motor [MEUR]
    out_9425_1 = opex_MEUR.rename(columns={'opex[MEUR]': 'opex-by-vehicle-motor[MEUR]'})
    out_1_2 = pd.concat([out_9425_1, out_9433_1.set_index(out_9433_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # Set 0
    out_1 = missing_value(df=out_1, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue=None)
    out_9565_1 = pd.concat([out_1, MEUR.set_index(MEUR.index.astype(str) + '_dup')])

    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for fuel mix
    out_4646_1 = pd.DataFrame(columns=['category_fffuels', 'category_biofuel', 'category_efuel'], data=[['liquid-ff-diesel', 'liquid-bio-diesel', 'liquid-syn-diesel'], ['liquid-ff-gasoline', 'liquid-bio-gasoline', 'liquid-syn-gasoline'], ['gaseous-ff-natural', 'gaseous-bio', 'gaseous-syn'], ['liquid-ff-kerosene', 'liquid-bio-kerosene', 'liquid-syn-kerosene'], ['liquid-ff-marinefueloil', 'liquid-bio-marinefueloil', 'liquid-syn-marinefueloil']])
    # Fuel Mix fffuels to biofuels
    out_5061_1 = fuel_mix(input_energy=energy_demand_TWh, switch_values=fuel_mix_percent, values_to_from=out_4646_1, col_category_to='category_biofuel')
    # Fuel Mix fffuels to efuels
    out_5067_1 = fuel_mix(input_energy=out_5061_1, switch_values=fuel_mix_percent, values_to_from=out_4646_1, col_category_to='category_efuel')

    # Energy-demand
    # before calibration
    # (fuel mix is applied)

    # energy-demand [TWh]  After fuel-mix Before calibration
    energy_demand_TWh = export_variable(input_table=out_5067_1, selected_variable='energy-demand[TWh]')
    # Group by  energy-carrier (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Calibration

    # Calibration

    # Calibration energy-demand [TWh]
    energy_demand_TWh_3 = import_data(trigram='tra', variable_name='energy-demand', variable_type='Calibration')
    # Apply Calibration on energy-demand[TWh]
    _, out_7107_2, out_7107_3 = calibration(input_table=energy_demand_TWh_2, cal_table=energy_demand_TWh_3, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand[TWh]')
    # Apply cal_rate to energy-demand[TWh]
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=out_7107_2, operation_selection='x * y', output_name='energy-demand[TWh]')

    # Energy-demand
    # after calibration
    # (fuel mix is applied)
    # 
    # Add vehicule-type dimension

    # energy-demand [TWh] After Calibration Before fuel switch
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # Fuel Switch

    # Apply fuel-switch levers (switch)
    # => Change fuel used for each transport (switch for fossil fuel to bio fuels)

    # Fuel Switch fffuels to biofuels
    out_7160_1 = x_switch(demand_table=energy_demand_TWh, switch_table=fuel_switch_percent, correlation_table=ratio)
    # Fuel Switch fffuels to efuels
    out_7161_1 = x_switch(demand_table=out_7160_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='synfuels')

    # Split ETS / non-ETS

    # RCP transport-ets-share [%] FROM TECH
    transport_ets_share_percent = import_data(trigram='tec', variable_name='transport-ets-share', variable_type='RCP')
    # Convert Unit % to - (*0.01)
    transport_ets_share_ = transport_ets_share_percent.drop(columns='transport-ets-share[%]').assign(**{'transport-ets-share[-]': transport_ets_share_percent['transport-ets-share[%]'] * 0.01})
    # energy-demand[TWh] (replace) = energy-demand[TWh] * transport-ets-share[-]  LEFT Join If missing => set to 1
    energy_demand_TWh = mcd(input_table_1=out_7161_1, input_table_2=transport_ets_share_, operation_selection='x * y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9203(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)
    out_9203_1 = helper_9203(input_table=energy_demand_TWh)

    # International-share

    # OTS only

    # OTS (only) aviation-domestic-share [%]
    aviation_domestic_share_percent = import_data(trigram='tra', variable_name='aviation-domestic-share', variable_type='OTS (only)')
    # Same as last available year
    aviation_domestic_share_percent = add_missing_years(df_data=aviation_domestic_share_percent)
    # Exclude aviation (we apply domestic share on it)
    out_9203_1_excluded = out_9203_1.loc[out_9203_1['vehicule-type'].isin(['aviation'])].copy()
    out_9203_1 = out_9203_1.loc[~out_9203_1['vehicule-type'].isin(['aviation'])].copy()
    # energy-demand[TWh] (replace) international aviation = energy-demand[TWh] * 1 - aviation-domestic-share[%]
    energy_demand_TWh = mcd(input_table_1=out_9203_1_excluded, input_table_2=aviation_domestic_share_percent, operation_selection='x * (1-y)', output_name='energy-demand[TWh]')
    # domestic-type = bunkers-aviation
    energy_demand_TWh['domestic-type'] = "bunkers-aviation"
    # energy-demand[TWh] (replace) domestic aviation = energy-demand[TWh] * aviation-domestic-share[%]
    energy_demand_TWh_2 = mcd(input_table_1=out_9203_1_excluded, input_table_2=aviation_domestic_share_percent, operation_selection='x * y', output_name='energy-demand[TWh]')
    # domestic-type = domestic
    energy_demand_TWh_2['domestic-type'] = "domestic"
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # Exclude marine (= 100% bunkers)
    out_9203_1_excluded = out_9203_1.loc[out_9203_1['vehicule-type'].isin(['marine'])].copy()
    out_9203_1 = out_9203_1.loc[~out_9203_1['vehicule-type'].isin(['marine'])].copy()
    # domestic-type = bunkers-marine
    out_9203_1_excluded['domestic-type'] = "bunkers-marine"
    # domestic-type = domestic
    out_9203_1['domestic-type'] = "domestic"
    out_9203_1 = pd.concat([out_9203_1, out_9203_1_excluded.set_index(out_9203_1_excluded.index.astype(str) + '_dup')])
    out_9452_1 = pd.concat([out_9203_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])

    # Energy-demand
    # after calibration
    # (fuel mix and fuel switch are applied)

    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=out_9452_1, selected_variable='energy-demand[TWh]')

    # Emissions

    # Use of variables

    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # emissions[Mt] = tra-combustion-emission-factor[Mt/TWh] * energy-demand[TWh]
    emissions_Mt = mcd(input_table_1=energy_demand_TWh, input_table_2=tra_combustion_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  gaes, domestic-type (sum)
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'gaes', 'domestic-type'], aggregation_method='Sum')

    # Calibration

    # Calibration

    # Calibration emissions [Mt]
    emissions_Mt_3 = import_data(trigram='tra', variable_name='emissions', variable_type='Calibration')
    # Apply Calibration on emissions[Mt]
    _, out_7103_2, out_7103_3 = calibration(input_table=emissions_Mt_2, cal_table=emissions_Mt_3, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')
    # Apply cal_rate emissions[Mt] = emissions[Mt] * cal_rate
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_7103_2, operation_selection='x * y', output_name='emissions[Mt]')

    # Energy-demand
    # after calibration
    # (fuel mix and fuel switch are applied)

    # emissions [Mt]
    emissions_Mt = export_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # Climate Emissions

    # Select variables

    # emissions[Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Add emissions-or-capture
    emissions_Mt['emissions-or-capture'] = "emissions"
    # All for : Climate Emissions
    emissions_Mt = column_filter(df=emissions_Mt, pattern='^.*$')

    # Cal_rate for GAES emissions

    # Cal rate for  emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out_7103_3, selected_variable='cal_rate_emissions[Mt]')
    # energy-carrier Top : electricity Bottom : all the others 
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['electricity'])].copy()

    # For : PyPsa

    # Group by year, veh-types, energy-carrier, tra-user, motor-types, domestic-type, energy-demand
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier', 'transport-user', 'motor-type', 'domestic-type'], aggregation_method='Sum')

    # For : Air Quality

    # Group by country, year, veh-user, motor-type, energy-carrier,  domestic-type
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier', 'transport-user', 'motor-type', 'domestic-type'], aggregation_method='Sum')
    demand = pd.concat([energy_demand_TWh_3, final_transport_demand_vkm.set_index(final_transport_demand_vkm.index.astype(str) + '_dup')])
    # All for : Air Quality
    demand = column_filter(df=demand, pattern='^.*$')
    # GroupBy transport-user
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'energy-carrier', 'transport-user'], aggregation_method='Sum')
    # Top: only passenger
    energy_demand_TWh_3 = energy_demand_TWh_3.loc[energy_demand_TWh_3['transport-user'].isin(['passenger'])].copy()
    # energy-demand-urban[TWh] = urban-share[%vkm] * energy-demand[TWh] (total)
    energy_demand_urban_TWh = mcd(input_table_1=urban_share_percent_vkm, input_table_2=energy_demand_TWh_3, operation_selection='x * y', output_name='energy-demand-urban[TWh]')
    # GroupBy transport-user
    energy_demand_urban_TWh = group_by_dimensions(df=energy_demand_urban_TWh, groupby_dimensions=['Country', 'Years', 'transport-user', 'energy-carrier'], aggregation_method='Sum')
    # energy-demand-urban[TWh]
    energy_demand_urban_TWh = export_variable(input_table=energy_demand_urban_TWh, selected_variable='energy-demand-urban[TWh]')
    # GroupBy transport-user
    energy_demand_urban_TWh_2 = group_by_dimensions(df=energy_demand_urban_TWh, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # share-energy-demand-urban[%TWh] = energy-demand-urban[TWh] (per vector) / energy-demand-urban[TWh] (total)
    share_energy_demand_urban_percent_TWh = mcd(input_table_1=energy_demand_urban_TWh, input_table_2=energy_demand_urban_TWh_2, operation_selection='x / y', output_name='share-energy-demand-urban[%TWh]')
    # share-energy-demand-urban[%TWh]
    share_energy_demand_urban_percent_TWh = export_variable(input_table=share_energy_demand_urban_percent_TWh, selected_variable='share-energy-demand-urban[%TWh]')
    # KPI
    energy_demand_urban_TWh = pd.concat([energy_demand_urban_TWh, share_energy_demand_urban_percent_TWh.set_index(share_energy_demand_urban_percent_TWh.index.astype(str) + '_dup')])
    # KPI
    out_9618_1 = pd.concat([transport_pkm, energy_demand_urban_TWh.set_index(energy_demand_urban_TWh.index.astype(str) + '_dup')])
    # KPI
    out_9619_1 = pd.concat([emissions_veh_g_per_km, out_9618_1.set_index(out_9618_1.index.astype(str) + '_dup')])

    # For : Power Supply 
    # 
    # => Power supply : keep only inland energy demand when bunkers

    # Select variables

    # energy-demand [TWh]
    energy_demand_TWh_3 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # For : Pathway Explorer

    # energy-demand[TWh]
    energy_demand_TWh_5 = use_variable(input_table=energy_demand_TWh_3, selected_variable='energy-demand[TWh]')
    # domestic-type Top : domestic Bottom : bunkers 
    energy_demand_TWh_4 = energy_demand_TWh_5.loc[energy_demand_TWh_5['domestic-type'].isin(['domestic'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh_5.loc[~energy_demand_TWh_5['domestic-type'].isin(['domestic'])].copy()
    # Group by  transport-user, domestic-type (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years', 'transport-user', 'domestic-type'], aggregation_method='Sum')
    # Rename variable to energy-demand-domestic-by- user[TWh]
    out_9517_1 = energy_demand_TWh_5.rename(columns={'energy-demand[TWh]': 'energy-demand-domestic-by-user[TWh]'})
    # Group by  energy-carrier, domestic-type (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'domestic-type'], aggregation_method='Sum')
    # Rename variable to energy-demand-domestic-by- carrier[TWh]
    out_9518_1 = energy_demand_TWh_5.rename(columns={'energy-demand[TWh]': 'energy-demand-domestic-by-carrier[TWh]'})
    # energy-carrier Top : all except electricity Bottom : electricity, hydrogen (we want to keep fuels  consumption as if non bunkers except for electricity & H2 to be ETS compliant
    energy_demand_TWh_excluded_excluded = energy_demand_TWh_excluded.loc[energy_demand_TWh_excluded['energy-carrier'].isin(['electricity', 'hydrogen'])].copy()
    energy_demand_TWh_excluded_2 = energy_demand_TWh_excluded.loc[~energy_demand_TWh_excluded['energy-carrier'].isin(['electricity', 'hydrogen'])].copy()
    energy_demand_TWh_4 = pd.concat([energy_demand_TWh_4, energy_demand_TWh_excluded_2.set_index(energy_demand_TWh_excluded_2.index.astype(str) + '_dup')])
    # Set to 0
    energy_demand_TWh_excluded_excluded['energy-demand[TWh]'] = 0.0
    # energy-demand
    energy_demand_TWh_4 = pd.concat([energy_demand_TWh_4, energy_demand_TWh_excluded_excluded.set_index(energy_demand_TWh_excluded_excluded.index.astype(str) + '_dup')])
    # Group by energy-carrier, ets-or-not (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'ets-or-not'], aggregation_method='Sum')
    # Rename variable to energy-demand-by- carrier-ets[TWh]
    out_9226_1 = energy_demand_TWh_5.rename(columns={'energy-demand[TWh]': 'energy-demand-domestic-by-carrier-ets[TWh]'})
    # Group by ets-or-not (sum)
    energy_demand_TWh_4 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years', 'ets-or-not'], aggregation_method='Sum')
    # Rename variable to energy-demand-by-ets[TWh]
    out_9227_1 = energy_demand_TWh_4.rename(columns={'energy-demand[TWh]': 'energy-demand-domestic-by-ets[TWh]'})
    # energy-demand for Pathway Explorer
    out_1 = pd.concat([out_9226_1, out_9227_1.set_index(out_9227_1.index.astype(str) + '_dup')])
    # Group by  domestic-type (sum)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'domestic-type'], aggregation_method='Sum')
    # Rename variable to energy-demand-bunkers- by-type[TWh]
    out_9519_1 = energy_demand_TWh_excluded.rename(columns={'energy-demand[TWh]': 'energy-demand-bunkers-by-type[TWh]'})
    # energy-demand for Pathway Explorer
    out_1_2 = pd.concat([out_9518_1, out_9519_1.set_index(out_9519_1.index.astype(str) + '_dup')])
    # energy-demand for Pathway Explorer
    out_1_2 = pd.concat([out_9517_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # energy-demand for Pathway Explorer
    out_1 = pd.concat([out_1_2, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Concat outputs for Pathway  explorer
    out_9569_1 = pd.concat([out_1, energy_demand_TWh_2.set_index(energy_demand_TWh_2.index.astype(str) + '_dup')])
    # Concat outputs for Pathway  explorer
    out_1 = pd.concat([out_9569_1, out_9207_1.set_index(out_9207_1.index.astype(str) + '_dup')])
    # Join outputs for Pathway Explorer to all results
    out_9213_1 = pd.concat([out_1, share_percent.set_index(share_percent.index.astype(str) + '_dup')])
    # ALL RESULTS
    out_9213_1 = column_filter(df=out_9213_1, pattern='^.*$')
    # Years
    out_6033_1 = out_9213_1.assign(Years=out_9213_1['Years'].astype(str))
    # Group by  energy-carrier (sum) Send all demand to Power (bunkers / international-aviation / national transport)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # All for : Power Oil Refineries
    energy_demand_TWh_2 = column_filter(df=energy_demand_TWh_2, pattern='^.*$')
    # All for : Employment
    energy_demand_TWh_3 = column_filter(df=energy_demand_TWh_2, pattern='^.*$')
    # Employment = dummy !
    out_1736_1 = energy_demand_TWh_3.copy()
    out_1736_1['dummy'] = ''
    # Top: fossil-fuel
    energy_demand_TWh_3 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['liquid-ff-kerosene', 'gaseous-ff-natural', 'liquid-ff-diesel', 'liquid-ff-marinefueloil', 'liquid-ff-gasoline'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh.loc[~energy_demand_TWh['energy-carrier'].isin(['liquid-ff-kerosene', 'gaseous-ff-natural', 'liquid-ff-diesel', 'liquid-ff-marinefueloil', 'liquid-ff-gasoline'])].copy()
    # Top: efuel & hydrogen
    energy_demand_TWh_excluded_2 = energy_demand_TWh_excluded.loc[energy_demand_TWh_excluded['energy-carrier'].isin(['liquid-syn-kerosene', 'gaseous-syn', 'liquid-syn-diesel', 'liquid-syn-marinefueloil', 'liquid-syn-gasoline', 'hydrogen'])].copy()
    energy_demand_TWh_excluded_excluded = energy_demand_TWh_excluded.loc[~energy_demand_TWh_excluded['energy-carrier'].isin(['liquid-syn-kerosene', 'gaseous-syn', 'liquid-syn-diesel', 'liquid-syn-marinefueloil', 'liquid-syn-gasoline', 'hydrogen'])].copy()
    # Top: bio-fuel
    energy_demand_TWh_excluded_excluded_2 = energy_demand_TWh_excluded_excluded.loc[energy_demand_TWh_excluded_excluded['energy-carrier'].isin(['liquid-bio-kerosene', 'gaseous-bio', 'liquid-bio-diesel', 'liquid-bio-marinefueloil', 'liquid-bio-gasoline'])].copy()
    energy_demand_TWh_excluded_excluded_excluded = energy_demand_TWh_excluded_excluded.loc[~energy_demand_TWh_excluded_excluded['energy-carrier'].isin(['liquid-bio-kerosene', 'gaseous-bio', 'liquid-bio-diesel', 'liquid-bio-marinefueloil', 'liquid-bio-gasoline'])].copy()
    # Top: electricity
    energy_demand_TWh_excluded_excluded_excluded = energy_demand_TWh_excluded_excluded_excluded.loc[energy_demand_TWh_excluded_excluded_excluded['energy-carrier'].isin(['electricity'])].copy()

    # Energy demand per carrier

    # Group by year
    energy_demand_TWh_excluded_excluded_excluded = group_by_dimensions(df=energy_demand_TWh_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # energy-demand[TWh] to energy-demand-elec[TWh]
    energy_demand_TWh_excluded_excluded_excluded = column_rename_regex(df=energy_demand_TWh_excluded_excluded_excluded, search_string='energy-demand', replace_string='energy-demand-elec')
    # Group by year
    energy_demand_TWh_excluded_excluded = group_by_dimensions(df=energy_demand_TWh_excluded_excluded_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # energy-demand[TWh] to energy-demand-biofuel[TWh]
    energy_demand_TWh_excluded_excluded = column_rename_regex(df=energy_demand_TWh_excluded_excluded, search_string='energy-demand', replace_string='energy-demand-biofuel')
    # KPI
    energy_demand_TWh_excluded_excluded = pd.concat([energy_demand_TWh_excluded_excluded, energy_demand_TWh_excluded_excluded_excluded.set_index(energy_demand_TWh_excluded_excluded_excluded.index.astype(str) + '_dup')])
    # Group by year
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # energy-demand[TWh] to energy-demand-efuel-H2[TWh]
    energy_demand_TWh_excluded = column_rename_regex(df=energy_demand_TWh_excluded, search_string='energy-demand', replace_string='energy-demand-efuel-hydrogene')
    # KPI
    energy_demand_TWh_excluded = pd.concat([energy_demand_TWh_excluded, energy_demand_TWh_excluded_excluded.set_index(energy_demand_TWh_excluded_excluded.index.astype(str) + '_dup')])
    # Group by year
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # energy-demand[TWh] to energy-demand-ff[TWh]
    energy_demand_TWh = column_rename_regex(df=energy_demand_TWh, search_string='energy-demand', replace_string='energy-demand-ff')
    # KPI
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_excluded.set_index(energy_demand_TWh_excluded.index.astype(str) + '_dup')])
    # KPI
    out_9349_1 = pd.concat([out_9619_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # KPI
    out_9348_1 = pd.concat([out_9349_1, elec_share_percent_vkm.set_index(elec_share_percent_vkm.index.astype(str) + '_dup')])
    # KPI
    out_9347_1 = pd.concat([out_9348_1, modal_share_HDV_percent_tkm.set_index(modal_share_HDV_percent_tkm.index.astype(str) + '_dup')])
    # KPI
    out_9346_1 = pd.concat([out_9347_1, elec_share_percent_vkm_2.set_index(elec_share_percent_vkm_2.index.astype(str) + '_dup')])
    # KPI
    out_9343_1 = pd.concat([out_9346_1, total_veh_fleet_passenger_LDV_number.set_index(total_veh_fleet_passenger_LDV_number.index.astype(str) + '_dup')])
    # KPI
    out_9344_1 = pd.concat([out_9343_1, transport_demand.set_index(transport_demand.index.astype(str) + '_dup')])
    # KPI
    out_9345_1 = pd.concat([out_9344_1, modal_share_passenger_LDV_percent_pkm.set_index(modal_share_passenger_LDV_percent_pkm.index.astype(str) + '_dup')])
    # KPI
    out_9301_1 = pd.concat([out_9345_1, technology_share_new_percent.set_index(technology_share_new_percent.index.astype(str) + '_dup')])
    # Concatenate KPIs for PE
    out_1 = pd.concat([out_1, out_9301_1.set_index(out_9301_1.index.astype(str) + '_dup')])
    # Concatenate Costs for PE
    out_1 = pd.concat([out_1, out_9565_1.set_index(out_9565_1.index.astype(str) + '_dup')])
    out_9212_1 = add_trigram(module_name=module_name, df=out_1)
    # All for : Pathway Explorer
    out_9212_1 = column_filter(df=out_9212_1, pattern='^.*$')

    # Cal_rate for energy-demand[TWh]

    # Cal rate for  energy-demand[TWh]
    cal_rate_energy_demand_TWh = use_variable(input_table=out_7107_3, selected_variable='cal_rate_energy-demand[TWh]')
    cal_rate_demand = pd.concat([cal_rate_transport_demand, cal_rate_energy_demand_TWh.set_index(cal_rate_energy_demand_TWh.index.astype(str) + '_dup')])
    cal_rate = pd.concat([cal_rate_demand, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])
    # All for : CAL RATE
    cal_rate = column_filter(df=cal_rate, pattern='^.*$')

    return out_9212_1, out_7164_1, energy_demand_TWh_2, energy_demand_TWh_2, cal_rate, out_1736_1, emissions_Mt, out_6033_1, demand, out_6036_1


