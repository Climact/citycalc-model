import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


# Buildings module
def buildings(lifestyle):
    # Visual quality check : 
    # energy demand
    # By building-type and end-use
    # => To be implemented !


    # Visual quality check : 
    # energy demand
    # By building-type and end-use
    # => To be implemented !


    # Visual quality check : 
    # energy demand
    # By building-type and end-use
    # => To be implemented !


    # Floor area : Non-residential & residential
    # demand


    # Quality check (based on 2015 - 2020)
    # accurate
    # 
    # A refaire !


    # SHOULD NOT BE HERE BUT
    # IN DATA ??


    # Quality check (based on 2015 - 2020)
    # accurate
    # 
    # A refaire !


    # To be improved ? : 
    # For the moment, we consider that unoccupied building do not consume energy
    # (for all energy-need except for cooking/hotwater (residential) and appliances (beacause they are not computed based on m2 need))


    # Quality check (based on 2015 - 2020)
    # accurate
    # 
    # A refaire !


    # Note :
    # 
    # On suppose un facteur d'émission nul pour :
    # - synfuels
    # - hydrogen
    # => les emissions de ces fuels (suite au fuel switch) = calculées dans power
    # /!\ : dans Power on ne regarde que les émissions liées à production de ces fuels mais non à leur utilisation => à revoir ici donc ?
    # 
    # Pour les biofuels : valeurs non nulles (gaseous)
    # /!\ : pas de double comptage avec ce qui est fait dans AFOLU ??


    # Quality check : 
    # buidling stock remains constant before and after the buidling stock logic


    # Quality check : 
    # buidling stock remains constant before and after the buidling stock logic


    # CUM PROD


    # Floor area : Unoccupied buildings
    # Objective Get unoccupied buildings
    # Depending on how many buildings remains (existing unrenovated + renovated), we determine the amount of building unoccupied each year (if new construction, we considere it will be occupied even if it is not the case).


    # Quality check (based on 2015 - 2020)
    # accurate
    # 
    # A refaire !


    # To be improved ? : 
    # For the moment, we consider that unoccupied building do not consume energy
    # (for all energy-need except for cooking/hotwater (residential) and appliances (beacause they are not computed based on m2 need))


    # We don't keep renovation category ??!!


    # Energy need : servers (buidling-type = non-residential / end-use = others / renovation-category = exi / energy-carrier = electricity)
    # 
    # => Assumption : energy-demand[TWh] = population[num] * 0.00000005 
    # 
    # => Should be linked to others energy-demand - but - in old module this part of energy-demand is not calibrated neither using fuel switch as it is done for other end-use
    # => We keep this part apart - SHOULD BE CHNAGED IN FUTUR ??


    # Add building-type
    # = non-residential


    # Add building-use
    # = others


    # Add end-use
    # = servers


    # Add energy-carrier
    # = electricity


    # Add renovation-category
    # = exi


    # Add energy-way-of-prod
    # = elec-direct ?


    # => Already in [-] ???
    # Remove this node ??


    # OPEX


    # TO DO : ADD FUELS TO NEW HEATING CAPACITIES SYSTEMS => USE % given in energy need (recompute it here !!!)
    # 
    # See upstream


    # For windows / roof / walls : add to scope !


    # Renovation rate
    # => Total


    # TEMP > delete when previous has been migrated


    # For : Industry
    # => Yearly floor area (new construction and new renovation)
    # => SHOULD NOT BE material-surface that is used INSTEAD ???


    # Pivot


    # For : Minerals
    # => New pipes length (for district heating)


    # For : Air Quality
    # => Energy demand for heating by vector & building-type


    # Pivot



    module_name = 'buildings'

    # Energy DEMAND

    # Energy carrier switch

    # Hard-coded here 
    # => To move in google sheet ??

    out_9207_1 = pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['fffuels', 'gaseous-ff-natural', 'biofuels', 'gaseous-bio'], ['fffuels', 'liquid-ff-oil', 'biofuels', 'liquid-bio'], ['fffuels', 'gaseous-ff-natural', 'hydrogen', 'hydrogen'], ['fffuels', 'gaseous-ff-natural', 'synfuels', 'gaseous-syn'], ['fffuels', 'liquid-ff-oil', 'synfuels', 'liquid-syn']])
    # ratio[-] = 1 (energy final, no need to  account for energy efficiency differences)
    ratio = out_9207_1.assign(**{'ratio[-]': 1.0})

    # Formating data for other modules + Pathway Explorer

    # For : Minerals
    # => New appliances

    # Note : others => phone  (in industry = smartphone??!!)
    out_8231_1 = pd.DataFrame(columns=['new_name', 'old_name'], data=[['new-appliances_computer[num]', 'bld_new_computer[num]'], ['new-appliances_dishwasher[num]', 'bld_new_dishwasher[num]'], ['new-appliances_dryer[num]', 'bld_new_dryer[num]'], ['new-appliances_freezer[num]', 'bld_new_freezer[num]'], ['new-appliances_fridge[num]', 'bld_new_fridge[num]'], ['new-appliances_others[num]', 'bld_new_phone[num]'], ['new-appliances_tv[num]', 'bld_new_tv[num]'], ['new-appliances_wmachine[num]', 'bld_new_wmachine[num]']])

    # For : Minerals
    # => Yearly floor area (new construction and new renovation)

    out_8078_1 = pd.DataFrame(columns=['new_name', 'old_name'], data=[['floor-area-yearly_non-residential_constructed[m2]', 'bld_floor-area_new_non-residential[m2]'], ['floor-area-yearly_residential_constructed[m2]', 'bld_floor-area_new_residential[m2]'], ['floor-area-yearly_non-residential_renovated[m2]', 'bld_floor-area_reno_non-residential[m2]'], ['floor-area-yearly_residential_renovated[m2]', 'bld_floor-area_reno_residential[m2]']])

    # Input data 
    # + inject module name (flow variable)

    # Product / material / ressource demand

    # Appliances - Production Demand Calculation
    # Objective: The submodule aims at modeling the new appliances needed to reach the demand
    # 
    # Main inputs: 
    # number of appliances by type in year x (appliance-own[num]),
    # lifetime of appliances by type in year x (appliance-lifetime[num]),
    # 
    # Main outputs: 
    # number of new appliances by type per year (new-appliances[num])

    # Select Variables

    # product-substitution-rate [%] (from lifestyle)
    product_substitution_rate_percent = use_variable(input_table=lifestyle, selected_variable='product-substitution-rate[%]')
    # Convert Unit [%] to [-] (* 0.01) ?
    product_substitution_rate_ = product_substitution_rate_percent.drop(columns='product-substitution-rate[%]').assign(**{'product-substitution-rate[-]': product_substitution_rate_percent['product-substitution-rate[%]'] * 0.01})
    # Select Years > baseyear
    product_substitution_rate_, product_substitution_rate__excluded = filter_dimension(df=product_substitution_rate_, dimension='Years', operation_selection='>', value_years=Globals.get().base_year)
    # product-substitution-factor[-] = 1
    product_substitution_factor = product_substitution_rate__excluded.assign(**{'product-substitution-factor[-]': 1.0})
    # product-substitution-factor[-] = 1 + product-substitution-rate[-]
    product_substitution_factor_2 = product_substitution_rate_.assign(**{'product-substitution-factor[-]': 1.0+product_substitution_rate_['product-substitution-rate[-]']})
    # Historical and Future
    product_substitution_factor = pd.concat([product_substitution_factor_2, product_substitution_factor])
    # product-substitution-factor [-]
    product_substitution_factor = use_variable(input_table=product_substitution_factor, selected_variable='product-substitution-factor[-]')

    # Parameters

    # RCP appliance-lifetime [years]
    appliance_lifetime_years = import_data(trigram='bld', variable_name='appliance-lifetime', variable_type='RCP')
    # appliance-lifetime[years] (replace) = appliance-lifetime[years] * product-substitution-factor[-]
    appliance_lifetime_years = mcd(input_table_1=product_substitution_factor, input_table_2=appliance_lifetime_years, operation_selection='x * y', output_name='appliance-lifetime[years]')
    # new-appliance-ratio[-] = 1 / appliance-lifetime[years]
    new_appliance_ratio = appliance_lifetime_years.assign(**{'new-appliance-ratio[-]': 1.0/appliance_lifetime_years['appliance-lifetime[years]']})

    # Select Variables

    # new-appliance-ratio [-]
    new_appliance_ratio = use_variable(input_table=new_appliance_ratio, selected_variable='new-appliance-ratio[-]')
    # appliance-own [num] (from lifestyle)
    appliance_own_num = use_variable(input_table=lifestyle, selected_variable='appliance-own[num]')
    # new-appliances[num] = appliance-own[num] * new-appliance-ratio[-]
    new_appliances_num = mcd(input_table_1=appliance_own_num, input_table_2=new_appliance_ratio, operation_selection='x * y', output_name='new-appliances[num]')
    # new-appliances [num]
    new_appliances_num = export_variable(input_table=new_appliances_num, selected_variable='new-appliances[num]')

    # For : Industry
    # => New appliances

    # new-appliances[num]
    new_appliances_num = use_variable(input_table=new_appliances_num, selected_variable='new-appliances[num]')

    # Pivot

    # by country, Years on appliances
    out_8033_1, _, _ = pivoting(df=new_appliances_num, agg_dict={'new-appliances[num]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['appliances'])
    out_8036_1 = missing_value_column_filter(df=out_8033_1, missing_threshold=0.9, type_of_pattern='Manual')
    out_8036_1 = column_rename_regex(df=out_8036_1, search_string='(.*)\\+(.*)(\\[.*)', replace_string='$2_$1$3')

    def helper_8232(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_8232_1 = helper_8232(input_table_1=out_8231_1, input_table_2=out_8036_1)

    # Costs
    # Objective : Calculate Capex and opex
    # 
    # Main inputs :
    # - new appliances [num]
    # - floor area yearly [Mm2] (only renovated and constructed - exi = removed)
    # - energy-demand [TWh] (for heating) and heating-capacity[kW]
    # - new pipe length [km]
    # 
    # Main outputs :
    # - cost of new appliances (capex only)
    # - cost of yearly construction and renovation
    # - cost for individual heating system
    # - cost for shared heating system (capex only => based on pipe length)

    # Appliances
    # Only new one => Capex

    # RCP costs-by-appliances [MEUR/unit] From TECH
    costs_by_appliances_MEUR_per_unit = import_data(trigram='tec', variable_name='costs-by-appliances', variable_type='RCP')
    # RCP price-indices [-] From TECH
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')
    # OTS/LL wacc [%] From TECH
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = bld
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['bld'])].copy()
    # Group by all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Compute capex for new-appliances[num]
    out_9282_1 = compute_costs(df_activity=new_appliances_num, df_unit_costs=costs_by_appliances_MEUR_per_unit, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='new-appliances[num]')
    # RCP appliances-cost-user [-]
    appliances_cost_user_ = import_data(trigram='bld', variable_name='appliances-cost-user', variable_type='RCP')
    # Get cost-users capex[MEUR] = capex[MEUR] * appliances-cost-user[-]
    capex_MEUR = mcd(input_table_1=out_9282_1, input_table_2=appliances_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by Country, Years (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # capex-by-appliances[MEUR]
    out_9320_1 = capex_MEUR.rename(columns={'capex[MEUR]': 'capex-by-appliances[MEUR]'})

    # Floor area : Residential demand
    # Objective Compute the building demand (in m2) for the residential sector.
    # To do so, we compute the amount of buildings (depends on household-size and population). Then, we split this nb between household (mfh) and appartment (sfh) and we compute the corresponding demand (depends on building-surface)

    # Select variables from Lifestyle module

    # population [cap]
    population_cap = use_variable(input_table=lifestyle, selected_variable='population[cap]')

    # Apply household-size levers (reduce)
    # => determine the nb of pers living in the same household

    # OTS/FTS household-size [cap/household] (from lifestyle)
    household_size_cap_per_household = import_data(trigram='lfs', variable_name='household-size')
    # households[num] = population[cap] / household-size [cap/household]
    households_num = mcd(input_table_1=population_cap, input_table_2=household_size_cap_per_household, operation_selection='x / y', output_name='households[num]')

    # Apply floor-intensity levers (switch)
    # => determine the % of household that are house (mfh). The remaining % correspond to appartment

    # OTS/FTS household-share-appartment [%] (from lifestyle)
    household_share_appartment_percent = import_data(trigram='lfs', variable_name='household-share-appartment')
    # households[num] (for mfh) = households-total[num] * household-share-appartment[%]
    households_num_2 = mcd(input_table_1=households_num, input_table_2=household_share_appartment_percent, operation_selection='x * y', output_name='households[num]')
    # add building-use = mfh
    households_num_2['building-use'] = "mfh"
    # households[num] (for sfh) = households-total[num] * (1 - household-share-appartment[%])
    households_num_3 = mcd(input_table_1=households_num, input_table_2=household_share_appartment_percent, operation_selection='x * (1-y)', output_name='households[num]')
    # add building-use = sfh
    households_num_3['building-use'] = "sfh"
    households_num_2 = pd.concat([households_num_2, households_num_3])
    # households [num]
    households_num_2 = export_variable(input_table=households_num_2, selected_variable='households[num]')

    # Apply household-surface levers (reduce)
    # => determine the m2 per household (mfh / sfh)

    # OTS/FTS building-size [m2/household]
    building_size_m2_per_household = import_data(trigram='bld', variable_name='building-size')
    # floor-area-demand[m2] = households[num] * building-size [m2/household]
    floor_area_demand_m2 = mcd(input_table_1=households_num_2, input_table_2=building_size_m2_per_household, operation_selection='x * y', output_name='floor-area-demand[m2]')

    # Calibration : floor-area-demand (residential)

    # Calibration floor-area [Mm2] (residential)
    floor_area_Mm2 = import_data(trigram='bld', variable_name='floor-area', variable_type='Calibration')
    # Convert Unit 1000m2 to m2 (*1000)
    floor_area_m2 = floor_area_Mm2.drop(columns='floor-area[1000m2]').assign(**{'floor-area[m2]': floor_area_Mm2['floor-area[1000m2]'] * 1000.0})
    # Apply Calibration on floor-area-demand[m2]
    floor_area_demand_m2_2, _, _ = calibration(input_table=floor_area_demand_m2, cal_table=floor_area_m2, data_to_be_cal='floor-area-demand[m2]', data_cal='floor-area[m2]')
    # household-share[%] = households[num] / household[num] (total)
    household_share_percent = mcd(input_table_1=households_num_2, input_table_2=households_num, operation_selection='x / y', output_name='household-share[%]')
    # household-share [%]
    household_share_percent = export_variable(input_table=household_share_percent, selected_variable='household-share[%]')

    # Total energy demand per end-use

    # Energy need : Appliances (only residential)
    # => Energy-demand = appliances-use[h] * energy-need-appliances[kWh/h]
    # => Apply share of mfh / sfh on energy-need to get the demand by building use

    # household-share [%]
    household_share_percent = use_variable(input_table=household_share_percent, selected_variable='household-share[%]')

    # Energy need : differents end-use linked to household (cooking / hotwater) (residential)
    # => Energy-demand = households[num] * energy-need-by-household[kWh/household]

    # households [num]
    households_num = use_variable(input_table=households_num_2, selected_variable='households[num]')

    # Apply energy-need (household) levers (reduce)
    # => determine the energy consumption by household for some specific end-use (hotwater, cooking, ...)

    # OTS/FTS energy-need-by-household [kWh/household]
    energy_need_by_household_kWh_per_household = import_data(trigram='bld', variable_name='energy-need-by-household')
    # energy-demand[kWh] = households[num] * energy-need-by-household[kWh/household]
    energy_demand_kWh = mcd(input_table_1=households_num, input_table_2=energy_need_by_household_kWh_per_household, operation_selection='x * y', output_name='energy-demand[kWh]')
    # kWh to GWh (*0.000001)
    energy_demand_GWh = energy_demand_kWh.drop(columns='energy-demand[kWh]').assign(**{'energy-demand[GWh]': energy_demand_kWh['energy-demand[kWh]'] * 1e-06})

    # Floor area : Non-residential demand
    # Objective Compute the floor area demand for non residential buildings (for the moment, only OTS/FTS data - no computation)

    # Apply floor-area levers (reduce)
    # => determine the floor area demand for  residential area and so the share between sfh and mfh

    # OTS/FTS floor-area-demand [1000m2]
    floor_area_demand_1000m2 = import_data(trigram='bld', variable_name='floor-area-demand')
    # Convert Unit 1000m2 to m2 (*1000)
    floor_area_demand_m2 = floor_area_demand_1000m2.drop(columns='floor-area-demand[1000m2]').assign(**{'floor-area-demand[m2]': floor_area_demand_1000m2['floor-area-demand[1000m2]'] * 1000.0})
    floor_area_demand_m2_2 = pd.concat([floor_area_demand_m2_2, floor_area_demand_m2])
    # floor-area-demand [m2]
    floor_area_demand_m2_2 = export_variable(input_table=floor_area_demand_m2_2, selected_variable='floor-area-demand[m2]')

    # Floor area : New building (construction)
    # Objective xx

    # floor-area-demand[m2]
    floor_area_demand_m2_2 = use_variable(input_table=floor_area_demand_m2_2, selected_variable='floor-area-demand[m2]')

    # Floor area : Offer
    # Objective Compute the floor area offer
    # It depends on the initial building stock + the demolition we apply to building stock

    # floor-area-demand [m2] (non-residential)
    floor_area_demand_m2 = use_variable(input_table=floor_area_demand_m2, selected_variable='floor-area-demand[m2]')
    # Keep Years == 2000
    floor_area_Mm2, _ = filter_dimension(df=floor_area_Mm2, dimension='Years', operation_selection='=', value_years='2000')
    # Convert Unit 1000m2 to m2 (*1000)
    floor_area_m2 = floor_area_Mm2.drop(columns='floor-area[1000m2]').assign(**{'floor-area[m2]': floor_area_Mm2['floor-area[1000m2]'] * 1000.0})

    # Apply demolition-rate levers (?)
    # => determine the rate of demolition (example : allows to destroy bad EPC category and rebuild them with a good EPC label, instead of renovated them)

    # OTS/FTS building-demolition-rate [-]
    building_demolition_rate_ = import_data(trigram='bld', variable_name='building-demolition-rate')
    # undemolition-rate[-] = 1 - building-demolition-rate[-]
    undemolition_rate = building_demolition_rate_.assign(**{'undemolition-rate[-]': 1.0-building_demolition_rate_['building-demolition-rate[%]']})
    # Compute timestep
    _, out_9363_2 = lag_variable(df=undemolition_rate, in_var='building-demolition-rate[%]')
    # undemolition-rate [-]
    undemolition_rate = use_variable(input_table=undemolition_rate, selected_variable='undemolition-rate[-]')
    # undemolition-rate[-] (replace) = undemolition-rate[-] ** Timestep
    undemolition_rate = mcd(input_table_1=undemolition_rate, input_table_2=out_9363_2, operation_selection='x ^ y', output_name='undemolition-rate[-]')

    def helper_7514(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # get dimensions columns
        dimensions = []
        for name in output_table.select_dtypes(include=['object']):  # object => refers to string
            dimensions.append(name)
        
        # Get variable name
        variable_name = 'undemolition-rate[-]'
        new_variable_name = 'undemolition-rate-acc[-]'
        
        # Cum prod
        sort_dimensions = dimensions.copy()
        sort_dimensions.append('Years')
        output_table = output_table.sort_values(by=sort_dimensions)
        output_table[new_variable_name] = output_table.groupby(by=dimensions)[variable_name].cumprod()
        return output_table
    # undemoliition-rate-acc[-] = Cum product of undemolished-rate[-]  By all categories (Years ascending sorting)
    out_7514_1 = helper_7514(input_table=undemolition_rate)
    # undemolition-rate-acc [-]
    undemolition_rate_acc = export_variable(input_table=out_7514_1, selected_variable='undemolition-rate-acc[-]')
    # Keep Years == 2000
    floor_area_demand_m2, _ = filter_dimension(df=floor_area_demand_m2, dimension='Years', operation_selection='=', value_years='2000')
    # to floor-area[m2]
    out_9381_1 = floor_area_demand_m2.rename(columns={'floor-area-demand[m2]': 'floor-area[m2]'})
    out_9380_1 = pd.concat([floor_area_m2, out_9381_1])
    out_9380_1 = column_filter(df=out_9380_1, columns_to_drop=['Years'])
    # floor-area-offer[m2] = floora-area[m2] * undemolition-rate-acc[-]
    floor_area_offer_m2 = mcd(input_table_1=out_9380_1, input_table_2=undemolition_rate_acc, operation_selection='x * y', output_name='floor-area-offer[m2]')
    # floor-area-offer [m2]
    floor_area_offer_m2 = export_variable(input_table=floor_area_offer_m2, selected_variable='floor-area-offer[m2]')

    # Floor area : Renovation demand (EPC switch)
    # Objective Compute the floor area offer
    # It depends on the initial building stock + the demolition we apply to building stock

    # floor-area-offer [m2]
    floor_area_offer_m2 = use_variable(input_table=floor_area_offer_m2, selected_variable='floor-area-offer[m2]')
    # CP (bld_)renovation-category [%]
    bld_renovation_category_percent = import_data(trigram='bld', variable_name='bld_renovation-category', variable_type='CP')
    # Remove module and data-type
    bld_renovation_category_percent = column_filter(df=bld_renovation_category_percent, columns_to_drop=['data_type', 'module'])
    # CP (bld_)epc-order [%]
    bld_epc_order_percent = import_data(trigram='bld', variable_name='bld_epc-order', variable_type='CP')
    # Remove module and data-type
    bld_epc_order_percent = column_filter(df=bld_epc_order_percent, columns_to_drop=['data_type', 'module'])
    # OTS (only) epc-mix [%]
    epc_mix_percent = import_data(trigram='bld', variable_name='epc-mix', variable_type='OTS (only)')

    # Apply trigger-point linked levers (reduce)
    # => determine rate of each trigger point
    # => determine if we activate or not this trigger point

    # OTS/FTS building-renovation-rate [%]
    building_renovation_rate_percent = import_data(trigram='bld', variable_name='building-renovation-rate')
    # OTS/FTS trigger-point-activation [-]
    trigger_point_activation_ = import_data(trigram='bld', variable_name='trigger-point-activation')
    # building-renovation-rate[%] (replace) = building-renovation-rate [%] * trigger-point-activation[-]
    building_renovation_rate_percent_2 = mcd(input_table_1=building_renovation_rate_percent, input_table_2=trigger_point_activation_, operation_selection='x * y', output_name='building-renovation-rate[%]')

    # Apply epc-switch levers (switch)
    # => determine to which epc category the building goes after renovation

    # OTS/FTS renovation-category-switch [%]
    renovation_category_switch_percent = import_data(trigram='bld', variable_name='renovation-category-switch')
    # Same as last available year (for historic-mean)
    renovation_category_switch_percent = add_missing_years(df_data=renovation_category_switch_percent)
    # RCP renovation-delay [years]
    renovation_delay_years = import_data(trigram='bld', variable_name='renovation-delay', variable_type='RCP')
    # OTS/FTS ban-values [years]
    ban_values_years = import_data(trigram='bld', variable_name='ban-values')

    # Reformat values (depends on lever position)


    def helper_9376(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # get dimensions columns
        dimensions = []
        for name in output_table.select_dtypes(include=['object']):  # object => refers to string
            dimensions.append(name)
        
        # Get variable name
        variable_name = [x for x in output_table.columns if x.find("[") > 0][0]
        
        # Cum prod
        sort_dimensions = dimensions.copy()
        sort_dimensions.append('Years')
        output_table = output_table.sort_values(by=sort_dimensions)
        
        dict_yrs = {
            "start-year":0,
            "end-year":1
        }
        
        # Get start year and end-year
        df_all = pd.DataFrame()
        for i in dict_yrs:
            val = dict_yrs[i]
            df = output_table.loc[(output_table[variable_name] == val), :].copy()
            if i == "start-year":
                df = df.groupby(by=dimensions)["Years"].max().reset_index()
            else:
                df = df.groupby(by=dimensions)["Years"].min().reset_index()
            df.rename(columns={"Years":i}, inplace=True)
            if df_all.empty:
                df_all = df.copy()
            else:
                df_all = df_all.merge(df, on=dimensions, how='outer')
        # If missing Years : set 3000 (for both start and end year) => No ban applied
        mask = (df_all["end-year"].isna())
        for col in ["start-year", "end-year"]:
            df_all.loc[mask, col] = 3000
        df_all["steady-year"] = df_all["start-year"] + df_all["end-year"]/2 - df_all["start-year"]/2
        df_all["steady-year"] = df_all["steady-year"].round(0)
        # Pivot => start-/steady-/end-year = year-category (dimension)
        output_table = pd.melt(df_all, id_vars=dimensions, value_vars=["start-year", "steady-year", "end-year"])
        output_table.rename(columns={"variable": "year-category", "value": variable_name}, inplace=True)
        return output_table
    # ban-values[years]  Get start-year = last years with 0 value end-year = first year with 1 value steady-year = half year (between start- and end-year) 
    out_9376_1 = helper_9376(input_table=ban_values_years)
    # 1) Building stock initial 2) Order for switch 3) PEB-mix 4) Status-change 5) PEB-switch 6) Delay in switch 7) BAN (end-year/start-year)  POUR MIX : METTRE VALEUR 100% pour la dernière categories !  Rajouter les année manquantes ? (pas de 1 an au lieu de 5 ?)
    out_9508_1, out_9508_2 = buildings_stock_logic(demand_df=floor_area_offer_m2, order_df=bld_epc_order_percent, mix_df=epc_mix_percent, status_change_df=building_renovation_rate_percent_2, switch_df=renovation_category_switch_percent, switch_delay_df=renovation_delay_years, ban_df=out_9376_1)
    # yearly-change [m2]
    yearly_change_m2 = use_variable(input_table=out_9508_1, selected_variable='yearly-change[m2]')
    # yearly-change[m2] = yearly-change[m2] * ratio[-] (we associate renovation-category)
    yearly_change_m2 = mcd(input_table_1=bld_renovation_category_percent, input_table_2=yearly_change_m2, operation_selection='x * y', output_name='yearly-change[m2]')
    # Group by all except trigger-point, category-from and category-to (sum)
    yearly_change_m2 = group_by_dimensions(df=yearly_change_m2, groupby_dimensions=['renovation-category', 'Country', 'Years', 'building-type', 'building-use'], aggregation_method='Sum')
    # area-type = renovated
    yearly_change_m2['area-type'] = "renovated"
    # yearly-change [m2] (renovated)
    yearly_change_m2 = export_variable(input_table=yearly_change_m2, selected_variable='yearly-change[m2]')
    # Group by  all dimensions (sum)
    out_9508_2 = group_by_dimensions(df=out_9508_2, groupby_dimensions=['Years', 'Country', 'building-type', 'building-use', 'epc-category', 'area-type'], aggregation_method='Sum')

    # Remove unoccupied buildings from existing / renovated buildings
    # Future improvement : allows switch from residential to non-residentail (and vice-versa) 
    # when some of them are unoccupied to avoid new construction

    #  buidling-stock [m2]
    building_stock_m2_2 = use_variable(input_table=out_9508_2, selected_variable='building-stock[m2]')
    # Group by all dimensions (sum)
    building_stock_m2_4 = group_by_dimensions(df=building_stock_m2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'epc-category', 'area-type'], aggregation_method='Sum')
    # Group by all dimensions except area-type / epc-cat (sum)
    building_stock_m2_3 = group_by_dimensions(df=building_stock_m2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use'], aggregation_method='Sum')
    # Group by all dimensions except area-type (sum)
    building_stock_m2 = group_by_dimensions(df=building_stock_m2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'epc-category'], aggregation_method='Sum')
    # pct-epc-by-area-type[%] = building-stock[m2] (total except area-type) / building-stock[m2] (total except area-type / epc-cat)
    pct_epc_by_area_type_percent = mcd(input_table_1=building_stock_m2_4, input_table_2=building_stock_m2, operation_selection='x / y', output_name='pct-epc-by-area-type[%]')
    # pct-epc-by-area-type-epc[%] = building-stock[m2] (total except area-type) / building-stock[m2] (total except area-type / epc-cat)
    pct_epc_by_area_type_epc_percent = mcd(input_table_1=building_stock_m2, input_table_2=building_stock_m2_3, operation_selection='x / y', output_name='pct-epc-by-area-type-epc[%]')
    # missing-floor-area [m2] = floor-area-demand[m2] - floor-area-offer[m2]
    missing_floor_area_m2 = mcd(input_table_1=floor_area_demand_m2_2, input_table_2=floor_area_offer_m2, operation_selection='x - y', output_name='missing-floor-area[m2]')

    # Future improvment !
    # Attention : we shoul flag area-type that switch from sfh to mfh (or sfh to mfh) in order to attribute sepcific renovation there !!!


    def helper_9505(input_table) -> pd.DataFrame:
        import numpy as np
        
        # Copy input to output
        output_table = input_table.copy()
        metric = 'missing-floor-area[m2]'
        
        # Split residential and non-residential
        mask_resid = (output_table['building-type'] == "residential")
        df_resid = output_table.loc[mask_resid, :].copy()
        df_non_resid = output_table.loc[~mask_resid, :].copy()
        
        # For residential allocated extra buildings (if any) to other category if required
        for i in ["mfh", "sfh"]:
            mask_i = (df_resid['building-use'] == i)
            df_i = df_resid.loc[mask_i, :].copy()
            df_i.rename(columns={metric:i}, inplace=True)
            del df_i["building-use"]
            common_cols = list(np.intersect1d(df_resid.columns, df_i.columns))
            df_resid = pd.merge(df_resid, df_i, on=common_cols, how='inner')
        
        df_resid["extra"] = 0
        for cat in ["mfh", "sfh"]:
            other_cat = "sfh"
            if cat == "sfh":
                other_cat = "mfh"
            mask = (df_resid[cat] > 0) & (df_resid[other_cat] < 0) & (df_resid[cat] > df_resid[other_cat].abs())
            df_resid.loc[mask, "extra"] = df_resid.loc[mask, other_cat].abs()
            mask = (df_resid[cat] > 0) & (df_resid[other_cat] < 0) & (df_resid[cat] < df_resid[other_cat].abs())
            df_resid.loc[mask, "extra"] = df_resid.loc[mask, cat]
        
        mask = (df_resid[metric] > 0)
        df_resid.loc[mask, metric] = df_resid.loc[mask, metric] - df_resid.loc[mask, "extra"]
        df_resid.loc[~mask, metric] = df_resid.loc[~mask, metric] + df_resid.loc[~mask, "extra"]
        for col in ["mfh", "sfh", "extra"]:
            del df_resid[col]
        
        # Output
        output_table = pd.concat([df_resid, df_non_resid], ignore_index=True)
        return output_table
    # switch mfh to sfh or sfh to mfh if one of them is missing and the other one not missing  On devrait appliquer ce switch à l'offre qui rentre dans la stock logic !!! Voir si utile ? Pcq la demande énergétique ne varie pas entre les sfh et mfh mais ça biaise quand même la répartition des mfh et sfh dans la rénovation !
    out_9505_1 = helper_9505(input_table=missing_floor_area_m2)
    # If > 0 : set 0 (no unoccupied buildings) If < 0 : set absolute value (unoccupied building)
    missing_floor_area_m2 = out_9505_1.copy()
    mask = missing_floor_area_m2['missing-floor-area[m2]']>=0
    missing_floor_area_m2.loc[mask, 'missing-floor-area[m2]'] = 0
    missing_floor_area_m2.loc[~mask, 'missing-floor-area[m2]'] = -missing_floor_area_m2.loc[~mask, 'missing-floor-area[m2]']
    # area-type = unoccupied
    missing_floor_area_m2['area-type'] = "unoccupied"
    # building-stock[m2] (replace) = missing-floor-area[m2] * pct-epc-by-area-type-epc[%]
    building_stock_m2 = mcd(input_table_1=pct_epc_by_area_type_epc_percent, input_table_2=missing_floor_area_m2, operation_selection='x * y', output_name='building-stock[m2]')
    # buidling-stock [m2] (unoccupied)
    building_stock_m2 = export_variable(input_table=building_stock_m2, selected_variable='building-stock[m2]')
    # Group by all dimensions except area-type (sum)
    building_stock_m2_3 = group_by_dimensions(df=building_stock_m2, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'epc-category'], aggregation_method='Sum')
    # building-stock[m2] (replace) = building-stock[m2] * pct-epc-by-area-type[%] (distribute building removals  by area-type)
    building_stock_m2_3 = mcd(input_table_1=pct_epc_by_area_type_percent, input_table_2=building_stock_m2_3, operation_selection='x * y', output_name='building-stock[m2]')
    # buidling-stock[m2] (replace) = buidling-stock[m2] (initial) - buidling-stock[m2] (unoccupied)
    building_stock_m2_2 = mcd(input_table_1=building_stock_m2_2, input_table_2=building_stock_m2_3, operation_selection='x - y', output_name='building-stock[m2]')
    # buidling-stock [m2] (renovated and existing-occupied)
    building_stock_m2_2 = export_variable(input_table=building_stock_m2_2, selected_variable='building-stock[m2]')
    # If < 0 : set 0 (no need for new building)
    missing_floor_area_m2 = out_9505_1.copy()
    mask = missing_floor_area_m2['missing-floor-area[m2]']<0
    missing_floor_area_m2.loc[mask, 'missing-floor-area[m2]'] = 0
    missing_floor_area_m2.loc[~mask, 'missing-floor-area[m2]'] = missing_floor_area_m2.loc[~mask, 'missing-floor-area[m2]']

    def helper_9398(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # get dimensions columns
        dimensions = []
        for name in output_table.select_dtypes(include=['object']):  # object => refers to string
            dimensions.append(name)
        
        # Get variable name
        missing_stock = 'missing-floor-area[m2]'
        building_stock = 'building-stock[m2]'
        yearly_change = 'yearly-change[m2]'
        
        # Add new columns to table ; default values = 0
        output_table[building_stock] = 0
        output_table[yearly_change] = 0
        
        # Cum prod
        sort_dimensions = dimensions.copy()
        sort_dimensions.append('Years')
        output_table = output_table.sort_values(by=sort_dimensions)
        yr_list = output_table["Years"].unique().tolist()
        yr_list.sort()
        for yr in yr_list:
            # Build new building if require (missing > existing stock)
            mask = (output_table["Years"] == yr) & (output_table[missing_stock] > output_table[building_stock])
            output_table.loc[mask, yearly_change] = output_table.loc[mask, missing_stock] - output_table.loc[mask, building_stock]
            # Recompute existing stock
            output_table[building_stock] = output_table.groupby(by=dimensions)[yearly_change].cumsum()
        return output_table
    # outputs : 1) buidling-stock[m2] 2) yearly-change[m2]
    out_9398_1 = helper_9398(input_table=missing_floor_area_m2)
    # area-type = constructed
    out_9398_1['area-type'] = "constructed"
    #  buidling-stock [m2]
    building_stock_m2_3 = use_variable(input_table=out_9398_1, selected_variable='building-stock[m2]')
    # yearly-change [m2]
    yearly_change_m2_2 = use_variable(input_table=out_9398_1, selected_variable='yearly-change[m2]')
    # CP (bld_)construction-category [%]
    bld_construction_category_percent = import_data(trigram='bld', variable_name='bld_construction-category', variable_type='CP')
    # Remove module and data-type
    bld_construction_category_percent = column_filter(df=bld_construction_category_percent, columns_to_drop=['data_type', 'module'])
    # OTS / FTS construction-epc-mix [%]
    construction_epc_mix_percent = import_data(trigram='bld', variable_name='construction-epc-mix')
    # yearly-change[m2] (replace) = yearly-change[m2] * construction-epc-mix [%]
    yearly_change_m2_2 = mcd(input_table_1=yearly_change_m2_2, input_table_2=construction_epc_mix_percent, operation_selection='x * y', output_name='yearly-change[m2]')
    # yearly-change[m2] = yearly-change[m2] * ratio[-] (we associate renovation-category)
    yearly_change_m2_2 = mcd(input_table_1=bld_construction_category_percent, input_table_2=yearly_change_m2_2, operation_selection='x * y', output_name='yearly-change[m2]')
    # Group by  all except epc-category (sum)
    yearly_change_m2_2 = group_by_dimensions(df=yearly_change_m2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type', 'renovation-category'], aggregation_method='Sum')
    # yearly-change [m2] (constructed)
    yearly_change_m2_2 = export_variable(input_table=yearly_change_m2_2, selected_variable='yearly-change[m2]')
    yearly_change_m2 = pd.concat([yearly_change_m2, yearly_change_m2_2])
    # Convert Unit m2 to Mm2
    yearly_change_Mm2 = yearly_change_m2.drop(columns='yearly-change[m2]').assign(**{'yearly-change[Mm2]': yearly_change_m2['yearly-change[m2]'] * 1e-06})
    # yearly-change [Mm2]
    yearly_change_Mm2 = export_variable(input_table=yearly_change_Mm2, selected_variable='yearly-change[Mm2]')

    # Product DEMAND

    # Buildings and Renovation - Product Demand Calculation
    # 
    # Objective: The submodule calculates the material need for construction and renovation (depends on the area that are constructed / renovated)
    # 
    # Main inputs: 
    # yearly constructed and renovated area [Mm2]
    # material surface per floor area [%]
    # 
    # Main outputs: 
    # material surface for the renovation and construction of buildings

    # yearly-change [Mm2]
    yearly_change_Mm2 = use_variable(input_table=yearly_change_Mm2, selected_variable='yearly-change[Mm2]')

    # New heating systems capacity
    # For constructed and renovated (existing : no need of new capacities)
    # 
    # Objective: The submodule calculates the power of individual residential and non residential heating systems to be constructed each year:
    # 
    # Main inputs: 
    # yearly floor area activity [Mm2]
    # hypothesis : heating occurs during 4 full months
    # main outputs: 
    # power of individual heating systems in residential / non-residential park [kW]

    # yearly-change [Mm2]
    yearly_change_Mm2_2 = use_variable(input_table=yearly_change_Mm2, selected_variable='yearly-change[Mm2]')


    # yearly-change [Mm2]
    yearly_change_Mm2_2 = use_variable(input_table=yearly_change_Mm2_2, selected_variable='yearly-change[Mm2]')
    # keep area-type = renovated
    yearly_change_Mm2_3 = yearly_change_Mm2_2.loc[yearly_change_Mm2_2['area-type'].isin(['renovated'])].copy()
    # Group by  Country, Years, building-type, building-use, end-use
    yearly_change_Mm2_2 = group_by_dimensions(df=yearly_change_Mm2_3, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type'], aggregation_method='Sum')
    # keep renovation-category = dep
    yearly_change_Mm2_3 = yearly_change_Mm2_3.loc[yearly_change_Mm2_3['renovation-category'].isin(['dep'])].copy()
    # Group by  Country, Years, building-type, building-use, end-use
    yearly_change_Mm2_3 = group_by_dimensions(df=yearly_change_Mm2_3, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type'], aggregation_method='Sum')
    # deep-renovation[m2/m2] = yearly-change[Mm2] (renovated, dep) * energy-need (renovated, all)
    deep_renovation_m2_per_m2 = mcd(input_table_1=yearly_change_Mm2_3, input_table_2=yearly_change_Mm2_2, operation_selection='x / y', output_name='deep-renovation[m2/m2]')
    # If no renovation, we've just divided by 0.
    deep_renovation_m2_per_m2 = missing_value(df=deep_renovation_m2_per_m2, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # RCP energy-need [kWh/m2]
    energy_need_kWh_per_m2 = import_data(trigram='bld', variable_name='energy-need', variable_type='RCP')
    # to floor-area-yearly [Mm2]
    out_9473_1 = yearly_change_Mm2.rename(columns={'yearly-change[Mm2]': 'floor-area-yearly[Mm2]'})
    # Mm2 to m2 (*1.000.000)
    floor_area_yearly_m2 = out_9473_1.drop(columns='floor-area-yearly[Mm2]').assign(**{'floor-area-yearly[m2]': out_9473_1['floor-area-yearly[Mm2]'] * 1000000.0})
    # Group by  Country, Years, area-type, building-type (sum)
    floor_area_yearly_m2_2 = group_by_dimensions(df=floor_area_yearly_m2, groupby_dimensions=['Country', 'Years', 'building-type', 'renovation-category', 'area-type'], aggregation_method='Sum')
    out_8495_1 = pd.concat([new_appliances_num, floor_area_yearly_m2_2])

    # Pivot

    # on building-type area-type
    out_8212_1, _, _ = pivoting(df=floor_area_yearly_m2, agg_dict={'floor-area-yearly[m2]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['building-type', 'area-type'])
    out_8213_1 = missing_value_column_filter(df=out_8212_1, missing_threshold=0.9, type_of_pattern='Manual')
    out_8213_1 = column_rename_regex(df=out_8213_1, search_string='(.*)\\+(.*)(\\[.*)', replace_string='$2_$1$3')

    def helper_8079(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_8079_1 = helper_8079(input_table_1=out_8078_1, input_table_2=out_8213_1)
    # Keep only renovation and construction
    out_8079_1 = column_filter(df=out_8079_1, pattern='^Country$|^Years$|^.*reno.*$|^.*new.*$')
    out_8233_1 = joiner(df_left=out_8079_1, df_right=out_8232_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Mm2 to m2 (*1.000.000)
    yearly_change_m2 = yearly_change_Mm2.drop(columns='yearly-change[Mm2]').assign(**{'yearly-change[m2]': yearly_change_Mm2['yearly-change[Mm2]'] * 1000000.0})

    # Building envelopes :
    # Consts computed on : renovated and constructed stock (Mm2)
    # Note : Windows, roof and walls : not in the actual scope

    # RCP costs-for-building [MEUR/m2] From TECH
    costs_for_building_MEUR_per_m2 = import_data(trigram='tec', variable_name='costs-for-building', variable_type='RCP')
    # Compute capex for yearly-change[m2]
    out_9310_1 = compute_costs(df_activity=yearly_change_m2, df_unit_costs=costs_for_building_MEUR_per_m2, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='yearly-change[m2]')
    # RCP building-cost-user [-]
    building_cost_user_ = import_data(trigram='bld', variable_name='building-cost-user', variable_type='RCP')
    # Get cost-users capex[MEUR] = capex[MEUR] * building-cost-user[-]
    capex_MEUR = mcd(input_table_1=out_9310_1, input_table_2=building_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by Country, Years, building-type, area-type (sum)
    capex_MEUR_2 = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'building-type', 'area-type'], aggregation_method='Sum')
    # capex-by-area-type[MEUR]
    out_9323_1 = capex_MEUR_2.rename(columns={'capex[MEUR]': 'capex-by-area-type[MEUR]'})
    #  buidling-stock[m2] (replace) =  buidling-stock[m2] * construction-epc-mix [%]
    building_stock_m2_3 = mcd(input_table_1=building_stock_m2_3, input_table_2=construction_epc_mix_percent, operation_selection='x * y', output_name='building-stock[m2]')
    # buidling-stock [m2] (constructed)
    building_stock_m2_3 = export_variable(input_table=building_stock_m2_3, selected_variable='building-stock[m2]')
    building_stock_m2 = pd.concat([building_stock_m2, building_stock_m2_3])
    building_stock_m2 = pd.concat([building_stock_m2_2, building_stock_m2])
    # Convert Unit m2 to Mm2
    building_stock_Mm2 = building_stock_m2.drop(columns='building-stock[m2]').assign(**{'building-stock[Mm2]': building_stock_m2['building-stock[m2]'] * 1e-06})
    # buidling-stock [Mm2]
    building_stock_Mm2 = export_variable(input_table=building_stock_Mm2, selected_variable='building-stock[Mm2]')

    # KPI's (require extra computation)

    # building-stock [Mm2]
    building_stock_Mm2_2 = use_variable(input_table=building_stock_Mm2, selected_variable='building-stock[Mm2]')

    # Average floor area per capita 
    # m²/cap

    # building-stock  [Mm2]
    building_stock_Mm2 = use_variable(input_table=building_stock_Mm2_2, selected_variable='building-stock[Mm2]')
    building_stock_Mm2_3 = building_stock_Mm2.loc[~building_stock_Mm2['building-use'].isin(['offices-all'])].copy()
    # Group by  Country, Years (SUM)
    building_stock_Mm2_3 = group_by_dimensions(df=building_stock_Mm2_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Convert Unit Mm2 to m2
    building_stock_m2 = building_stock_Mm2_3.drop(columns='building-stock[Mm2]').assign(**{'building-stock[m2]': building_stock_Mm2_3['building-stock[Mm2]'] * 1000000.0})

    # Renovated area (cumulutaed per year)
    # => Total/residential/services

    # Group by  Country, Years, building-type, area-type (SUM)
    building_stock_Mm2_3 = group_by_dimensions(df=building_stock_Mm2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'renovation-category', 'area-type'], aggregation_method='Sum')
    # keep  renovated
    building_stock_Mm2_3 = building_stock_Mm2_3.loc[building_stock_Mm2_3['area-type'].isin(['renovated'])].copy()
    # Group by  Country, Years, building-type, area-type (SUM)
    building_stock_Mm2_3 = group_by_dimensions(df=building_stock_Mm2_3, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # renovated area
    out_9258_1 = building_stock_Mm2_3.rename(columns={'building-stock[Mm2]': 'renovated-area-by-type[Mm2]'})

    # Renovated share
    # Share of total buildings stock (%)
    # per renovation category, per building-type

    # Group by  Country, Years, building-type, area-type (SUM)
    building_stock_Mm2_4 = group_by_dimensions(df=building_stock_Mm2_2, groupby_dimensions=['Country', 'Years', 'building-type', 'area-type'], aggregation_method='Sum')
    # Group by  Country, Years, building-type, area-type (SUM)
    building_stock_Mm2_3 = group_by_dimensions(df=building_stock_Mm2_4, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # keep  renovated
    building_stock_Mm2_4 = building_stock_Mm2_4.loc[building_stock_Mm2_4['area-type'].isin(['renovated'])].copy()
    # Group by  Country, Years, aera-type (SUM)
    building_stock_Mm2_5 = group_by_dimensions(df=building_stock_Mm2_4, groupby_dimensions=['Country', 'Years', 'area-type'], aggregation_method='Sum')
    # renovated share[%] = renovated-area[Mm2] / total-area[Mm2]
    renovated_share_percent = mcd(input_table_1=building_stock_Mm2_5, input_table_2=building_stock_Mm2_3, operation_selection='x / y', output_name='renovated-share[%]')
    # renovated share-by-category[%] = renovated-area-by-category[Mm2] / total-area[Mm2]
    renovated_share_by_category_percent = mcd(input_table_1=building_stock_Mm2_4, input_table_2=building_stock_Mm2_3, operation_selection='x / y', output_name='renovated-share-by-category[%]')
    renovated_share_percent = pd.concat([renovated_share_percent, renovated_share_by_category_percent])

    # Renovation depth 
    # Average final energy consumption for space heating [kWh/m²]

    # Group by  Country, Years, building-type, area-type (SUM)
    building_stock_Mm2_5 = group_by_dimensions(df=building_stock_Mm2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    building_stock_Mm2_3 = building_stock_Mm2_2.loc[~building_stock_Mm2_2['area-type'].isin(['unoccupied'])].copy()

    # Energy need : differents end-use linked to m2 (lighting / others / cooling) (residential & non-residential) + (cooking / hotwater for non-residential) 
    # => Energy-demand = cooled-area[Mm2] * energy-need-by-m2[kWh/m2]
    # => Note : for cooling : energy-demand is recomputed to take into account the cooled-penetration-rate
    #       => Cooled-area[Mm2] = floor-area-demand[Mm2] * cooled-penetration-rate[%]

    # Group by  all dimensions except epc-category  (sum)
    building_stock_Mm2_4 = group_by_dimensions(df=building_stock_Mm2_3, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type'], aggregation_method='Sum')

    # Apply energy-need (by m2) levers (reduce)
    # => determine the energy consumption for cooling by m2

    # OTS/FTS energy-need-by-m2 [kWh / m2 year]
    energy_need_by_m2_kWh__per__m2_year = import_data(trigram='bld', variable_name='energy-need-by-m2')
    # Remove end-use = cooling
    energy_need_by_m2_kWh__per__m2_year_excluded = energy_need_by_m2_kWh__per__m2_year.loc[energy_need_by_m2_kWh__per__m2_year['end-use'].isin(['cooling'])].copy()
    energy_need_by_m2_kWh__per__m2_year = energy_need_by_m2_kWh__per__m2_year.loc[~energy_need_by_m2_kWh__per__m2_year['end-use'].isin(['cooling'])].copy()
    # energy-demand[GWh] = building-stock[Mm2] * energy-need-by-m2[kWh/m2]
    energy_demand_GWh_3 = mcd(input_table_1=building_stock_Mm2_4, input_table_2=energy_need_by_m2_kWh__per__m2_year, operation_selection='x * y', output_name='energy-demand[GWh]')

    # Apply cooled-penetration-rate levers (reduce ?)
    # => determine the % of m2 that need to be cooled

    # OTS/FTS cooled-penetration-rate [%]
    building_cooled_penetration_rate_percent = import_data(trigram='bld', variable_name='building-cooled-penetration-rate')
    # cooled-area[Mm2] = building-stock[Mm2] * cooled-penetration-rate[%]
    cooled_area_Mm2 = mcd(input_table_1=building_stock_Mm2_4, input_table_2=building_cooled_penetration_rate_percent, operation_selection='x * y', output_name='cooled-area[Mm2]')
    # to floor-area-acc [Mm2]
    out_9474_1 = building_stock_Mm2_2.rename(columns={'building-stock[Mm2]': 'floor-area-acc[Mm2]'})

    # For : Pathway Explorer
    # => Area-accumulated (distinction between constructed / renovated / existing) by buidling-type

    # Group by  Country, Years, building-type, area-type (SUM)
    out_9474_1 = group_by_dimensions(df=out_9474_1, groupby_dimensions=['Country', 'Years', 'building-type', 'area-type'], aggregation_method='Sum')

    # For : Pathway Explorer
    # => Area-accumulated (distinction between EPC label) by buidling-type (residential and non-residential)

    # Keep Years >= baseyear
    building_stock_Mm2, _ = filter_dimension(df=building_stock_Mm2, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # to epc-floor-area-acc [Mm2]
    out_9512_1 = building_stock_Mm2.rename(columns={'building-stock[Mm2]': 'epc-floor-area-acc[Mm2]'})
    # Group by  Country, Years, building-type, epc-category (SUM)
    out_9512_1 = group_by_dimensions(df=out_9512_1, groupby_dimensions=['Country', 'Years', 'building-type', 'epc-category'], aggregation_method='Sum')
    out_1 = pd.concat([out_9474_1, out_9512_1])
    # appliance-use [h]
    appliance_use_h = use_variable(input_table=lifestyle, selected_variable='appliance-use[h]')

    # Apply energy-need (appliances) levers (reduce)
    # => determine the performance of each appliances (how much energy consumption is made by hour of use)

    # OTS/FTS energy-need-appliances [kWh/h]
    energy_need_appliances_kWh_per_h = import_data(trigram='bld', variable_name='energy-need-appliances')
    # energy-demand[kWh] = appliance-use[h] * appliance-energy-need[kWh/h]
    energy_demand_kWh = mcd(input_table_1=appliance_use_h, input_table_2=energy_need_appliances_kWh_per_h, operation_selection='x * y', output_name='energy-demand[kWh]')
    # kWh to GWh (*0.000001)
    energy_demand_GWh_2 = energy_demand_kWh.drop(columns='energy-demand[kWh]').assign(**{'energy-demand[GWh]': energy_demand_kWh['energy-demand[kWh]'] * 1e-06})
    # energy-demand[GWh] (replace) = energy-demand[GWh] * household-share[%]
    energy_demand_GWh_2 = mcd(input_table_1=energy_demand_GWh_2, input_table_2=household_share_percent, operation_selection='x * y', output_name='energy-demand[GWh]')
    # Group by Country, Years, building-type, building-use, end-use
    energy_demand_GWh_2 = group_by_dimensions(df=energy_demand_GWh_2, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use', 'building-use'], aggregation_method='Sum')

    # Add ventilation :
    # Set to 1

    # heating-cooling-behaviour-index [-]
    heating_cooling_behaviour_index = use_variable(input_table=lifestyle, selected_variable='heating-cooling-behaviour-index[-]')
    # Group by  Years, Country
    heating_cooling_behaviour_index_2 = group_by_dimensions(df=heating_cooling_behaviour_index, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # end-use = ventilation
    heating_cooling_behaviour_index_2['end-use'] = "ventilation"
    # Set value to 1
    heating_cooling_behaviour_index_2['heating-cooling-behaviour-index[-]'] = 1.0
    # Node 7821
    heating_cooling_behaviour_index = pd.concat([heating_cooling_behaviour_index, heating_cooling_behaviour_index_2])
    # energy-need-by-m2[kWh/m2] (replace) = energy-need-by-m2[kWh/m2] * heating-cooling-behaviour-index[-]  LEFT OUTER JOIN If behaviour-index missing, set to 1 (no change in energy-need)
    energy_need_by_m2_kWh_per_m2 = mcd(input_table_1=energy_need_by_m2_kWh__per__m2_year_excluded, input_table_2=heating_cooling_behaviour_index, operation_selection='x * y', output_name='energy-need-by-m2[kWh/m2]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # energy-demand[GWh] = cooled-area[Mm2] * energy-need-by-m2[kWh/m2]
    energy_demand_GWh_4 = mcd(input_table_1=cooled_area_Mm2, input_table_2=energy_need_by_m2_kWh_per_m2, operation_selection='x * y', output_name='energy-demand[GWh]')
    # Join cooling and other end-use
    energy_demand_GWh_3 = pd.concat([energy_demand_GWh_3, energy_demand_GWh_4])

    # Energy need : heating and ventilation (residential and non-residential)
    # => For heating : Add energy-need for renovated buildings = energy need (residential) * energy-achieved
    # => For heating / cooling : apply heating-cooling behaviour index on energy-need
    # => For cooling : apply cooling penetration rate on floor-area-demand
    # => Energy-demand = floor-area-acc[Mm2] * energy-need[kWh/m2]

    # energy-need[kWh/m2] (replace) = energy-need[kWh/m2] * heating-cooling-behaviour-index[-]  LEFT OUTER JOIN If behaviour-index missing, set to 1 (no change in energy-need)
    energy_need_kWh_per_m2 = mcd(input_table_1=heating_cooling_behaviour_index, input_table_2=energy_need_kWh_per_m2, operation_selection='x * y', output_name='energy-need[kWh/m2]', fill_value_bool='Inner Join')
    # energy-demand[GWh] = building-stock[Mm2] * energy-need[kWh/m2]
    energy_demand_GWh_4 = mcd(input_table_1=building_stock_Mm2_3, input_table_2=energy_need_kWh_per_m2, operation_selection='x * y', output_name='energy-demand[GWh]')
    # Group by  Country, Years, building-type, building-use, end-use
    energy_demand_GWh_4 = group_by_dimensions(df=energy_demand_GWh_4, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type', 'end-use'], aggregation_method='Sum')
    energy_demand_GWh_3 = pd.concat([energy_demand_GWh_3, energy_demand_GWh_4])
    energy_demand_GWh = pd.concat([energy_demand_GWh, energy_demand_GWh_3])
    energy_demand_GWh = pd.concat([energy_demand_GWh_2, energy_demand_GWh])
    # If missing (string) set to "" (renovation-category)
    energy_demand_GWh = missing_value(df=energy_demand_GWh, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    energy_demand_GWh = energy_demand_GWh.loc[~energy_demand_GWh['building-use'].isin(['offices-private', 'offices-public'])].copy()
    # Export energy-demand [GWh]
    energy_demand_GWh = export_variable(input_table=energy_demand_GWh, selected_variable='energy-demand[GWh]')

    # District heating / hotwater
    # => A part of the energy-demand for heating and hotwater can be furnished by district heating
    # => district-energy-demand[GWh] = energy-demand[GWh] * distring-heating-share[%]
    # => energy-demand[GWh] (remaining after district heating) = energy-demand[GWh] - district-energy-demand[GWh]

    # Energy-demand [GWh]
    energy_demand_GWh = use_variable(input_table=energy_demand_GWh, selected_variable='energy-demand[GWh]')

    # District heating / hotwater energy-demand

    # Apply district-heating-share levers (switch ?)
    # => determine the % of energy-demand (heating / cooling) which is achieved with district heating

    # OTS/FTS district-heating-share [%]
    district_heating_share_percent = import_data(trigram='bld', variable_name='district-heating-share')
    # district-energy-demand[GWh] = energy-need[GWh] * district-heating-share[%]
    district_energy_demand_GWh = mcd(input_table_1=energy_demand_GWh, input_table_2=district_heating_share_percent, operation_selection='x * y', output_name='district-energy-demand[GWh]')
    # energy-demand[GWh]  (replace) = energy-demand[GWh]  -  district-energy-demand[GWh]  LEFT OUTER JOIN If missing district-energy-need, set to 0
    energy_demand_GWh = mcd(input_table_1=energy_demand_GWh, input_table_2=district_energy_demand_GWh, operation_selection='x - y', output_name='energy-demand[GWh]', fill_value_bool='Left [x] Outer Join')
    # Export energy-demand[GWh] (without district heating)
    energy_demand_GWh = export_variable(input_table=energy_demand_GWh, selected_variable='energy-demand[GWh]')

    # Final DECENTRALISED energy demand ( = without heating / hotwater coming from District heating)
    # => Determine the energy-carrier used to reach the energy-demand

    # energy-demand [GWh] (without district-heating)
    energy_demand_GWh = use_variable(input_table=energy_demand_GWh, selected_variable='energy-demand[GWh]')

    # Recompute technology-mix to account for solid-biomass energy-carrier
    # => technology-mix = technology-mix * (1 - solid-biomass-share)

    # Apply technology-mix levers (switch)
    # => determine which energy-carrier is used to reach the energy-demand

    # OTS/FTS technology-mix [%]
    technology_mix_percent = import_data(trigram='bld', variable_name='technology-mix')

    # Apply solid-biomass-share levers (switch)
    # => determine which amount of energy-demand is provided by solid-biomass

    # OTS/FTS solid-biomass-share [%]
    solid_biomass_share_percent = import_data(trigram='bld', variable_name='solid-biomass-share')
    # energy-demand[GWh] (replace) = energy-demand[GWh] * solid-biomass-share[%]
    energy_demand_GWh_2 = mcd(input_table_1=energy_demand_GWh, input_table_2=solid_biomass_share_percent, operation_selection='x * y', output_name='energy-demand[GWh]')
    # add solid biomass  as energy-carrier
    out_7970_1 = energy_demand_GWh_2.copy()
    out_7970_1['energy-way-of-prod'] = 'solid-biomass'
    # technology-mix[%] (replace) = technology-mix[%] * (1 - solid-biomass-share[%])   Left outer join if missing value for solid-biomass-share set 0
    technology_mix_percent = mcd(input_table_1=technology_mix_percent, input_table_2=solid_biomass_share_percent, operation_selection='x * (1-y)', output_name='technology-mix[%]', fill_value_bool='Left [x] Outer Join')
    # energy-demand[GWh] (replace) = energy-demand[GWh] * technology-mix[%]
    energy_demand_GWh = mcd(input_table_1=energy_demand_GWh, input_table_2=technology_mix_percent, operation_selection='x * y', output_name='energy-demand[GWh]', fill_value_bool='Inner Join')
    out_7732_1 = pd.concat([energy_demand_GWh, out_7970_1])

    # Apply energy-efficiency levers (improve)
    # => determine which amount of energy-demand is provided by solid-biomass

    # OTS/FTS energy-efficiency [%]
    energy_efficiency_percent = import_data(trigram='bld', variable_name='energy-efficiency')
    # energy demand[GWh] = energy-demand[GWh] / energy-efficiency[%]
    energy_demand_GWh = mcd(input_table_1=out_7732_1, input_table_2=energy_efficiency_percent, operation_selection='x / y', output_name='energy-demand[GWh]', fill_value_bool='Inner Join')
    # Set 0 (when divide by 0)
    energy_demand_GWh = missing_value(df=energy_demand_GWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # GWh to TWh (*0.001)
    energy_demand_TWh = energy_demand_GWh.drop(columns='energy-demand[GWh]').assign(**{'energy-demand[TWh]': energy_demand_GWh['energy-demand[GWh]'] * 0.001})
    # energy demand [TWh] (before calibration)
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # Calibration

    # energy demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # Calibration

    # Calibration energy-demand [TWh]
    energy_demand_TWh_2 = import_data(trigram='bld', variable_name='energy-demand', variable_type='Calibration')
    # Advenced row filter
    energy_demand_TWh_3 = energy_demand_TWh_2.loc[energy_demand_TWh_2['building-type'].isin(['heat-district'])].copy()
    # Group by  Country, Years, end-use, building-type, energy-carrier
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Years', 'Country', 'end-use'], aggregation_method='Sum')
    # Add by end-use to variable
    out_9344_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'district-energy-demand[TWh]'})
    # Convert Unit GWh to TWh
    district_energy_demand_GWh_2 = out_9344_1.drop(columns='district-energy-demand[TWh]').assign(**{'district-energy-demand[GWh]': out_9344_1['district-energy-demand[TWh]'] * 1000.0})
    # Group by  Country, Years, end-use, building-type, energy-carrier
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use', 'energy-carrier'], aggregation_method='Sum')
    # Apply Calibration on energy-demand[TWh]
    _, out_7751_2, out_7751_3 = calibration(input_table=energy_demand_TWh_3, cal_table=energy_demand_TWh_2, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand[TWh]')

    # Calibration RATES

    # Cal_rate for energy-demand[TWh]

    # cal_rate for energy-demand
    cal_rate_energy_demand_TWh = use_variable(input_table=out_7751_3, selected_variable='cal_rate_energy-demand[TWh]')
    # apply cal-rate to energy demand[TWh]
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=out_7751_2, operation_selection='x * y', output_name='energy-demand[TWh]')
    # energy demand [TWh] (after calibration before fuel switch)
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh]  (calibrated)
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # Apply fuel-switch levers (switch)
    # => switch one fuel to another one (usually less GHG emitter)

    # fuel-switch [%]
    fuel_switch_percent = import_data(trigram='bld', variable_name='fuel-switch')
    # Fuel switch ffuels to biofuels
    out_0_1 = x_switch(demand_table=energy_demand_TWh_2, switch_table=fuel_switch_percent, correlation_table=ratio)
    # Fuel switch ffuels to hydrogen
    out_8468_1 = x_switch(demand_table=out_0_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='hydrogen')
    # Fuel switch ffuels to synfuels
    out_8469_1 = x_switch(demand_table=out_8468_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='synfuels')
    # Export energy-demand [TWh] (calibrated and after fuel switch)
    energy_demand_TWh = export_variable(input_table=out_8469_1, selected_variable='energy-demand[TWh]')

    # Emissions
    # => Emissions = energy-demand x emission-factor

    # energy-demand [TWh]  (calibrated and after fuel switch)
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # RCP bld-emission-factor [g/kWh]
    bld_emission_factor_g_per_kWh = import_data(trigram='bld', variable_name='bld-emission-factor', variable_type='RCP')
    # emissions[kt] = energy-demand[TWh] * bld-emission-factor[g/kWh]
    emissions_kt = mcd(input_table_1=energy_demand_TWh, input_table_2=bld_emission_factor_g_per_kWh, operation_selection='x * y', output_name='emissions[kt]')

    # Calibration - emissions

    # Group by  Years, Country, building-type, gaes (SUM)
    emissions_kt_2 = group_by_dimensions(df=emissions_kt, groupby_dimensions=['gaes', 'building-type', 'Country', 'Years'], aggregation_method='Sum')
    # Calibration emissions [kt]
    emissions_kt_3 = import_data(trigram='bld', variable_name='emissions', variable_type='Calibration')
    # Apply Calibration on emissions[kt]
    _, out_7979_2, out_7979_3 = calibration(input_table=emissions_kt_2, cal_table=emissions_kt_3, data_to_be_cal='emissions[kt]', data_cal='emissions[kt]')

    # Cal_rate for emissions[kt]

    # cal-rate for emissions
    cal_rate_emissions_kt = use_variable(input_table=out_7979_3, selected_variable='cal_rate_emissions[kt]')
    # apply cal-rate on emissions[kt]
    emissions_kt = mcd(input_table_1=emissions_kt, input_table_2=out_7979_2, operation_selection='x * y', output_name='emissions[kt]')
    # kt to Mt (*0.001)
    emissions_Mt = emissions_kt.drop(columns='emissions[kt]').assign(**{'emissions[Mt]': emissions_kt['emissions[kt]'] * 0.001})
    # Export emissions[Mt]
    emissions_Mt = export_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # For : Climate emission

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Group by  Country, Years, end-use,  gaes, building-type energy-carrier (SUM)
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'end-use', 'building-type', 'energy-carrier', 'gaes'], aggregation_method='Sum')
    # Add emissions-or-capture
    emissions_Mt['emissions-or-capture'] = "emissions"
    # Module = Climate Emissions
    emissions_Mt = column_filter(df=emissions_Mt, pattern='^.*$')
    # keep heating
    energy_demand_TWh_3 = energy_demand_TWh.loc[energy_demand_TWh['end-use'].isin(['heating'])].copy()
    # TWh to kW Assumption : heating during 4 full months (=122 days = 2928 hours) (1 kWh = 3.41E-4)
    energy_demand_kWh = energy_demand_TWh_3.drop(columns='energy-demand[TWh]').assign(**{'energy-demand[kWh]': energy_demand_TWh_3['energy-demand[TWh]'] * 1000000000.0})
    energy_demand_kWh = energy_demand_kWh.loc[~energy_demand_kWh['energy-way-of-prod'].isin(['solid-biomass'])].copy()

    # Heating systems :
    # Individual systems => Capex (new capacities) and Opex (energy demand)
    # Note : shared systems = not included here as energy-demand for district heating is separated from the rest of energy-demand

    # Group by  Country, Years, building-type, building-use, energy-way-of-prod
    energy_demand_kWh = group_by_dimensions(df=energy_demand_kWh, groupby_dimensions=['Country', 'Years', 'building-type', 'building-use', 'area-type'], aggregation_method='Sum')
    # RCP costs-for-building-energy [MEUR/kW] From TECH
    costs_for_building_energy_MEUR_per_kW = import_data(trigram='tec', variable_name='costs-for-building-energy', variable_type='RCP')
    # Compute opex for energy-demand [kW]
    out_9293_1 = compute_costs(df_activity=energy_demand_kWh, df_unit_costs=costs_for_building_energy_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='energy-demand[kWh]', cost_type='OPEX')
    # RCP building-energy-cost-user [-]
    building_energy_cost_user_ = import_data(trigram='bld', variable_name='building-energy-cost-user', variable_type='RCP')
    # Get cost-users opex[MEUR] = opex[MEUR] * building-energy-cost-user[-]
    opex_MEUR = mcd(input_table_1=out_9293_1, input_table_2=building_energy_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')

    # Costs by user
    # - Capex: Pipes, Heating Systems, Buildings enveloppe
    # - Opex: Heating Systems

    # Group by Country, Years (sum)
    opex_MEUR_2 = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    # Group by Country, Years, building-type (sum)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # opex-by-heating-system [MEUR]
    out_9327_1 = opex_MEUR.rename(columns={'capex[MEUR]': 'capex-by-heating-system[MEUR]'})
    # keep end-use = heating
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[energy_demand_TWh_2['end-use'].isin(['heating'])].copy()
    # keep area-type = constructed
    energy_demand_TWh_3 = energy_demand_TWh_2.loc[energy_demand_TWh_2['area-type'].isin(['constructed'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh_2.loc[~energy_demand_TWh_2['area-type'].isin(['constructed'])].copy()
    # keep area-type = renovated
    energy_demand_TWh_excluded = energy_demand_TWh_excluded.loc[energy_demand_TWh_excluded['area-type'].isin(['renovated'])].copy()
    # energy-demand[TWh] = energy-demand[TWh] * deep-renovation[m2/m2]
    energy_demand_TWh_2 = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=deep_renovation_m2_per_m2, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh_2 = pd.concat([energy_demand_TWh_3, energy_demand_TWh_2])
    # Group by  Country, Years, building-type, building-use, end-use, energy-way-of-prod, energy-carrier
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use', 'building-use', 'energy-way-of-prod', 'energy-carrier'], aggregation_method='Sum')
    # TWh to TW Assumption : heating during 4 full months (=122 days = 2928 hours) (1 kWh = 3.41E-4)
    energy_demand_TW = energy_demand_TWh_2.drop(columns='energy-demand[TWh]').assign(**{'energy-demand[TW]': energy_demand_TWh_2['energy-demand[TWh]'] * 0.000341})
    # TW to kW (*10^6)
    energy_demand_kW = energy_demand_TW.drop(columns='energy-demand[TW]').assign(**{'energy-demand[kW]': energy_demand_TW['energy-demand[TW]'] * 1000000000.0})
    # new-heating-capacity [kW]
    out_9493_1 = energy_demand_kW.rename(columns={'energy-demand[kW]': 'new-heating-capacity[kW]'})
    # new-heating-capacity [kW]
    new_heating_capacity_kW = export_variable(input_table=out_9493_1, selected_variable='new-heating-capacity[kW]')
    # new-heating-capacity [kW]
    new_heating_capacity_kW = use_variable(input_table=new_heating_capacity_kW, selected_variable='new-heating-capacity[kW]')
    new_heating_capacity_kW = new_heating_capacity_kW.loc[~new_heating_capacity_kW['energy-way-of-prod'].isin(['solid-biomass'])].copy()
    # Compute capex for new-heating-capacity[kW]
    out_9299_1 = compute_costs(df_activity=new_heating_capacity_kW, df_unit_costs=costs_for_building_energy_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='new-heating-capacity[kW]')
    # Get cost-users capex[MEUR] = capex[MEUR] * building-energy-cost-user[-]
    capex_MEUR_3 = mcd(input_table_1=out_9299_1, input_table_2=building_energy_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by Country, Years, energy-way-of-prod (sum)
    capex_MEUR_2 = group_by_dimensions(df=capex_MEUR_3, groupby_dimensions=['Country', 'Years', 'energy-way-of-prod'], aggregation_method='Sum')
    # capex-by-energy-carrier [MEUR]
    out_9330_1 = capex_MEUR_2.rename(columns={'capex[MEUR]': 'capex-by-energy-carrier[MEUR]'})
    out_1_2 = pd.concat([out_9330_1, out_9323_1])
    # Group by Country, Years, building-type (sum)
    capex_MEUR_2 = group_by_dimensions(df=capex_MEUR_3, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # capex-by-heating-system [MEUR]
    out_9328_1 = capex_MEUR_2.rename(columns={'capex[MEUR]': 'capex-by-heating-system[MEUR]'})
    out_1_2 = pd.concat([out_9328_1, out_1_2])
    out_1_2 = pd.concat([out_9327_1, out_1_2])
    # Export district-energy-demand [GWh] (before calibration)
    district_energy_demand_GWh = export_variable(input_table=district_energy_demand_GWh, selected_variable='district-energy-demand[GWh]')
    # district-energy-demand [GWh]
    district_energy_demand_GWh = use_variable(input_table=district_energy_demand_GWh, selected_variable='district-energy-demand[GWh]')
    # Group by  Country, Years, end-use, building-type, energy-carrier
    district_energy_demand_GWh_3 = group_by_dimensions(df=district_energy_demand_GWh, groupby_dimensions=['Country', 'Years', 'end-use'], aggregation_method='Sum')
    # Apply Calibration on energy-demand[GWh]
    _, out_9347_2, out_9347_3 = calibration(input_table=district_energy_demand_GWh_3, cal_table=district_energy_demand_GWh_2, data_to_be_cal='district-energy-demand[GWh]', data_cal='district-energy-demand[GWh]')
    # apply cal-rate to district-energy-demand[TWh]
    district_energy_demand_GWh = mcd(input_table_1=out_9347_2, input_table_2=district_energy_demand_GWh, operation_selection='x * y', output_name='district-energy-demand[GWh]')
    # districti-energy-demand [GWh] (after calibration)
    district_energy_demand_GWh = export_variable(input_table=district_energy_demand_GWh, selected_variable='district-energy-demand[GWh]')

    # Pipes (for district heating)
    # constructed each year
    # 
    # Objective: The submodule calculates the km of district heating pipes to be constructed each year:
    # 1) due to renovation
    # 2) due to increase of demand
    # 
    # Main inputs: 
    # energy need district heating [GWh] => only heating is kept (no need to account for hotwater as it uses same pipes than heating)
    # pipes need per energy supplied [m/MWh] => Hypothesis : km of pipes = 0.33 * energy need district heating [GWh]
    # 
    # main outputs: 
    # new construction of pipes[km]

    # district-energy-demand [GWh]
    district_energy_demand_GWh = use_variable(input_table=district_energy_demand_GWh, selected_variable='district-energy-demand[GWh]')
    # keep end-use = heating
    district_energy_demand_GWh_2 = district_energy_demand_GWh.loc[district_energy_demand_GWh['end-use'].isin(['heating'])].copy()
    # Hypothesis pipes-length[km] = district-energy-demand[GWh] * 0.33[km/GWh]
    pipes_length_km = district_energy_demand_GWh_2.assign(**{'pipes-length[km]': district_energy_demand_GWh_2['district-energy-demand[GWh]']*0.33})
    # pipes-length [km]
    pipes_length_km = use_variable(input_table=pipes_length_km, selected_variable='pipes-length[km]')
    # Group by Country, Years (SUM)
    pipes_length_km = group_by_dimensions(df=pipes_length_km, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # pipes-overtimestep[km]
    # => determine new need of pipes to reach the pipes demand (for district heating demand)

    # Lag variable pipes-length [km]
    out_8196_1, _ = lag_variable(df=pipes_length_km, in_var='pipes-length[km]')
    # pipes-length-overtimestep[km] = pipes-length[km] - pipes-length-lagged[km]
    pipes_length_overtimestep_km = mcd(input_table_1=pipes_length_km, input_table_2=out_8196_1, operation_selection='x - y', output_name='pipes-length-overtimestep[km]')
    # Years <= baseyear
    pipes_length_overtimestep_km, pipes_length_overtimestep_km_excluded = filter_dimension(df=pipes_length_overtimestep_km, dimension='Years', operation_selection='≤', value_years=Globals.get().base_year)
    # Force to be = 0
    pipes_length_overtimestep_km['pipes-length-overtimestep[km]'] = 0.0
    # if < 0 => set to 0
    mask = pipes_length_overtimestep_km_excluded['pipes-length-overtimestep[km]']<0
    pipes_length_overtimestep_km_excluded.loc[mask, 'pipes-length-overtimestep[km]'] =  0
    pipes_length_overtimestep_km_excluded.loc[~mask, 'pipes-length-overtimestep[km]'] =  pipes_length_overtimestep_km_excluded.loc[~mask, 'pipes-length-overtimestep[km]']
    pipes_length_overtimestep_km = pd.concat([pipes_length_overtimestep_km, pipes_length_overtimestep_km_excluded])

    # renewal-pipes-length[km]
    # => determine new need of pipes due to renovation of pipes
    # => Hypothesis : 2% of pipes are renewed each year

    # renewal-pipes-length[km] = pipes-length[km] - pipes-length-overtimestep[km]
    renewal_pipes_length_km = mcd(input_table_1=pipes_length_km, input_table_2=pipes_length_overtimestep_km, operation_selection='x - y', output_name='renewal-pipes-length[km]', fill_value_bool='Inner Join')
    # factor 0.02
    renewal_pipes_length_km['renewal-pipes-length[km]'] = renewal_pipes_length_km['renewal-pipes-length[km]']*0.02
    # new-pipes-length[km] = renewal-pipes-length[km] + pipes-length-overtimestep[km]
    new_pipes_length_km = mcd(input_table_1=renewal_pipes_length_km, input_table_2=pipes_length_overtimestep_km, operation_selection='x + y', output_name='new-pipes-length[km]')
    # Export new-pipes-length [km]
    new_pipes_length_km = export_variable(input_table=new_pipes_length_km, selected_variable='new-pipes-length[km]')

    # For : Industry
    # => New pipes length (for district heating)

    # new-pipes-length [km]
    new_pipes_length_km = use_variable(input_table=new_pipes_length_km, selected_variable='new-pipes-length[km]')
    out_8496_1 = pd.concat([new_pipes_length_km, out_8495_1])
    # Module = Industry
    out_8496_1 = column_filter(df=out_8496_1, pattern='^.*$')
    # As it was in old module : bld_new_dhg_pipe[km]
    out_8234_1 = new_pipes_length_km.rename(columns={'new-pipes-length[km]': 'bld_new_dhg_pipe[km]'})
    out_8235_1 = joiner(df_left=out_8234_1, df_right=out_8233_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Module = Minerals
    out_8235_1 = column_filter(df=out_8235_1, pattern='^.*$')
    # Years
    out_7502_1 = out_8235_1.assign(Years=out_8235_1['Years'].astype(str))

    # Pipes
    # Only new one => Capex

    # RCP costs-by-building-infra [MEUR/km] From TECH
    costs_by_building_infra_MEUR_per_km = import_data(trigram='tec', variable_name='costs-by-building-infra', variable_type='RCP')
    # Compute capex for new-pipes-length[km]
    out_9286_1 = compute_costs(df_activity=new_pipes_length_km, df_unit_costs=costs_by_building_infra_MEUR_per_km, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='new-pipes-length[km]')
    # RCP building-infra-cost-user [-]
    building_infra_cost_user_ = import_data(trigram='bld', variable_name='building-infra-cost-user', variable_type='RCP')
    # Get cost-users capex[MEUR] = capex[MEUR] * appliances-cost-user[-]
    capex_MEUR_2 = mcd(input_table_1=out_9286_1, input_table_2=building_infra_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    capex_MEUR_3 = pd.concat([capex_MEUR_2, capex_MEUR_3])
    capex_MEUR = pd.concat([capex_MEUR_3, capex_MEUR])

    # CAPEX

    # Group by Country, Years (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    MEUR = pd.concat([capex_MEUR, opex_MEUR_2])
    # Group by Country, Years (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # capex-by-dh-pipe[MEUR]
    out_9322_1 = capex_MEUR.rename(columns={'capex[MEUR]': 'capex-by-dh-pipe[MEUR]'})
    out_1_2 = pd.concat([out_9322_1, out_1_2])
    out_1_2 = pd.concat([out_9320_1, out_1_2])
    out_9338_1 = pd.concat([MEUR, out_1_2])

    # Final energy consumption including district heating
    # => division hotwater, space heating

    # Group by  Country, Years, building-type, end-use (SUM)
    district_energy_demand_GWh_2 = group_by_dimensions(df=district_energy_demand_GWh, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use'], aggregation_method='Sum')
    # Convert Unit GWh to TWh
    district_energy_demand_TWh = district_energy_demand_GWh_2.drop(columns='district-energy-demand[GWh]').assign(**{'district-energy-demand[TWh]': district_energy_demand_GWh_2['district-energy-demand[GWh]'] * 0.001})

    # District heating share
    # => division hotwater, space heating

    # Group by  Country, Years, end-use (SUM)
    district_energy_demand_TWh_3 = group_by_dimensions(df=district_energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'end-use'], aggregation_method='Sum')

    # Final energy consumption
    # => division residential/services

    # Group by  Country, Years, building-type (SUM)
    district_energy_demand_GWh_2 = group_by_dimensions(df=district_energy_demand_GWh, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # Convert Unit GWh to TWh
    district_energy_demand_TWh_4 = district_energy_demand_GWh_2.drop(columns='district-energy-demand[GWh]').assign(**{'district-energy-demand[TWh]': district_energy_demand_GWh_2['district-energy-demand[GWh]'] * 0.001})
    # GWh to TWh (*0.001)
    district_energy_demand_TWh_2 = district_energy_demand_GWh.drop(columns='district-energy-demand[GWh]').assign(**{'district-energy-demand[TWh]': district_energy_demand_GWh['district-energy-demand[GWh]'] * 0.001})

    # For : Pathway Explorer
    # => Energy demand for district-heating

    # Group by  Country, Years, building-type (SUM)
    district_energy_demand_TWh_5 = group_by_dimensions(df=district_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    out_9184_1 = pd.concat([district_energy_demand_TWh_5, out_1])

    # For : Power supply
    # => Energy demand for district-heating

    # Group by  Country, Years (SUM)
    district_energy_demand_TWh_2 = group_by_dimensions(df=district_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # district-energy-demand[TWh] to energy-demand[TWh]
    out_8279_1 = district_energy_demand_TWh_2.rename(columns={'district-energy-demand[TWh]': 'energy-demand[TWh]'})
    out_8279_1['energy-carrier'] = "heat"

    # Cal_rate for district-energy-demand[TWh]

    # cal_rate for district-energy-demand
    cal_rate_district_energy_demand_GWh = use_variable(input_table=out_9347_3, selected_variable='cal_rate_district-energy-demand[GWh]')
    cal_rate_energy_demand = pd.concat([cal_rate_energy_demand_TWh, cal_rate_district_energy_demand_GWh])
    cal_rate = pd.concat([cal_rate_energy_demand, cal_rate_emissions_kt])
    # Module = CALIBRATION
    cal_rate = column_filter(df=cal_rate, pattern='^.*$')
    # floor-per-cap [m2/cap]
    floor_per_cap_m2_per_cap = mcd(input_table_1=building_stock_m2, input_table_2=population_cap, operation_selection='x / y', output_name='floor-per-cap[m2/cap]')
    out_9278_1 = pd.concat([building_renovation_rate_percent, floor_per_cap_m2_per_cap])
    out_1 = pd.concat([out_9258_1, out_9278_1])
    out_9250_1 = pd.concat([renovated_share_percent, out_1])
    # Hypothesis energy-demand[TWh] = population[inhabitants] * 0.00000005
    energy_demand_TWh_2 = population_cap.assign(**{'energy-demand[TWh]': population_cap['population[cap]']*0.00000005})
    # building-type = non-residential
    energy_demand_TWh_2['building-type'] = "non-residential"
    # building-use = others
    energy_demand_TWh_2['building-use'] = "others"
    # renovation-category = exi
    energy_demand_TWh_2['renovation-category'] = "exi"
    # end-use = servers
    energy_demand_TWh_2['end-use'] = "servers"
    # energy-carrier = electricity
    energy_demand_TWh_2['energy-carrier'] = "electricity"
    # energy-way-of-prod = elec-direct
    energy_demand_TWh_2['energy-way-of-prod'] = "elec-direct"
    # Export energy-demand [TWh] (for servers)
    energy_demand_TWh_2 = export_variable(input_table=energy_demand_TWh_2, selected_variable='energy-demand[TWh]')

    # Add energy demand for servers
    # 
    # => Note : in old module, servers energy demand was not used to estimate GHG emissions of building sector ;
    # should it be included here ?
    # If yes, pay attention calibration data should be adapted !

    # energy-demand [TWh] (for servers)
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh_2, selected_variable='energy-demand[TWh]')
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_2])
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Advenced row filter
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['end-use'].isin(['hotwater', 'heating'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh.loc[~energy_demand_TWh['end-use'].isin(['hotwater', 'heating'])].copy()

    # Ambiant share
    # => total final energy consumption

    # Group by  Country, Years (SUM)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Group by  Country, Years, building-type, end-use (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use'], aggregation_method='Sum')
    # energy-demand-incl-dh[TWh] decentralized energy [TWh]  + district heating TWh]
    energy_demand_incl_district_heating_TWh = mcd(input_table_1=energy_demand_TWh_2, input_table_2=district_energy_demand_TWh, operation_selection='x + y', output_name='energy-demand-incl-district-heating[TWh]')
    # Group by  Country, Years (SUM)
    energy_demand_incl_district_heating_TWh_2 = group_by_dimensions(df=energy_demand_incl_district_heating_TWh, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # total-energy-incl-dh[TWh]
    total_energy_incl_dh_TWh = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=energy_demand_incl_district_heating_TWh_2, operation_selection='x + y', output_name='total-energy-incl-dh[TWh]')
    # keep space heating
    energy_demand_incl_district_heating_TWh_2 = energy_demand_incl_district_heating_TWh.loc[energy_demand_incl_district_heating_TWh['end-use'].isin(['heating'])].copy()
    # Group by  Country, Years (SUM)
    energy_demand_incl_district_heating_TWh_2 = group_by_dimensions(df=energy_demand_incl_district_heating_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Convert Unit Gpkm to pkm
    energy_demand_incl_district_heating_GWh = energy_demand_incl_district_heating_TWh_2.drop(columns='energy-demand-incl-district-heating[TWh]').assign(**{'energy-demand-incl-district-heating[GWh]': energy_demand_incl_district_heating_TWh_2['energy-demand-incl-district-heating[TWh]'] * 1000.0})
    # renovation depth [kWh/m²] = energy-space-heating[GWh] / Surface area [Mm²]
    renovation_depth_kWh_per_m2 = mcd(input_table_1=energy_demand_incl_district_heating_GWh, input_table_2=building_stock_Mm2_5, operation_selection='x / y', output_name='renovation-depth[kWh/m2]')
    # Group by  Country, Years, end-use (SUM)
    energy_demand_incl_district_heating_TWh_2 = group_by_dimensions(df=energy_demand_incl_district_heating_TWh, groupby_dimensions=['Country', 'Years', 'end-use'], aggregation_method='Sum')
    # dh-share[%] = energy-demand-dh[TWh]/ energy-demand-incl-dh[TWh]
    district_heating_share_by_end_use_percent = mcd(input_table_1=district_energy_demand_TWh_3, input_table_2=energy_demand_incl_district_heating_TWh_2, operation_selection='x / y', output_name='district-heating-share-by-end-use[%]')
    # keep  electricity
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['electricity'])].copy()

    # Electricity share
    # => total final energy consumption

    # Group by  Country, Years (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # electricity-share[%]
    electricity_share_percent = mcd(input_table_1=energy_demand_TWh_2, input_table_2=total_energy_incl_dh_TWh, operation_selection='x / y', output_name='electricity-share[%]')
    # keep  ambiant
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['ambiant'])].copy()
    # Group by  Country, Years (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # ambiant-share[%]
    ambiant_share_percent = mcd(input_table_1=energy_demand_TWh_2, input_table_2=total_energy_incl_dh_TWh, operation_selection='x / y', output_name='ambiant-share[%]')
    share_percent = pd.concat([electricity_share_percent, ambiant_share_percent])
    share_percent = pd.concat([district_heating_share_by_end_use_percent, share_percent])

    # Final energy consumption
    # => per end-use, per vector, and per residential/services (full granularity)

    # Group by  Country, Years, building-type, area-type (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'building-type', 'end-use', 'energy-carrier'], aggregation_method='Sum')
    out_9340_1 = pd.concat([share_percent, energy_demand_TWh_2])
    # Group by  Country, Years, building-type (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # energy-demand-by-type[TWh] decentralized energy [TWh]  + district heating TWh]
    energy_demand_by_type_TWh = mcd(input_table_1=energy_demand_TWh_2, input_table_2=district_energy_demand_TWh_4, operation_selection='x + y', output_name='energy-demand-by-type[TWh]')
    energy_demand_TWh_2 = pd.concat([energy_demand_by_type_TWh, energy_demand_incl_district_heating_TWh])
    out_9223_1 = pd.concat([energy_demand_TWh_2, out_9340_1])
    out_9251_1 = pd.concat([out_9223_1, renovation_depth_kWh_per_m2])
    out_1 = pd.concat([out_9251_1, out_9250_1])

    # For : Power supply
    # => Energy demand by vector

    # Group by  Country, Years, energy-carrier (SUM)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    out_8498_1 = pd.concat([out_8279_1, energy_demand_TWh_2])
    # Module = Power supply
    out_8498_1 = column_filter(df=out_8498_1, pattern='^.*$')
    # Module = Air Quality
    energy_demand_TWh_3 = column_filter(df=energy_demand_TWh, pattern='^.*$')

    # For : Pathway Explorer
    # => Energy demand by end-use & building-type + by vector

    # Group by  Country, Years, building-type, end-use (SUM)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'end-use', 'building-type'], aggregation_method='Sum')
    # Add by end-use to variable
    out_9188_1 = energy_demand_TWh.rename(columns={'energy-demand[TWh]': 'energy-demand-by-end-use[TWh]'})
    # Add by end-use to vector
    out_9189_1 = energy_demand_TWh_2.rename(columns={'energy-demand[TWh]': 'energy-demand-by-vector[TWh]'})
    out_1_2 = pd.concat([out_9188_1, out_9189_1])
    out_1_2 = pd.concat([out_1_2, out_9184_1])
    # Add  KPIs
    out_1 = pd.concat([out_1_2, out_1])
    # Add  Costs
    out_1 = pd.concat([out_1, out_9338_1])
    out_9191_1 = add_trigram(module_name=module_name, df=out_1)
    # Module = Pathway Explorer
    out_9191_1 = column_filter(df=out_9191_1, pattern='^.*$')

    return out_9191_1, cal_rate, out_8496_1, out_8498_1, emissions_Mt, energy_demand_TWh_3, out_7502_1


