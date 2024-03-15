import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *

# Lifestyle module
def lifestyle():

    # 0. Module parameters
    #-------------------
    module_name = 'lifestyle'


    # 1. Population
    #---------------

    # Population [cap]
    population_cap = import_data(trigram='lfs', variable_name='population') # OTS/FTS
    population_cap = group_by_dimensions(df=population_cap, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')


    # 2. Buildings
    #-------------
    output_bld = building(pop=population_cap)
    
    # 3. Transport
    #-------------
    output_tra = transport(pop=population_cap)

    # 4. Industry
    #------------
    output_ind = industry(pop=population_cap)

    # 5. AFOLU
    #---------
    output_afo = afolu(pop=population_cap)

    # 6. Supply
    #----------
    
    # 7. Formating
    #-------------

    ## For other modules
    output_wat = column_filter(df=population_cap, pattern='^.*$')
    
    ## For interface
    ### Food waste
    food_waste_kcal = use_variable(input_table=output_afo, selected_variable='food-waste[kcal]')
    food_waste_kcal_grouped = group_by_dimensions(df=food_waste_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    food_waste_per_cap_kcal_per_cap = mcd(input_table_1=food_waste_kcal_grouped, input_table_2=population_cap, operation_selection='x / y', output_name='food-waste-per-cap[kcal/cap]')
    food_waste_per_cap_kcal_per_cap_per_day = food_waste_per_cap_kcal_per_cap.drop(columns='food-waste-per-cap[kcal/cap]').assign(**{'food-waste-per-cap[kcal/cap/day]': food_waste_per_cap_kcal_per_cap['food-waste-per-cap[kcal/cap]'] * 0.00274})
    ### Food demand
    food_demand_kcal = use_variable(input_table=output_afo, selected_variable='food-demand[kcal]')
    food_demand_kcal_2 = group_by_dimensions(df=food_demand_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    food_demand_per_cap_kcal_per_cap = mcd(input_table_1=food_demand_kcal_2, input_table_2=population_cap, operation_selection='x / y', output_name='food-demand-per-cap[kcal/cap]')
    food_demand_per_cap_kcal_per_cap_per_day = food_demand_per_cap_kcal_per_cap.drop(columns='food-demand-per-cap[kcal/cap]').assign(**{'food-demand-per-cap[kcal/cap/day]': food_demand_per_cap_kcal_per_cap['food-demand-per-cap[kcal/cap]'] * 0.00274})
    ### Houshold number
    households_total_num = use_variable(input_table=output_bld, selected_variable='households-total[num]')

    concat = pd.concat([food_demand_kcal, population_cap.set_index(population_cap.index.astype(str) + '_dup')])
    concat = pd.concat([concat, food_waste_kcal.set_index(food_waste_kcal.index.astype(str) + '_dup')])
    concat = pd.concat([concat, food_demand_per_cap_kcal_per_cap_per_day.set_index(food_demand_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    concat = pd.concat([concat, food_waste_per_cap_kcal_per_cap_per_day.set_index(food_waste_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    concat = pd.concat([concat, households_total_num.set_index(households_total_num.index.astype(str) + '_dup')])
    output_interface = add_trigram(module_name=module_name, df=concat)
    output_interface = column_filter(df=output_interface, pattern='^.*$')

    # Calibration RATES
    cal_rate = use_variable(input_table=output_afo, selected_variable='cal_rate_food-supply[kcal]')
    cal_rate = column_filter(df=cal_rate, pattern='^.*$')

    return output_interface, cal_rate, output_bld, output_tra, output_ind, output_afo, output_wat


def building(pop):
    # Total number of household [num], from the the household size and the total population
    household_size_cap_per_household = import_data(trigram='lfs', variable_name='household-size') # OTS/FTS
    households_total_num = mcd(input_table_1=pop, input_table_2=household_size_cap_per_household, operation_selection='x / y', output_name='households-total[num]')
    
    # Product substitution rate [%] NOTE: determine after which time product are changed (0 = when arrived to lifetime / < 0 = before lifetime is reached / > 0 = after lifetime is reached)
    product_substitution_rate_percent = import_data(trigram='lfs', variable_name='product-substitution-rate') # OTS/FTS
    
    # Appliance ownership [num]
    appliance_own_num_per_household = import_data(trigram='lfs', variable_name='appliance-own') # OTS/FTS
    households_total_num = use_variable(input_table=households_total_num, selected_variable='households-total[num]')
    households_total_num_2 = group_by_dimensions(df=households_total_num, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum') # NOTE: how to handle ranaming after grouing?
    appliance_own_num = mcd(input_table_1=households_total_num_2, input_table_2=appliance_own_num_per_household, operation_selection='x * y', output_name='appliance-own[num]')

    # Appliances use [h]
    appliance_use_h_per_year_times_num = import_data(trigram='lfs', variable_name='appliance-use')
    appliance_use_h = mcd(input_table_1=appliance_own_num, input_table_2=appliance_use_h_per_year_times_num, operation_selection='x * y', output_name='appliance-use[h]')
    appliance = pd.concat([appliance_own_num, appliance_use_h.set_index(appliance_use_h.index.astype(str) + '_dup')])

    # Heating and Cooling behaviour 
    ## INFO:
    ## - heating-cooling-behaviour-index[-]
    ## => T°C of comfort for heating ; we compare it to baseyear value to know if we have to increase/decrease space heating
    ## => We consider that the increase/decrease of comfort for heating is the same for space cooling
    ## Ex. basyear T°C = 18°C => increase comfort to 20°C (heating) => + 2°C comfort for heating and -2°C for cooling
    heatcool_behaviour_deg_C = import_data(trigram='lfs', variable_name='heatcool-behaviour')
    heatcool_behaviour_deg_C_BASEYEAR, _ = filter_dimension(df=heatcool_behaviour_deg_C, dimension='Years', operation_selection='=', value_years=Globals.get().base_year)
    heatcool_behaviour_deg_C_BASEYEAR = group_by_dimensions(df=heatcool_behaviour_deg_C_BASEYEAR, groupby_dimensions=['Country'], aggregation_method='Sum')
    heating_behaviour_index = mcd(input_table_1=heatcool_behaviour_deg_C_BASEYEAR, input_table_2=heatcool_behaviour_deg_C, operation_selection='y / x', output_name='heating-cooling-behaviour-index[-]')
    cooling_behaviour_index = heating_behaviour_index.assign(**{'end-use': "cooling"})
    heating_cooling_behaviour_index = pd.concat([cooling_behaviour_index, heating_behaviour_index.set_index(heating_behaviour_index.index.astype(str) + '_dup')])
    heating_cooling_behaviour_index = export_variable(input_table=heating_cooling_behaviour_index, selected_variable='heating-cooling-behaviour-index[-]')

    # Output
    concat_1 = pd.concat([households_total_num, pop.set_index(pop.index.astype(str) + '_dup')])
    concat_2 = pd.concat([appliance, product_substitution_rate_percent.set_index(product_substitution_rate_percent.index.astype(str) + '_dup')])
    concat_3 = pd.concat([concat_2, heating_cooling_behaviour_index.set_index(heating_cooling_behaviour_index.index.astype(str) + '_dup')])
    output_bld = pd.concat([concat_1, concat_3.set_index(concat_3.index.astype(str) + '_dup')])
    output_bld = column_filter(df=output_bld, pattern='^.*$')

    return output_bld


def transport(pop):
    # International transport demand [pkm]
    pkm_international_demand_pkm_per_cap_per_year = import_data(trigram='lfs', variable_name='pkm-international-demand')
    transport_demand_pkm = mcd(input_table_1=pop, input_table_2=pkm_international_demand_pkm_per_cap_per_year, operation_selection='x * y', output_name='transport-demand[pkm]')
    transport_demand_pkm['transport-user'] = "passenger"


    # Distance traveled [pkm]
    pkm_inland_demand_pkm_per_cap_per_year = import_data(trigram='lfs', variable_name='pkm-inland-demand')
    pkm_inland_demand_pkm_per_cap_per_year = group_by_dimensions(df=pkm_inland_demand_pkm_per_cap_per_year, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    pkm_inland_demand_pkm = mcd(input_table_1=pop, input_table_2=pkm_inland_demand_pkm_per_cap_per_year, operation_selection='x * y', output_name='pkm-inland-demand[pkm]')
    
    ## Non-urban parameter NOTE:non-urban-parameter = a * urban-pop[%] + b
    non_urban_factor_a = import_data(trigram='lfs', variable_name='non-urban-factor-a', variable_type='RCP')
    non_urban_factor_b_ = import_data(trigram='lfs', variable_name='non-urban-factor-b', variable_type='RCP')
    population_distribution_percent = import_data(trigram='lfs', variable_name='population-distribution')
    non_urban_parameter_percent = mcd(input_table_1=non_urban_factor_a, input_table_2=population_distribution_percent, operation_selection='x * y', output_name='non-urban-parameter[%]')
    non_urban_parameter_percent = mcd(input_table_1=non_urban_parameter_percent, input_table_2=non_urban_factor_b_, operation_selection='x + y', output_name='non-urban-parameter[%]')
    non_urban_parameter_percent = group_by_dimensions(df=non_urban_parameter_percent, groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')
    non_urban_parameter_percent = use_variable(input_table=non_urban_parameter_percent, selected_variable='non-urban-parameter[%]')
    ## Non-urban
    distance_traveled_nonurb_pkm = mcd(input_table_1=non_urban_parameter_percent, input_table_2=pkm_inland_demand_pkm, operation_selection='x * y', output_name='distance-traveled[pkm]')
    
    ## Urban
    distance_traveled_urb_pkm = mcd(input_table_1=distance_traveled_nonurb_pkm, input_table_2=pkm_inland_demand_pkm, operation_selection='y - x', output_name='distance-traveled[pkm]')
    nshift_share_percent = import_data(trigram='lfs', variable_name='nshift-share', variable_type='RCP')
    distance_traveled_urb_pkm = mcd(input_table_1=distance_traveled_urb_pkm, input_table_2=nshift_share_percent, operation_selection='x * (1-y)', output_name='distance-traveled[pkm]')
    distance_traveled_urb_pkm = group_by_dimensions(df=distance_traveled_urb_pkm, groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')
    
    ## Urban + Non-urban
    distance_traveled_nonurb_pkm = distance_traveled_nonurb_pkm.assign(**{'distance-type': "nonurban"})
    distance_traveled_pkm = pd.concat([distance_traveled_nonurb_pkm, distance_traveled_urb_pkm.set_index(distance_traveled_urb_pkm.index.astype(str) + '_dup')])
    distance_traveled_pkm = export_variable(input_table=distance_traveled_pkm, selected_variable='distance-traveled[pkm]')
    pkm = pd.concat([distance_traveled_pkm, transport_demand_pkm.set_index(transport_demand_pkm.index.astype(str) + '_dup')])

    ## Output
    output_tra = pd.concat([pop, pkm.set_index(pkm.index.astype(str) + '_dup')])
    output_tra = column_filter(df=output_tra, pattern='^.*$')

    return output_tra

def industry(pop):
    # Compute the product demand for paper pack (and other packaging) [t]
    product_demand_per_cap_unit_per_cap = import_data(trigram='lfs', variable_name='product-demand-per-cap') #OTS/FTS
    product_demand_unit = mcd(input_table_1=pop, input_table_2=product_demand_per_cap_unit_per_cap, operation_selection='x * y', output_name='product-demand[unit]')
    product_demand_unit = export_variable(input_table=product_demand_unit, selected_variable='product-demand[unit]')

    # Prepare output --> Function ?
    output_ind = column_filter(df=product_demand_unit, pattern='^.*$')

    return output_ind

def afolu(pop):
    # Energy production based on waste (other than food wastes) [TWh]
    domestic_energy_production_TWh_per_cap = import_data(trigram='lfs', variable_name='domestic-energy-production')
    energy_production_TWh = mcd(input_table_1=pop, input_table_2=domestic_energy_production_TWh_per_cap, operation_selection='x * y', output_name='energy-production[TWh]')


    # Food Demand [kcal]
    ## Compute food supply [kcal]
    food_supply_kcal_per_cap_per_day = import_data(trigram='lfs', variable_name='total-food-supply', variable_type='OTS (only)')
    food_supply_kcal_per_cap_per_day = add_missing_years(df_data=food_supply_kcal_per_cap_per_day)
    food_supply_kcal_per_cap_per_day = group_by_dimensions(df=food_supply_kcal_per_cap_per_day, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    food_supply_kcal_per_cap_per_year = food_supply_kcal_per_cap_per_day.assign(**{'food-supply[kcal/cap/year]': food_supply_kcal_per_cap_per_day['total-food-supply[kcal/cap/day]']*365.0})
    food_supply_kcal_per_cap_per_year = use_variable(input_table=food_supply_kcal_per_cap_per_year, selected_variable='food-supply[kcal/cap/year]')
    food_supply_kcal = mcd(input_table_1=pop, input_table_2=food_supply_kcal_per_cap_per_year, operation_selection='x * y', output_name='food-supply[kcal]')
    ### Apply food-supply-share [%]
    food_supply_share_percent = import_data(trigram='lfs', variable_name='food-supply-share', variable_type='RCP')
    food_supply_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_supply_share_percent, operation_selection='x * y', output_name='food-supply[kcal]')
    ### Apply calibration on food supply [kcal]
    food_supply_kcal = use_variable(input_table=food_supply_kcal, selected_variable='food-supply[kcal]')
    food_supply_kcal_calibration = import_data(trigram='lfs', variable_name='food-supply', variable_type='Calibration')
    food_supply_kcal, _, calibration_factor_food_supply = calibration(input_table=food_supply_kcal, cal_table=food_supply_kcal_calibration, data_to_be_cal='food-supply[kcal]', data_cal='food-supply[kcal]')
    ### Apply food-waste-share [%]
    food_waste_share_percent = import_data(trigram='lfs', variable_name='food-waste-share', variable_type='RCP')
    food_waste_share_percent = group_by_dimensions(df=food_waste_share_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    food_consumption_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent, operation_selection='x * (1-y)', output_name='food-consumption[kcal]')
    ### Apply food consumption lever (reduce)
    food_consumption_percent = import_data(trigram='lfs', variable_name='food-consumption')
    food_consumption_kcal = mcd(input_table_1=food_consumption_kcal, input_table_2=food_consumption_percent, operation_selection='x * y', output_name='food-consumption[kcal]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    ### Apply Diet switch
    #### ratio[-] = 1 (used to reassociate other dimensions linked to product)    
    food_consumption_DIMENSION = group_by_dimensions(df=food_consumption_kcal, groupby_dimensions=['category', 'product', 'treatment'], aggregation_method='Sum')
    food_consumption_DIMENSION = food_consumption_DIMENSION.assign(**{'ratio[-]': 1.0})
    food_consumption_DIMENSION = column_filter(df=food_consumption_DIMENSION, columns_to_drop=['food-consumption[kcal]'])
    ####  ratio[-] = 1 (used to determined which dimension is taken into account in the switch) NOTE: TO BE DEFINED IN GS
    food_consumption_SWITCH = pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['red-meat', 'meat-bovine', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-sheep', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-other', 'white-meat', 'meat-poultry'], ['meat', 'meat-bovine', 'vegetal-protein', 'pulse'], ['meat', 'meat-sheep', 'vegetal-protein', 'pulse'], ['meat', 'meat-other', 'vegetal-protein', 'pulse'], ['meat', 'meat-poultry', 'vegetal-protein', 'pulse'], ['meat', 'meat-pig', 'vegetal-protein', 'pulse']])
    food_consumption_SWITCH = food_consumption_SWITCH.assign(**{'ratio[-]': 1.0})# ratio[-] = 1 (no need to  account for energy efficiency differences in food)
    #### Compute food consumption
    food_consumption_kcal = group_by_dimensions(df=food_consumption_kcal, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    diet_switch_percent = import_data(trigram='lfs', variable_name='diet-switch')
    ##### Diet Switch red meat to white meat
    food_consumption_kcal = x_switch(demand_table=food_consumption_kcal, switch_table=diet_switch_percent, correlation_table=food_consumption_SWITCH, col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='red-meat', category_to_selected='white-meat')
    ##### Diet Switch all meat to vegetal meat
    food_consumption_kcal = x_switch(demand_table=food_consumption_kcal, switch_table=diet_switch_percent, correlation_table=food_consumption_SWITCH, col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='meat', category_to_selected='vegetal-protein')
    food_consumption_kcal = mcd(input_table_1=food_consumption_DIMENSION, input_table_2=food_consumption_kcal, operation_selection='x * y', output_name='food-consumption[kcal]')
    #### Renaming to mactch with interface TO CLEAN !!! (clean interface + output to agr and in agr module)  food-consumption as food-demand
    food_demand_kcal = food_consumption_kcal.rename(columns={'food-consumption[kcal]': 'food-demand[kcal]'})
    food_demand_kcal = export_variable(input_table=food_demand_kcal, selected_variable='food-demand[kcal]')

    # Food Waste [kcal]
    food_waste_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent, operation_selection='x * y', output_name='food-waste[kcal]')
    food_waste_percent = import_data(trigram='lfs', variable_name='food-waste')
    food_waste_percent = group_by_dimensions(df=food_waste_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    food_waste_kcal = mcd(input_table_1=food_waste_kcal, input_table_2=food_waste_percent, operation_selection='x * y', output_name='food-waste[kcal]')
    food_waste_kcal = export_variable(input_table=food_waste_kcal, selected_variable='food-waste[kcal]')

    output_afo = pd.concat([food_demand_kcal, food_waste_kcal.set_index(food_waste_kcal.index.astype(str) + '_dup')])
    output_afo = pd.concat([energy_production_TWh, output_afo.set_index(output_afo.index.astype(str) + '_dup')])
    output_afo = pd.concat([calibration_factor_food_supply, output_afo.set_index(output_afo.index.astype(str) + '_dup')])
    output_afo = column_filter(df=output_afo, pattern='^.*$')

    return output_afo