import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *

"""
- Grouping everything in a similar way as the knime
- Deleting (a lot of) comments for more concice and relevant ones
- Some naming/way of computing just don't make sense
- Groupong with function ? Could improve readability but wmight not not be reused.
- Units should be kept (?) ==> no unit lead to strange naming (see non-urban factor)
- The export/use variable can make the code lengthier that it need to be
"""


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
    population_cap_3 = export_variable(input_table=population_cap, selected_variable='population[cap]') # NOTE: Could be deleted/ made at an other place?
    population_cap_2 = use_variable(input_table=population_cap_3, selected_variable='population[cap]')


    # 2. Buildings
    #-------------

    # Total number of household [num], from the the household size and the total population
    household_size_cap_per_household = import_data(trigram='lfs', variable_name='household-size') # OTS/FTS
    households_total_num = mcd(input_table_1=population_cap_2, input_table_2=household_size_cap_per_household, operation_selection='x / y', output_name='households-total[num]')
    households_total_num = export_variable(input_table=households_total_num, selected_variable='households-total[num]')
    
    # Product substitution rate [%] NOTE: determine after which time product are changed (0 = when arrived to lifetime / < 0 = before lifetime is reached / > 0 = after lifetime is reached)
    product_substitution_rate_percent = import_data(trigram='lfs', variable_name='product-substitution-rate') # OTS/FTS
    product_substitution_rate_percent = export_variable(input_table=product_substitution_rate_percent, selected_variable='product-substitution-rate[%]') # NOTE: Could be deleted/ made at an other place?
    
    # Appliance ownership [num]
    appliance_own_num_per_household = import_data(trigram='lfs', variable_name='appliance-own') # OTS/FTS
    households_total_num = use_variable(input_table=households_total_num, selected_variable='households-total[num]')
    households_total_num_2 = group_by_dimensions(df=households_total_num, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum') # NOTE: how to handle ranaming after grouing?
    appliance_own_num = mcd(input_table_1=households_total_num_2, input_table_2=appliance_own_num_per_household, operation_selection='x * y', output_name='appliance-own[num]')
    appliance_own_num = export_variable(input_table=appliance_own_num, selected_variable='appliance-own[num]')

    # Appliances use [h]
    appliance_use_h_per_year_times_num = import_data(trigram='lfs', variable_name='appliance-use')
    appliance_use_h = mcd(input_table_1=appliance_own_num, input_table_2=appliance_use_h_per_year_times_num, operation_selection='x * y', output_name='appliance-use[h]')
    appliance_use_h = export_variable(input_table=appliance_use_h, selected_variable='appliance-use[h]')
    appliance = pd.concat([appliance_own_num, appliance_use_h.set_index(appliance_use_h.index.astype(str) + '_dup')])

    # prepare output, to be improved
    out_8216_1 = pd.concat([households_total_num, population_cap_3.set_index(population_cap_3.index.astype(str) + '_dup')])
    out_8211_1 = pd.concat([appliance, product_substitution_rate_percent.set_index(product_substitution_rate_percent.index.astype(str) + '_dup')])

    # 3. Transport
    #-------------

    # International transport demand [pkm]
    pkm_international_demand_pkm_per_cap_per_year = import_data(trigram='lfs', variable_name='pkm-international-demand')
    transport_demand_pkm = mcd(input_table_1=population_cap_2, input_table_2=pkm_international_demand_pkm_per_cap_per_year, operation_selection='x * y', output_name='transport-demand[pkm]')
    transport_demand_pkm['transport-user'] = "passenger"
    transport_demand_pkm = export_variable(input_table=transport_demand_pkm, selected_variable='transport-demand[pkm]')

    # Non-urban parameter NOTE:non-urban-parameter = a * urban-pop[%] + b
    non_urban_factor_a = import_data(trigram='lfs', variable_name='non-urban-factor-a', variable_type='RCP')
    non_urban_factor_b_ = import_data(trigram='lfs', variable_name='non-urban-factor-b', variable_type='RCP')
    population_distribution_percent = import_data(trigram='lfs', variable_name='population-distribution')
    non_urban_parameter_percent = mcd(input_table_1=non_urban_factor_a, input_table_2=population_distribution_percent, operation_selection='x * y', output_name='non-urban-parameter[%]')
    non_urban_parameter_percent = mcd(input_table_1=non_urban_parameter_percent, input_table_2=non_urban_factor_b_, operation_selection='x + y', output_name='non-urban-parameter[%]')
    non_urban_parameter_percent = group_by_dimensions(df=non_urban_parameter_percent, groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')



    # 4. Industry
    #------------
    # Compute the product demand for paper pack (and other packaging) [t]
    product_demand_per_cap_unit_per_cap = import_data(trigram='lfs', variable_name='product-demand-per-cap') #OTS/FTS
    product_demand_unit = mcd(input_table_1=population_cap_2, input_table_2=product_demand_per_cap_unit_per_cap, operation_selection='x * y', output_name='product-demand[unit]')
    product_demand_unit = export_variable(input_table=product_demand_unit, selected_variable='product-demand[unit]')

    # Prepare output --> Function ?
    product_demand_unit = column_filter(df=product_demand_unit, pattern='^.*$')

    # 5. AFOLU
    #---------

    # 6. Supply
    #----------

    # Agriculture

    # Food

    # Diet (Food demand) Calculation

    # Define in G.S. !!!

    out_9207_1 = pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['red-meat', 'meat-bovine', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-sheep', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-other', 'white-meat', 'meat-poultry'], ['meat', 'meat-bovine', 'vegetal-protein', 'pulse'], ['meat', 'meat-sheep', 'vegetal-protein', 'pulse'], ['meat', 'meat-other', 'vegetal-protein', 'pulse'], ['meat', 'meat-poultry', 'vegetal-protein', 'pulse'], ['meat', 'meat-pig', 'vegetal-protein', 'pulse']])
    # ratio[-] = 1 (no need to  account for energy efficiency differences in food)
    ratio = out_9207_1.assign(**{'ratio[-]': 1.0})


    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer (+ water / air quality / minerals)
    # 
    # - Population

    # population [cap]
    # Module = water
    population_cap = column_filter(df=population_cap_2, pattern='^.*$')


    

    # Transport

    # Distance traveled : 
    # - Inland distance traveled [pkm] (active and non-active)
    # - International distance traveled [pkm] (aviation)

    # Apply urban population lever (shift)
    # => determine the % of population livig in urban area

    # OTS/FTS population-distribution [%]

   

    # Inland
    # 1) Sum all demand by Country / Years
    # 2) Get total demand by Country = demand per passenger [pkm/cap/year] * population[cap/year]

    # Inland - Non urban
    # => = Total demand[pkm] * non-urban-parameter[%]

    # non-urban-parameter [%]
    non_urban_parameter_percent = use_variable(input_table=non_urban_parameter_percent, selected_variable='non-urban-parameter[%]')

    # Apply pkm levers (avoid)
    # => determine the km traveled per passenger
    # - Inland
    # - International

    

    # Energy production based on waste (other than food wastes)

    # Apply domestic-energy-production lever 
    # => determine the amount of energy that could be produced based on waste (other than food waste)

    # OTS/FTS domestic-energy-production [TWh/cap]
    domestic_energy_production_TWh_per_cap = import_data(trigram='lfs', variable_name='domestic-energy-production')
    # OTS (only) total-food-supply [kcal/cap/day]
    total_food_supply_kcal_per_cap_per_day = import_data(trigram='lfs', variable_name='total-food-supply', variable_type='OTS (only)')
    # Same as last available year
    total_food_supply_kcal_per_cap_per_day = add_missing_years(df_data=total_food_supply_kcal_per_cap_per_day)
    # Group by  Country, Years (sum)
    total_food_supply_kcal_per_cap_per_day = group_by_dimensions(df=total_food_supply_kcal_per_cap_per_day, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # food-supply[kcal/cap/year]  = total-food-supply[kcal/cap/day] x 365
    food_supply_kcal_per_cap_per_year = total_food_supply_kcal_per_cap_per_day.assign(**{'food-supply[kcal/cap/year]': total_food_supply_kcal_per_cap_per_day['total-food-supply[kcal/cap/day]']*365.0})
    # food-supply [kcal/cap/year] 
    food_supply_kcal_per_cap_per_year = use_variable(input_table=food_supply_kcal_per_cap_per_year, selected_variable='food-supply[kcal/cap/year]')
    # food-supply[kcal] = food-supply[kcal/cap/year] x population[cap]
    food_supply_kcal = mcd(input_table_1=population_cap_2, input_table_2=food_supply_kcal_per_cap_per_year, operation_selection='x * y', output_name='food-supply[kcal]')
    # RCP food-supply-share [%]
    food_supply_share_percent = import_data(trigram='lfs', variable_name='food-supply-share', variable_type='RCP')
    # food-supply[kcal] (detailed by food) = food-supply[kcal] x food-supply-share [%]
    food_supply_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_supply_share_percent, operation_selection='x * y', output_name='food-supply[kcal]')

    # Calibration
    # Food supply

    # food-supply [kcal] 
    food_supply_kcal_2 = use_variable(input_table=food_supply_kcal, selected_variable='food-supply[kcal]')
    # CAL food-supply [kcal]
    food_supply_kcal = import_data(trigram='lfs', variable_name='food-supply', variable_type='Calibration')
    # Apply Calibration on food-supply[kcal]
    food_supply_kcal, _, out_8165_3 = calibration(input_table=food_supply_kcal_2, cal_table=food_supply_kcal, data_to_be_cal='food-supply[kcal]', data_cal='food-supply[kcal]')
    # RCP food-waste-share [%]
    food_waste_share_percent = import_data(trigram='lfs', variable_name='food-waste-share', variable_type='RCP')
    # Group by  Country, Years (sum)
    food_waste_share_percent = group_by_dimensions(df=food_waste_share_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Waste

    # Food Waste Calculation

    # food-waste[kcal] = food-supply[kcal] x food-waste-share[%]
    food_waste_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent, operation_selection='x * y', output_name='food-waste[kcal]')

    # Apply food waste lever (reduce)
    # => determine the evolution of food waste

    # OTS/FTS food-waste [%]
    food_waste_percent = import_data(trigram='lfs', variable_name='food-waste')
    # Group by  Country, Years (sum)
    food_waste_percent = group_by_dimensions(df=food_waste_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # food-waste[kcal] (replace) = food-waste[kcal] x food-waste[%]
    food_waste_kcal = mcd(input_table_1=food_waste_kcal, input_table_2=food_waste_percent, operation_selection='x * y', output_name='food-waste[kcal]')
    # food-waste [kcal]
    food_waste_kcal = export_variable(input_table=food_waste_kcal, selected_variable='food-waste[kcal]')
    # food-waste[kcal]
    food_waste_kcal_2 = use_variable(input_table=food_waste_kcal, selected_variable='food-waste[kcal]')
    # Group by  Country, Years (sum)
    food_waste_kcal_3 = group_by_dimensions(df=food_waste_kcal_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # food-waste-per-cap[kcal/cap] = food-waste[kcal] / population[cap]
    food_waste_per_cap_kcal_per_cap = mcd(input_table_1=food_waste_kcal_3, input_table_2=population_cap_2, operation_selection='x / y', output_name='food-waste-per-cap[kcal/cap]')
    # Convert Unit kcal/cap to kcal/cap/day (/365 => x 0.00274)
    food_waste_per_cap_kcal_per_cap_per_day = food_waste_per_cap_kcal_per_cap.drop(columns='food-waste-per-cap[kcal/cap]').assign(**{'food-waste-per-cap[kcal/cap/day]': food_waste_per_cap_kcal_per_cap['food-waste-per-cap[kcal/cap]'] * 0.00274})
    # food-comsumption[kcal] = food-supply[kcal] x 1 - food-waste-share[%]
    food_consumption_kcal = mcd(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent, operation_selection='x * (1-y)', output_name='food-consumption[kcal]')

    # Apply food consumption lever (reduce)
    # => determine the evolution of food demand for :
    # - livestock
    # - crop products
    # - auqtic animals

    # OTS/FTS food-consumption [%]
    food_consumption_percent = import_data(trigram='lfs', variable_name='food-consumption')
    # food-comsumption[kcal] (replace) = food-comsumption[kcal] x food-comsumption[%]  LEFT JOIN If no evolution : keep it constant (1)
    food_consumption_kcal = mcd(input_table_1=food_consumption_kcal, input_table_2=food_consumption_percent, operation_selection='x * y', output_name='food-consumption[kcal]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Group by category, treatment product (sum)
    food_consumption_kcal_2 = group_by_dimensions(df=food_consumption_kcal, groupby_dimensions=['category', 'product', 'treatment'], aggregation_method='Sum')
    # ratio[-] = 1 (used to reassociate other dimensions linked to product)
    ratio_2 = food_consumption_kcal_2.assign(**{'ratio[-]': 1.0})
    # Remove food-consumption [kcal]
    ratio_2 = column_filter(df=ratio_2, columns_to_drop=['food-consumption[kcal]'])
    # Group by  Country, Years, product (sum)
    food_consumption_kcal = group_by_dimensions(df=food_consumption_kcal, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')

    # Apply diet switch lever (switch)
    # => determine the :
    # - % of red meat converted to white meat
    # - % of meat (including white meat) converted to vegetal proteins

    # OTS/FTS diet-switch [%]
    diet_switch_percent = import_data(trigram='lfs', variable_name='diet-switch')
    # Diet Switch red meat to white meat
    out_8268_1 = x_switch(demand_table=food_consumption_kcal, switch_table=diet_switch_percent, correlation_table=ratio, col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='red-meat', category_to_selected='white-meat')
    # Diet Switch all meat to vegetal meat
    out_8269_1 = x_switch(demand_table=out_8268_1, switch_table=diet_switch_percent, correlation_table=ratio, col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='meat', category_to_selected='vegetal-protein')
    # food-comsumption[kcal] (replace) = food-comsumption[kcal] x ratio[-]
    food_consumption_kcal = mcd(input_table_1=ratio_2, input_table_2=out_8269_1, operation_selection='x * y', output_name='food-consumption[kcal]')
    # TO CLEAN !!! (clean interface + output to agr and in agr module)  food-consumption as food-demand
    out_9216_1 = food_consumption_kcal.rename(columns={'food-consumption[kcal]': 'food-demand[kcal]'})
    # food-demand [kcal]
    food_demand_kcal = export_variable(input_table=out_9216_1, selected_variable='food-demand[kcal]')
    # Agriculture
    food_kcal = pd.concat([food_demand_kcal, food_waste_kcal.set_index(food_waste_kcal.index.astype(str) + '_dup')])
    # food-demand[kcal]
    food_demand_kcal = use_variable(input_table=food_demand_kcal, selected_variable='food-demand[kcal]')
    # Group by  Country, Years (sum)
    food_demand_kcal_2 = group_by_dimensions(df=food_demand_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # food-demand-per-cap[kcal/cap] = food-demand[kcal] / population[cap]
    food_demand_per_cap_kcal_per_cap = mcd(input_table_1=food_demand_kcal_2, input_table_2=population_cap_2, operation_selection='x / y', output_name='food-demand-per-cap[kcal/cap]')
    # Convert Unit kcal/cap to kcal/cap/day (/365 => x 0.00274)
    food_demand_per_cap_kcal_per_cap_per_day = food_demand_per_cap_kcal_per_cap.drop(columns='food-demand-per-cap[kcal/cap]').assign(**{'food-demand-per-cap[kcal/cap/day]': food_demand_per_cap_kcal_per_cap['food-demand-per-cap[kcal/cap]'] * 0.00274})
    out_8241_1 = pd.concat([food_demand_kcal, population_cap_2.set_index(population_cap_2.index.astype(str) + '_dup')])
    out_8242_1 = pd.concat([out_8241_1, food_waste_kcal_2.set_index(food_waste_kcal_2.index.astype(str) + '_dup')])
    out_9224_1 = pd.concat([out_8242_1, food_demand_per_cap_kcal_per_cap_per_day.set_index(food_demand_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    out_9225_1 = pd.concat([out_9224_1, food_waste_per_cap_kcal_per_cap_per_day.set_index(food_waste_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    out_8243_1 = pd.concat([out_9225_1, households_total_num.set_index(households_total_num.index.astype(str) + '_dup')])
    out_8244_1 = add_trigram(module_name=module_name, df=out_8243_1)
    # Module = Pathway Explorer
    out_8244_1 = column_filter(df=out_8244_1, pattern='^.*$')

    # Calibration RATES

    # Cal_rate for food-demand [kcal]

    # Cal rate for food-supply [kcal]
    cal_rate_food_supply_kcal = use_variable(input_table=out_8165_3, selected_variable='cal_rate_food-supply[kcal]')
    # Module = Calibration
    cal_rate_food_supply_kcal = column_filter(df=cal_rate_food_supply_kcal, pattern='^.*$')

    # Domestic energy production [TWh]

    # energy-production[TWh] = domestic-energy-production[TWh/cap] x population[cap]
    energy_production_TWh = mcd(input_table_1=population_cap_2, input_table_2=domestic_energy_production_TWh_per_cap, operation_selection='x * y', output_name='energy-production[TWh]')
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Agriculture
    out_8245_1 = pd.concat([energy_production_TWh, food_kcal.set_index(food_kcal.index.astype(str) + '_dup')])
    # Module = Agriculture
    out_8245_1 = column_filter(df=out_8245_1, pattern='^.*$')
    # OTS/FTS pkm-inland-demand [pkm/cap/year]
    pkm_inland_demand_pkm_per_cap_per_year = import_data(trigram='lfs', variable_name='pkm-inland-demand')
    # Group by  Country, Years (SUM)
    pkm_inland_demand_pkm_per_cap_per_year = group_by_dimensions(df=pkm_inland_demand_pkm_per_cap_per_year, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # pkm-inland-demand[pkm] = pkm-inland-demand[pkm/cap/year] * population[cap]
    pkm_inland_demand_pkm = mcd(input_table_1=population_cap_2, input_table_2=pkm_inland_demand_pkm_per_cap_per_year, operation_selection='x * y', output_name='pkm-inland-demand[pkm]')
    # distance-traveled[pkm] = non-urban-parameter[%] * pkm-inland-demand[pkm]
    distance_traveled_pkm_2 = mcd(input_table_1=non_urban_parameter_percent, input_table_2=pkm_inland_demand_pkm, operation_selection='x * y', output_name='distance-traveled[pkm]')
    # Replace urban by non-urban
    distance_traveled_pkm = distance_traveled_pkm_2.assign(**{'distance-type': "nonurban"})

    # Inland - Urban
    # => = (Total demand[pkm] - inland-distance-traveled[pkm] from non-urban) * (1 - nshif-share[%])

    # inland-distance-traveled[pkm] (replace) = pkm-inland-demand[pkm] (total) - inland-distance-traveled[pkm] (non-urban)
    distance_traveled_pkm_2 = mcd(input_table_1=distance_traveled_pkm_2, input_table_2=pkm_inland_demand_pkm, operation_selection='y - x', output_name='distance-traveled[pkm]')
    # RCP nshift-share [%]
    nshift_share_percent = import_data(trigram='lfs', variable_name='nshift-share', variable_type='RCP')
    # distance-traveled[pkm] = distance-traveled[pkm]] * (1 - nshift-share[%])
    distance_traveled_pkm_2 = mcd(input_table_1=distance_traveled_pkm_2, input_table_2=nshift_share_percent, operation_selection='x * (1-y)', output_name='distance-traveled[pkm]')
    # Group by Country, Years, distance-type (SUM) (remove Null dimension)
    distance_traveled_pkm_2 = group_by_dimensions(df=distance_traveled_pkm_2, groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')
    # Urban + Non-urban
    distance_traveled_pkm = pd.concat([distance_traveled_pkm, distance_traveled_pkm_2.set_index(distance_traveled_pkm_2.index.astype(str) + '_dup')])
    # distance-traveled [pkm]
    distance_traveled_pkm = export_variable(input_table=distance_traveled_pkm, selected_variable='distance-traveled[pkm]')
    # Transport
    pkm = pd.concat([distance_traveled_pkm, transport_demand_pkm.set_index(transport_demand_pkm.index.astype(str) + '_dup')])
    # Adding cap for Transport
    out_8252_1 = pd.concat([population_cap_3, pkm.set_index(pkm.index.astype(str) + '_dup')])
    # Module = Transport
    out_8252_1 = column_filter(df=out_8252_1, pattern='^.*$')

    

    # Heating and Cooling behaviour : 
    # - heating-cooling-behaviour-index[-]
    # => T°C of comfort for heating ; we compare it to baseyear value to know if we have to increase/decrease space heating
    # => We consider that the increase/decrease of comfort for heating is the same for space cooling
    # Ex. basyear T°C = 18°C => increase comfort to 20°C (heating) => + 2°C comfort for heating and -2°C for cooling

    # Apply heat-cool behaviour degrees levers (avoid)
    # => determine the T°C of comfort (heating)

    # OTS/FTS heatcool-behaviour [°C]
    heatcool_behaviour_deg_C = import_data(trigram='lfs', variable_name='heatcool-behaviour')
    # Filter baseyear
    heatcool_behaviour_deg_C_2, _ = filter_dimension(df=heatcool_behaviour_deg_C, dimension='Years', operation_selection='=', value_years=Globals.get().base_year)
    # Group by  Country (SUM)
    heatcool_behaviour_deg_C_2 = group_by_dimensions(df=heatcool_behaviour_deg_C_2, groupby_dimensions=['Country'], aggregation_method='Sum')
    # heating-cooling-behaviour-index [-] = heatcool-behaviour[°C] / heatcool-behaviour[°C] from baseyear
    heating_cooling_behaviour_index = mcd(input_table_1=heatcool_behaviour_deg_C_2, input_table_2=heatcool_behaviour_deg_C, operation_selection='y / x', output_name='heating-cooling-behaviour-index[-]')
    # Replace heating by cooling (we consider logic for heating is the same than for cooling)
    heating_cooling_behaviour_index_2 = heating_cooling_behaviour_index.assign(**{'end-use': "cooling"})
    # Heating + cooling
    heating_cooling_behaviour_index = pd.concat([heating_cooling_behaviour_index_2, heating_cooling_behaviour_index.set_index(heating_cooling_behaviour_index.index.astype(str) + '_dup')])
    # heating-cooling-behaviour-index [-]
    heating_cooling_behaviour_index = export_variable(input_table=heating_cooling_behaviour_index, selected_variable='heating-cooling-behaviour-index[-]')
    # Appliances
    out_8213_1 = pd.concat([out_8211_1, heating_cooling_behaviour_index.set_index(heating_cooling_behaviour_index.index.astype(str) + '_dup')])
    # Buildings
    out_1 = pd.concat([out_8216_1, out_8213_1.set_index(out_8213_1.index.astype(str) + '_dup')])
    # Module = Buildings
    out_1 = column_filter(df=out_1, pattern='^.*$')

    return out_8244_1, cal_rate_food_supply_kcal, out_1, out_8252_1, product_demand_unit, out_8245_1, population_cap, population_cap, population_cap


