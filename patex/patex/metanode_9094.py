import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


# Lifestyle module
def metanode_9094():
    # Apply here MCE 
    # => based on  ?? eco_added-value-industry ??
    # 
    # A appliquer sur FTS / OTS importé ou sur tout (après apply link-material-to-activity) ?


    # May be, this logic should be changed ?
    # Heating comfort should not be the same than cooling ?
    # 
    # Cfr ticket https://climact.atlassian.net/browse/XCALC-1109


    # Apply diet lever (shift)
    # => determine the diet of the population (kcal consummed by person and per day)


    # Apply kcal requirement lever (shift)
    # => determine the kcal requirement of the population


    # For remaining kcal requirements (compared to food demand) :
    # Distribute remaining food demand between different type of aliments (via food-demand-share values)



    module_name = 'lifestyle'

    # Agriculture

    # Food

    # Diet (Food demand) Calculation

    # Define in G.S. !!!

    out_9207_1 = TableCreatorNode(df=pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['red-meat', 'meat-bovine', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-sheep', 'white-meat', 'meat-poultry'], ['red-meat', 'meat-other', 'white-meat', 'meat-poultry'], ['meat', 'meat-bovine', 'vegetal-protein', 'pulse'], ['meat', 'meat-sheep', 'vegetal-protein', 'pulse'], ['meat', 'meat-other', 'vegetal-protein', 'pulse'], ['meat', 'meat-poultry', 'vegetal-protein', 'pulse'], ['meat', 'meat-pig', 'vegetal-protein', 'pulse']]))()
    # ratio[-] = 1 (no need to  account for energy efficiency differences in food)
    ratio = out_9207_1.assign(**{'ratio[-]': 1.0})

    # Population

    # Apply population levers (avoid)
    # => determine the amount of inhabitant per country

    # OTS/FTS population [cap]
    population_cap = ImportDataNode(trigram='lfs', variable_name='population')()
    # Group by  Country, Years (SUM)
    population_cap = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=population_cap)
    # population [cap]
    population_cap_3 = ExportVariableNode(selected_variable='population[cap]')(input_table=population_cap)

    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer (+ water / air quality / minerals)
    # 
    # - Population

    # population [cap]
    population_cap_2 = UseVariableNode(selected_variable='population[cap]')(input_table=population_cap_3)
    # Module = water
    population_cap = ColumnFilterNode(pattern='^.*$')(df=population_cap_2)

    # Buildings

    # Appliances : 
    # - Appliance-own [num]
    # - Appliances-use [h]
    # - Product-substitution-rate[%]   (product = appliances and other type of product (ex. cars))

    # Apply product substitution rate levers (avoid / improve)
    # => determine after which time product are changed (0 = when arrived to lifetime / < 0 = before lifetime is reached / > 0 = after lifetime is reached)
    # Ex. -0.1 = at 90% of their lifetime
    # Ex. 0.1 = at 110% of their lifetime

    # OTS/FTS product-substitution-rate [%]
    product_substitution_rate_percent = ImportDataNode(trigram='lfs', variable_name='product-substitution-rate')()
    # product-substitution-rate [%]
    product_substitution_rate_percent = ExportVariableNode(selected_variable='product-substitution-rate[%]')(input_table=product_substitution_rate_percent)

    # Apply appliances own levers (avoid)
    # => determine the amount of appliances used per house

    # OTS/FTS appliance-own [num/household]
    appliance_own_num_per_household = ImportDataNode(trigram='lfs', variable_name='appliance-own')()

    # Household : 
    # - Total amount of household [num]
    # - Share of appartement [%]
    # - Floor space [1000m2]

    # Apply household size levers (avoid)
    # => determine the amount of person living in a same house

    # OTS/FTS household-size [cap/household]
    household_size_cap_per_household = ImportDataNode(trigram='lfs', variable_name='household-size')()
    # households-total[num] = population[cap] / household-size [cap/households]
    households_total_num = MCDNode(operation_selection='x / y', output_name='households-total[num]')(input_table_1=population_cap_2, input_table_2=household_size_cap_per_household)
    # households-total [num]
    households_total_num = ExportVariableNode(selected_variable='households-total[num]')(input_table=households_total_num)
    out_8216_1 = pd.concat([households_total_num, population_cap_3.set_index(population_cap_3.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Food Demand
    # - Food Waste
    # - House Hold [num]

    # households-total[num]
    households_total_num = UseVariableNode(selected_variable='households-total[num]')(input_table=households_total_num)
    # Group by  Country, Years (SUM)
    households_total_num_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=households_total_num)
    # appliance-own[num] = appliance-own[num/household] * households-total[num]
    appliance_own_num = MCDNode(operation_selection='x * y', output_name='appliance-own[num]')(input_table_1=households_total_num_2, input_table_2=appliance_own_num_per_household)
    # appliance-own [num]
    appliance_own_num = ExportVariableNode(selected_variable='appliance-own[num]')(input_table=appliance_own_num)

    # Apply appliances use levers (avoid)
    # => determine the hour spend on each appliances

    # OTS/FTS appliance-use [h/year*num]
    appliance_use_h_per_year_times_num = ImportDataNode(trigram='lfs', variable_name='appliance-use')()
    # appliance-use[h] = appliance-own[num] * appliance-use[h/year*num]
    appliance_use_h = MCDNode(operation_selection='x * y', output_name='appliance-use[h]')(input_table_1=appliance_own_num, input_table_2=appliance_use_h_per_year_times_num)
    # appliance-use [h]
    appliance_use_h = ExportVariableNode(selected_variable='appliance-use[h]')(input_table=appliance_use_h)
    # Appliances
    appliance = pd.concat([appliance_own_num, appliance_use_h.set_index(appliance_use_h.index.astype(str) + '_dup')])
    # Appliances
    out_8211_1 = pd.concat([appliance, product_substitution_rate_percent.set_index(product_substitution_rate_percent.index.astype(str) + '_dup')])

    # Transport

    # Distance traveled : 
    # - Inland distance traveled [pkm] (active and non-active)
    # - International distance traveled [pkm] (aviation)

    # Apply urban population lever (shift)
    # => determine the % of population livig in urban area

    # OTS/FTS population-distribution [%]
    population_distribution_percent = ImportDataNode(trigram='lfs', variable_name='population-distribution')()

    # Non-urban parameter
    # => non-urban-parameter = a * urban-pop[%] + b

    # RCP non-urban-factor-a [-]
    non_urban_factor_a_ = ImportDataNode(trigram='lfs', variable_name='non-urban-factor-a', variable_type='RCP')()
    # non-urban-parameter[%] = population-distribution[%] * non-urban-factor-a[-]
    non_urban_parameter_percent = MCDNode(operation_selection='x * y', output_name='non-urban-parameter[%]')(input_table_1=non_urban_factor_a_, input_table_2=population_distribution_percent)
    # RCP non-urban-factor-b [-]
    non_urban_factor_b_ = ImportDataNode(trigram='lfs', variable_name='non-urban-factor-b', variable_type='RCP')()
    # non-urban-parameter[%] (replace) = non-urban-parameter[%] + non-urban-factor-b[-]
    non_urban_parameter_percent = MCDNode(operation_selection='x + y', output_name='non-urban-parameter[%]')(input_table_1=non_urban_parameter_percent, input_table_2=non_urban_factor_b_)
    # Group by Country, Years (SUM) (remove Null dimension)
    non_urban_parameter_percent = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')(df=non_urban_parameter_percent)

    # Inland
    # 1) Sum all demand by Country / Years
    # 2) Get total demand by Country = demand per passenger [pkm/cap/year] * population[cap/year]

    # Inland - Non urban
    # => = Total demand[pkm] * non-urban-parameter[%]

    # non-urban-parameter [%]
    non_urban_parameter_percent = UseVariableNode(selected_variable='non-urban-parameter[%]')(input_table=non_urban_parameter_percent)

    # Apply pkm levers (avoid)
    # => determine the km traveled per passenger
    # - Inland
    # - International

    # OTS/FTS pkm-international-demand [pkm/cap/year]
    pkm_international_demand_pkm_per_cap_per_year = ImportDataNode(trigram='lfs', variable_name='pkm-international-demand')()

    # International
    # 1) Get total demand by Country = demand per passenger [pkm/cap/year] * population[cap/year]

    # transport-demand[pkm] = pkm-international-demand[pkm/cap/year] * population[cap]
    transport_demand_pkm = MCDNode(operation_selection='x * y', output_name='transport-demand[pkm]')(input_table_1=population_cap_2, input_table_2=pkm_international_demand_pkm_per_cap_per_year)
    # Add transport-user = passenger
    transport_demand_pkm['transport-user'] = "passenger"
    # transport-demand [pkm]
    transport_demand_pkm = ExportVariableNode(selected_variable='transport-demand[pkm]')(input_table=transport_demand_pkm)

    # Energy production based on waste (other than food wastes)

    # Apply domestic-energy-production lever 
    # => determine the amount of energy that could be produced based on waste (other than food waste)

    # OTS/FTS domestic-energy-production [TWh/cap]
    domestic_energy_production_TWh_per_cap = ImportDataNode(trigram='lfs', variable_name='domestic-energy-production')()
    # OTS (only) total-food-supply [kcal/cap/day]
    total_food_supply_kcal_per_cap_per_day = ImportDataNode(trigram='lfs', variable_name='total-food-supply', variable_type='OTS (only)')()
    # Same as last available year
    total_food_supply_kcal_per_cap_per_day = AddMissingYearsNode()(df_data=total_food_supply_kcal_per_cap_per_day)
    # Group by  Country, Years (sum)
    total_food_supply_kcal_per_cap_per_day = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=total_food_supply_kcal_per_cap_per_day)
    # food-supply[kcal/cap/year]  = total-food-supply[kcal/cap/day] x 365
    food_supply_kcal_per_cap_per_year = total_food_supply_kcal_per_cap_per_day.assign(**{'food-supply[kcal/cap/year]': total_food_supply_kcal_per_cap_per_day['total-food-supply[kcal/cap/day]']*365.0})
    # food-supply [kcal/cap/year] 
    food_supply_kcal_per_cap_per_year = UseVariableNode(selected_variable='food-supply[kcal/cap/year]')(input_table=food_supply_kcal_per_cap_per_year)
    # food-supply[kcal] = food-supply[kcal/cap/year] x population[cap]
    food_supply_kcal = MCDNode(operation_selection='x * y', output_name='food-supply[kcal]')(input_table_1=population_cap_2, input_table_2=food_supply_kcal_per_cap_per_year)
    # RCP food-supply-share [%]
    food_supply_share_percent = ImportDataNode(trigram='lfs', variable_name='food-supply-share', variable_type='RCP')()
    # food-supply[kcal] (detailed by food) = food-supply[kcal] x food-supply-share [%]
    food_supply_kcal = MCDNode(operation_selection='x * y', output_name='food-supply[kcal]')(input_table_1=food_supply_kcal, input_table_2=food_supply_share_percent)

    # Calibration
    # Food supply

    # food-supply [kcal] 
    food_supply_kcal_2 = UseVariableNode(selected_variable='food-supply[kcal]')(input_table=food_supply_kcal)
    # CAL food-supply [kcal]
    food_supply_kcal = ImportDataNode(trigram='lfs', variable_name='food-supply', variable_type='Calibration')()
    # Apply Calibration on food-supply[kcal]
    food_supply_kcal, _, out_8165_3 = CalibrationNode(data_to_be_cal='food-supply[kcal]', data_cal='food-supply[kcal]')(input_table=food_supply_kcal_2, cal_table=food_supply_kcal)
    # RCP food-waste-share [%]
    food_waste_share_percent = ImportDataNode(trigram='lfs', variable_name='food-waste-share', variable_type='RCP')()
    # Group by  Country, Years (sum)
    food_waste_share_percent = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=food_waste_share_percent)

    # Waste

    # Food Waste Calculation

    # food-waste[kcal] = food-supply[kcal] x food-waste-share[%]
    food_waste_kcal = MCDNode(operation_selection='x * y', output_name='food-waste[kcal]')(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent)

    # Apply food waste lever (reduce)
    # => determine the evolution of food waste

    # OTS/FTS food-waste [%]
    food_waste_percent = ImportDataNode(trigram='lfs', variable_name='food-waste')()
    # Group by  Country, Years (sum)
    food_waste_percent = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=food_waste_percent)
    # food-waste[kcal] (replace) = food-waste[kcal] x food-waste[%]
    food_waste_kcal = MCDNode(operation_selection='x * y', output_name='food-waste[kcal]')(input_table_1=food_waste_kcal, input_table_2=food_waste_percent)
    # food-waste [kcal]
    food_waste_kcal = ExportVariableNode(selected_variable='food-waste[kcal]')(input_table=food_waste_kcal)
    # food-waste[kcal]
    food_waste_kcal_2 = UseVariableNode(selected_variable='food-waste[kcal]')(input_table=food_waste_kcal)
    # Group by  Country, Years (sum)
    food_waste_kcal_3 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=food_waste_kcal_2)
    # food-waste-per-cap[kcal/cap] = food-waste[kcal] / population[cap]
    food_waste_per_cap_kcal_per_cap = MCDNode(operation_selection='x / y', output_name='food-waste-per-cap[kcal/cap]')(input_table_1=food_waste_kcal_3, input_table_2=population_cap_2)
    # Convert Unit kcal/cap to kcal/cap/day (/365 => x 0.00274)
    food_waste_per_cap_kcal_per_cap_per_day = food_waste_per_cap_kcal_per_cap.drop(columns='food-waste-per-cap[kcal/cap]').assign(**{'food-waste-per-cap[kcal/cap/day]': food_waste_per_cap_kcal_per_cap['food-waste-per-cap[kcal/cap]'] * 0.00274})
    # food-comsumption[kcal] = food-supply[kcal] x 1 - food-waste-share[%]
    food_consumption_kcal = MCDNode(operation_selection='x * (1-y)', output_name='food-consumption[kcal]')(input_table_1=food_supply_kcal, input_table_2=food_waste_share_percent)

    # Apply food consumption lever (reduce)
    # => determine the evolution of food demand for :
    # - livestock
    # - crop products
    # - auqtic animals

    # OTS/FTS food-consumption [%]
    food_consumption_percent = ImportDataNode(trigram='lfs', variable_name='food-consumption')()
    # food-comsumption[kcal] (replace) = food-comsumption[kcal] x food-comsumption[%]  LEFT JOIN If no evolution : keep it constant (1)
    food_consumption_kcal = MCDNode(operation_selection='x * y', output_name='food-consumption[kcal]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)(input_table_1=food_consumption_kcal, input_table_2=food_consumption_percent)
    # Group by category, treatment product (sum)
    food_consumption_kcal_2 = GroupByDimensions(groupby_dimensions=['category', 'product', 'treatment'], aggregation_method='Sum')(df=food_consumption_kcal)
    # ratio[-] = 1 (used to reassociate other dimensions linked to product)
    ratio_2 = food_consumption_kcal_2.assign(**{'ratio[-]': 1.0})
    # Remove food-consumption [kcal]
    ratio_2 = ColumnFilterNode(columns_to_drop=['food-consumption[kcal]'])(df=ratio_2)
    # Group by  Country, Years, product (sum)
    food_consumption_kcal = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')(df=food_consumption_kcal)

    # Apply diet switch lever (switch)
    # => determine the :
    # - % of red meat converted to white meat
    # - % of meat (including white meat) converted to vegetal proteins

    # OTS/FTS diet-switch [%]
    diet_switch_percent = ImportDataNode(trigram='lfs', variable_name='diet-switch')()
    # Diet Switch red meat to white meat
    out_8268_1 = XSwitchNode(col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='red-meat', category_to_selected='white-meat')(demand_table=food_consumption_kcal, switch_table=diet_switch_percent, correlation_table=ratio)
    # Diet Switch all meat to vegetal meat
    out_8269_1 = XSwitchNode(col_energy='food-consumption[kcal]', col_energy_carrier='product', category_from_selected='meat', category_to_selected='vegetal-protein')(demand_table=out_8268_1, switch_table=diet_switch_percent, correlation_table=ratio)
    # food-comsumption[kcal] (replace) = food-comsumption[kcal] x ratio[-]
    food_consumption_kcal = MCDNode(operation_selection='x * y', output_name='food-consumption[kcal]')(input_table_1=ratio_2, input_table_2=out_8269_1)
    # TO CLEAN !!! (clean interface + output to agr and in agr module)  food-consumption as food-demand
    out_9216_1 = food_consumption_kcal.rename(columns={'food-consumption[kcal]': 'food-demand[kcal]'})
    # food-demand [kcal]
    food_demand_kcal = ExportVariableNode(selected_variable='food-demand[kcal]')(input_table=out_9216_1)
    # Agriculture
    food_kcal = pd.concat([food_demand_kcal, food_waste_kcal.set_index(food_waste_kcal.index.astype(str) + '_dup')])
    # food-demand[kcal]
    food_demand_kcal = UseVariableNode(selected_variable='food-demand[kcal]')(input_table=food_demand_kcal)
    # Group by  Country, Years (sum)
    food_demand_kcal_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=food_demand_kcal)
    # food-demand-per-cap[kcal/cap] = food-demand[kcal] / population[cap]
    food_demand_per_cap_kcal_per_cap = MCDNode(operation_selection='x / y', output_name='food-demand-per-cap[kcal/cap]')(input_table_1=food_demand_kcal_2, input_table_2=population_cap_2)
    # Convert Unit kcal/cap to kcal/cap/day (/365 => x 0.00274)
    food_demand_per_cap_kcal_per_cap_per_day = food_demand_per_cap_kcal_per_cap.drop(columns='food-demand-per-cap[kcal/cap]').assign(**{'food-demand-per-cap[kcal/cap/day]': food_demand_per_cap_kcal_per_cap['food-demand-per-cap[kcal/cap]'] * 0.00274})
    out_8241_1 = pd.concat([food_demand_kcal, population_cap_2.set_index(population_cap_2.index.astype(str) + '_dup')])
    out_8242_1 = pd.concat([out_8241_1, food_waste_kcal_2.set_index(food_waste_kcal_2.index.astype(str) + '_dup')])
    out_9224_1 = pd.concat([out_8242_1, food_demand_per_cap_kcal_per_cap_per_day.set_index(food_demand_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    out_9225_1 = pd.concat([out_9224_1, food_waste_per_cap_kcal_per_cap_per_day.set_index(food_waste_per_cap_kcal_per_cap_per_day.index.astype(str) + '_dup')])
    out_8243_1 = pd.concat([out_9225_1, households_total_num.set_index(households_total_num.index.astype(str) + '_dup')])
    out_8244_1 = AddTrigram()(module_name=module_name, df=out_8243_1)
    # Module = Pathway Explorer
    out_8244_1 = ColumnFilterNode(pattern='^.*$')(df=out_8244_1)

    # Calibration RATES

    # Cal_rate for food-demand [kcal]

    # Cal rate for food-supply [kcal]
    cal_rate_food_supply_kcal = UseVariableNode(selected_variable='cal_rate_food-supply[kcal]')(input_table=out_8165_3)
    # Module = Calibration
    cal_rate_food_supply_kcal = ColumnFilterNode(pattern='^.*$')(df=cal_rate_food_supply_kcal)

    # Domestic energy production [TWh]

    # energy-production[TWh] = domestic-energy-production[TWh/cap] x population[cap]
    energy_production_TWh = MCDNode(operation_selection='x * y', output_name='energy-production[TWh]')(input_table_1=population_cap_2, input_table_2=domestic_energy_production_TWh_per_cap)
    # energy-production [TWh]
    energy_production_TWh = ExportVariableNode(selected_variable='energy-production[TWh]')(input_table=energy_production_TWh)
    # Agriculture
    out_8245_1 = pd.concat([energy_production_TWh, food_kcal.set_index(food_kcal.index.astype(str) + '_dup')])
    # Module = Agriculture
    out_8245_1 = ColumnFilterNode(pattern='^.*$')(df=out_8245_1)
    # OTS/FTS pkm-inland-demand [pkm/cap/year]
    pkm_inland_demand_pkm_per_cap_per_year = ImportDataNode(trigram='lfs', variable_name='pkm-inland-demand')()
    # Group by  Country, Years (SUM)
    pkm_inland_demand_pkm_per_cap_per_year = GroupByDimensions(groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')(df=pkm_inland_demand_pkm_per_cap_per_year)
    # pkm-inland-demand[pkm] = pkm-inland-demand[pkm/cap/year] * population[cap]
    pkm_inland_demand_pkm = MCDNode(operation_selection='x * y', output_name='pkm-inland-demand[pkm]')(input_table_1=population_cap_2, input_table_2=pkm_inland_demand_pkm_per_cap_per_year)
    # distance-traveled[pkm] = non-urban-parameter[%] * pkm-inland-demand[pkm]
    distance_traveled_pkm_2 = MCDNode(operation_selection='x * y', output_name='distance-traveled[pkm]')(input_table_1=non_urban_parameter_percent, input_table_2=pkm_inland_demand_pkm)
    # Replace urban by non-urban
    distance_traveled_pkm = distance_traveled_pkm_2.assign(**{'distance-type': "nonurban"})

    # Inland - Urban
    # => = (Total demand[pkm] - inland-distance-traveled[pkm] from non-urban) * (1 - nshif-share[%])

    # inland-distance-traveled[pkm] (replace) = pkm-inland-demand[pkm] (total) - inland-distance-traveled[pkm] (non-urban)
    distance_traveled_pkm_2 = MCDNode(operation_selection='y - x', output_name='distance-traveled[pkm]')(input_table_1=distance_traveled_pkm_2, input_table_2=pkm_inland_demand_pkm)
    # RCP nshift-share [%]
    nshift_share_percent = ImportDataNode(trigram='lfs', variable_name='nshift-share', variable_type='RCP')()
    # distance-traveled[pkm] = distance-traveled[pkm]] * (1 - nshift-share[%])
    distance_traveled_pkm_2 = MCDNode(operation_selection='x * (1-y)', output_name='distance-traveled[pkm]')(input_table_1=distance_traveled_pkm_2, input_table_2=nshift_share_percent)
    # Group by Country, Years, distance-type (SUM) (remove Null dimension)
    distance_traveled_pkm_2 = GroupByDimensions(groupby_dimensions=['Country', 'Years', 'distance-type'], aggregation_method='Sum')(df=distance_traveled_pkm_2)
    # Urban + Non-urban
    distance_traveled_pkm = pd.concat([distance_traveled_pkm, distance_traveled_pkm_2.set_index(distance_traveled_pkm_2.index.astype(str) + '_dup')])
    # distance-traveled [pkm]
    distance_traveled_pkm = ExportVariableNode(selected_variable='distance-traveled[pkm]')(input_table=distance_traveled_pkm)
    # Transport
    pkm = pd.concat([distance_traveled_pkm, transport_demand_pkm.set_index(transport_demand_pkm.index.astype(str) + '_dup')])
    # Adding cap for Transport
    out_8252_1 = pd.concat([population_cap_3, pkm.set_index(pkm.index.astype(str) + '_dup')])
    # Module = Transport
    out_8252_1 = ColumnFilterNode(pattern='^.*$')(df=out_8252_1)

    # Industry

    # Product demand : 
    # - Paper pack (and other packaging) [t]

    # Apply paper-pack levers (avoid)
    # => determine the tons of packaging (paper, plastic, ...) used per cap

    # OTS/FTS product-demand-per-cap [unit/cap]
    product_demand_per_cap_unit_per_cap = ImportDataNode(trigram='lfs', variable_name='product-demand-per-cap')()
    # product-demand[unit] = population[cap] * product-demand-per-cap[unit/cap]
    product_demand_unit = MCDNode(operation_selection='x * y', output_name='product-demand[unit]')(input_table_1=population_cap_2, input_table_2=product_demand_per_cap_unit_per_cap)
    # product-demand [unit]
    product_demand_unit = ExportVariableNode(selected_variable='product-demand[unit]')(input_table=product_demand_unit)
    # Module = Industry
    product_demand_unit = ColumnFilterNode(pattern='^.*$')(df=product_demand_unit)

    # Heating and Cooling behaviour : 
    # - heating-cooling-behaviour-index[-]
    # => T°C of comfort for heating ; we compare it to baseyear value to know if we have to increase/decrease space heating
    # => We consider that the increase/decrease of comfort for heating is the same for space cooling
    # Ex. basyear T°C = 18°C => increase comfort to 20°C (heating) => + 2°C comfort for heating and -2°C for cooling

    # Apply heat-cool behaviour degrees levers (avoid)
    # => determine the T°C of comfort (heating)

    # OTS/FTS heatcool-behaviour [°C]
    heatcool_behaviour_deg_C = ImportDataNode(trigram='lfs', variable_name='heatcool-behaviour')()
    # Filter baseyear
    heatcool_behaviour_deg_C_2, _ = FilterDimension(dimension='Years', operation_selection='=', value_years=Globals.get().base_year)(df=heatcool_behaviour_deg_C)
    # Group by  Country (SUM)
    heatcool_behaviour_deg_C_2 = GroupByDimensions(groupby_dimensions=['Country'], aggregation_method='Sum')(df=heatcool_behaviour_deg_C_2)
    # heating-cooling-behaviour-index [-] = heatcool-behaviour[°C] / heatcool-behaviour[°C] from baseyear
    heating_cooling_behaviour_index = MCDNode(operation_selection='y / x', output_name='heating-cooling-behaviour-index[-]')(input_table_1=heatcool_behaviour_deg_C_2, input_table_2=heatcool_behaviour_deg_C)
    # Replace heating by cooling (we consider logic for heating is the same than for cooling)
    heating_cooling_behaviour_index_2 = heating_cooling_behaviour_index.assign(**{'end-use': "cooling"})
    # Heating + cooling
    heating_cooling_behaviour_index = pd.concat([heating_cooling_behaviour_index_2, heating_cooling_behaviour_index.set_index(heating_cooling_behaviour_index.index.astype(str) + '_dup')])
    # heating-cooling-behaviour-index [-]
    heating_cooling_behaviour_index = ExportVariableNode(selected_variable='heating-cooling-behaviour-index[-]')(input_table=heating_cooling_behaviour_index)
    # Appliances
    out_8213_1 = pd.concat([out_8211_1, heating_cooling_behaviour_index.set_index(heating_cooling_behaviour_index.index.astype(str) + '_dup')])
    # Buildings
    out_1 = pd.concat([out_8216_1, out_8213_1.set_index(out_8213_1.index.astype(str) + '_dup')])
    # Module = Buildings
    out_1 = ColumnFilterNode(pattern='^.*$')(df=out_1)

    return out_8244_1, cal_rate_food_supply_kcal, out_1, out_8252_1, product_demand_unit, out_8245_1, population_cap, population_cap, population_cap


