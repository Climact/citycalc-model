import pandas as pd

from patex.nodes.globals import Globals
from patex.nodes import *


def metanode_1(port_01, port_02, port_03, port_04):

    module_name = 'water'

    # Water demand

    # Household : 
    # - Population[cap] x water-requirement[m3/cap]

    # population [cap]
    population_cap = use_variable(input_table=port_01, selected_variable='population[cap]')

    # Livestock : 
    # - livestock-population[lsu] x livestock-water-requirement[m3/lsu]

    # livestock-population [lsu]
    livestock_population_lsu = use_variable(input_table=port_03, selected_variable='livestock-population[lsu]')

    # Plant cooling : 
    # - energy-production[GWh] x cooling-water-requirement[m3/GWh]

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=port_04, selected_variable='energy-production[TWh]')
    # Convert Unit TWh to GWh (*1000)
    energy_production_GWh = energy_production_TWh.drop(columns='energy-production[TWh]').assign(**{'energy-production[GWh]': energy_production_TWh['energy-production[TWh]'] * 1000.0})

    # Industry production : 
    # - material-production[t] x industry-water-requirement[m3/t]

    # material-production [Mt]
    material_production_Mt = use_variable(input_table=port_02, selected_variable='material-production[Mt]')
    # Convert Unit Mt to t (*1.000.000)
    material_production_t = material_production_Mt.drop(columns='material-production[Mt]').assign(**{'material-production[t]': material_production_Mt['material-production[Mt]'] * 1000000.0})

    # Irrigation : 
    # - crop-production[kcal] x irrigation-water-requirement[m3/kcal]

    # domestic-crop-production [kcal]
    domestic_crop_production_kcal = use_variable(input_table=port_03, selected_variable='domestic-crop-production[kcal]')
    # OTS (only) water-requirement [m3/unit]
    water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='water-requirement', variable_type='OTS (only)')
    # OTS (only) irrigation-water-requirement [m3/unit]
    irrigation_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='irrigation-water-requirement', variable_type='OTS (only)')
    # OTS (only) livestock-water-requirement [m3/unit]
    livestock_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='livestock-water-requirement', variable_type='OTS (only)')
    # OTS (only) technology-share [%]
    technology_share_percent = import_data(trigram='wat', variable_name='technology-share', variable_type='OTS (only)')
    # OTS (only) industry-water-requirement [m3/unit]
    industry_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='industry-water-requirement', variable_type='OTS (only)')
    # Same as last available year
    industry_water_requirement_m3_per_unit = add_missing_years(df_data=industry_water_requirement_m3_per_unit)
    # water-demand[m3] = material-production[t] * industry-water-requirement [m3/unit]
    water_demand_m3 = mcd(input_table_1=industry_water_requirement_m3_per_unit, input_table_2=material_production_t, operation_selection='x * y', output_name='water-demand[m3]')
    # Group by  Country, Years, water-use (sum)
    water_demand_m3_2 = group_by_dimensions(df=water_demand_m3, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    # Same as last available year
    technology_share_percent = add_missing_years(df_data=technology_share_percent)
    # cooling-energy-production[GWh] = energy-production[GWh] * technology-share [%]
    cooling_energy_production_GWh = mcd(input_table_1=technology_share_percent, input_table_2=energy_production_GWh, operation_selection='x * y', output_name='cooling-energy-production[GWh]')
    # OTS (only) cooling-water-requirement [m3/unit]
    cooling_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='cooling-water-requirement', variable_type='OTS (only)')
    # Same as last available year
    cooling_water_requirement_m3_per_unit = add_missing_years(df_data=cooling_water_requirement_m3_per_unit)
    # water-demand[m3] = cooling-energy-production[GWh] * cooling-water-requirement [m3/unit]
    water_demand_m3 = mcd(input_table_1=cooling_energy_production_GWh, input_table_2=cooling_water_requirement_m3_per_unit, operation_selection='x * y', output_name='water-demand[m3]')
    # Group by  Country, Years, cooling-technology, way-of-production (sum)
    water_demand_m3_3 = group_by_dimensions(df=water_demand_m3, groupby_dimensions=['Country', 'Years', 'cooling-technology', 'water-use', 'way-of-production'], aggregation_method='Sum')
    # Group by  Country, Years, water-use (sum)
    water_demand_m3 = group_by_dimensions(df=water_demand_m3, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    water_demand_m3_2 = pd.concat([water_demand_m3, water_demand_m3_2.set_index(water_demand_m3_2.index.astype(str) + '_dup')])
    # ratio-water-per-tech[%] = water-demand[m3] full details * water-demand[m3] by Country, Years
    ratio_water_per_tech_percent = mcd(input_table_1=water_demand_m3, input_table_2=water_demand_m3_3, operation_selection='y / x', output_name='ratio-water-per-tech[%]')
    # Division by 0 create NaN => replace them by real 0 to avoid error
    ratio_water_per_tech_percent = missing_value(df=ratio_water_per_tech_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Water consumption
    # Water consumption consider only potable / clean water use that will not return to the environment.
    # Notes :
    # - irrigation : we assume that water poured onto fields is entirely consumed
    # by plants for growth => so consumption factor = 100%
    # - household : only drinking water is sonsumed, the rest (water use for washing machine, shower, ...) is returned to the environement
    # - livestock : cfr household - only drinking water is consumed
    # 
    # For cooling, the consumption of water depends on cooling technologies and electricity way of production

    # ratio-water-per-tech [%]
    ratio_water_per_tech_percent = use_variable(input_table=ratio_water_per_tech_percent, selected_variable='ratio-water-per-tech[%]')
    # OTS (only) cooling-consumption-factor [%]
    cooling_consumption_factor_percent = import_data(trigram='wat', variable_name='cooling-consumption-factor', variable_type='OTS (only)')
    # Same as last available year
    cooling_consumption_factor_percent = add_missing_years(df_data=cooling_consumption_factor_percent)
    # Same as last available year
    livestock_water_requirement_m3_per_unit = add_missing_years(df_data=livestock_water_requirement_m3_per_unit)
    # water-demand[m3] = livestock-populatio[lsu] * livestock-water-requirement [m3/unit]
    water_demand_m3 = mcd(input_table_1=livestock_water_requirement_m3_per_unit, input_table_2=livestock_population_lsu, operation_selection='x * y', output_name='water-demand[m3]')
    # Group by  Country, Years, water-use (sum)
    water_demand_m3 = group_by_dimensions(df=water_demand_m3, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    water_demand_m3 = pd.concat([water_demand_m3, water_demand_m3_2.set_index(water_demand_m3_2.index.astype(str) + '_dup')])
    # Same as last available year
    irrigation_water_requirement_m3_per_unit = add_missing_years(df_data=irrigation_water_requirement_m3_per_unit)
    # water-demand[m3] = domestic-crop-production [kcal] * irrigation-water-requirement [m3/unit]
    water_demand_m3_2 = mcd(input_table_1=irrigation_water_requirement_m3_per_unit, input_table_2=domestic_crop_production_kcal, operation_selection='x * y', output_name='water-demand[m3]')
    # Group by  Country, Years, water-use (sum)
    water_demand_m3_2 = group_by_dimensions(df=water_demand_m3_2, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    water_demand_m3 = pd.concat([water_demand_m3_2, water_demand_m3.set_index(water_demand_m3.index.astype(str) + '_dup')])
    # Same as last available year
    water_requirement_m3_per_unit = add_missing_years(df_data=water_requirement_m3_per_unit)
    # water-demand[m3] = populatio[cap] * water-requirement [m3/unit]
    water_demand_m3_2 = mcd(input_table_1=water_requirement_m3_per_unit, input_table_2=population_cap, operation_selection='x * y', output_name='water-demand[m3]')
    # Group by  Country, Years, water-use (sum)
    water_demand_m3_2 = group_by_dimensions(df=water_demand_m3_2, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    water_demand_m3 = pd.concat([water_demand_m3_2, water_demand_m3.set_index(water_demand_m3.index.astype(str) + '_dup')])

    # Correction of water requirement : 
    # - for livestock : around 98% of the livestock water footprint is linked to feed production. We want to avoid of double counting with irrigation use.

    # water-demand [m3]
    water_demand_m3 = use_variable(input_table=water_demand_m3, selected_variable='water-demand[m3]')
    # RCP requirement-correction-factor [%]
    requirement_correction_factor_percent = import_data(trigram='wat', variable_name='requirement-correction-factor', variable_type='RCP')
    # water-demand[m3] (replace) = water-demand[m3] * requirement-corr.-factor[%]  LEFT JOIN If missing : set 1 (no correction to be applied)
    water_demand_m3 = mcd(input_table_1=water_demand_m3, input_table_2=requirement_correction_factor_percent, operation_selection='x * y', output_name='water-demand[m3]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # water-demand [m3]
    water_demand_m3 = export_variable(input_table=water_demand_m3, selected_variable='water-demand[m3]')

    # Water withdrawal
    # Water withdrawal account for losses and efficiency of water use. Therefore, it is higher than the water demand.
    # Except for :
    # - plant cooling
    # - livestock
    # - industry production
    # where we consider no loss occurs. For these water uses, water demand = water withdrawal
    # 
    # 
    # Losses for :
    # - Buildings = network leakage
    # - Irrigation = safety margin and conveyance

    # water-demand [m3]
    water_demand_m3_2 = use_variable(input_table=water_demand_m3, selected_variable='water-demand[m3]')
    # OTS (only) water-losses [%]
    water_losses_percent = import_data(trigram='wat', variable_name='water-losses', variable_type='OTS (only)')
    # OTS (only) water-use-efficiency [%]
    water_use_efficiency_percent = import_data(trigram='wat', variable_name='water-use-efficiency', variable_type='OTS (only)')
    # Same as last available year
    water_use_efficiency_percent = add_missing_years(df_data=water_use_efficiency_percent)
    # Same as last available year
    water_losses_percent = add_missing_years(df_data=water_losses_percent)
    # Group by  Country, Years, water-use (sum) No need to keep losses information
    water_losses_percent = group_by_dimensions(df=water_losses_percent, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    # water-losses[%] (replace) = water-losses[%] + 1
    water_losses_percent = math_formula(df=water_losses_percent, convert_to_int=False, replaced_column='water-losses[%]', splitted='$water-losses[%]$+1')
    # water-withdrawal[m3] = water-demand[m3] * water-losses[%]  LEFT JOIN If missing : set 1 (no losses)
    water_withdrawal_m3 = mcd(input_table_1=water_demand_m3_2, input_table_2=water_losses_percent, operation_selection='x * y', output_name='water-withdrawal[m3]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # water-withdrawal[m3] (replace) = water-withdrawal[m3] / water-use-efficiency[%]  LEFT JOIN If missing : set 1 (no impact of eeficiency)
    water_withdrawal_m3_2 = mcd(input_table_1=water_withdrawal_m3, input_table_2=water_use_efficiency_percent, operation_selection='x / y', output_name='water-withdrawal[m3]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Calibration water-withdrawal [m3]
    water_withdrawal_m3 = import_data(trigram='wat', variable_name='water-withdrawal', variable_type='Calibration')

    # Calibration

    # Apply Calibration on water-withdrawal[m3]
    _, out_9434_2, out_9434_3 = calibration(input_table=water_withdrawal_m3_2, cal_table=water_withdrawal_m3, data_to_be_cal='water-withdrawal[m3]', data_cal='water-withdrawal[m3]')

    # Calibration RATES

    # Cal_rate for water-withdrawal [m3]

    # cal-rate for water-withdrawal [m3]
    cal_rate_water_withdrawal_m3 = use_variable(input_table=out_9434_3, selected_variable='cal_rate_water-withdrawal[m3]')

    # Water demand
    # Apply cal rate

    # Calibration
    # We apply cal rate from water withdrawal
    # It 'is not perfect but allow to be closer to the reality with avlue availability

    # water-demand[m3] = water-demand[m3] * cal-rate[%]
    water_demand_m3 = mcd(input_table_1=out_9434_2, input_table_2=water_demand_m3, operation_selection='x * y', output_name='water-demand[m3]', fill_value=1.0)
    # water-demand [m3] (calibrated)
    water_demand_m3 = export_variable(input_table=water_demand_m3, selected_variable='water-demand[m3]')
    # water-demand [m3]
    water_demand_m3 = use_variable(input_table=water_demand_m3, selected_variable='water-demand[m3]')
    # OTS (only) consumption-factor [%]
    consumption_factor_percent = import_data(trigram='wat', variable_name='consumption-factor', variable_type='OTS (only)')
    # Same as last available year
    consumption_factor_percent = add_missing_years(df_data=consumption_factor_percent)
    # Exclude water-use = plant-cooling
    water_demand_m3_excluded = water_demand_m3.loc[water_demand_m3['water-use'].isin(['plant-cooling'])].copy()
    water_demand_m3 = water_demand_m3.loc[~water_demand_m3['water-use'].isin(['plant-cooling'])].copy()
    # water-consumption[m3] = water-demand[m3] * consumption-factor[%]  LEFT JOIN If missing : set 1 (all water demand is consumed)
    water_consumption_m3 = mcd(input_table_1=water_demand_m3, input_table_2=consumption_factor_percent, operation_selection='x * y', output_name='water-consumption[m3]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # water-demand[m3] (replace) = water-demand[m3] * ratio-water-per-tech[%] 
    water_demand_m3 = mcd(input_table_1=water_demand_m3_excluded, input_table_2=ratio_water_per_tech_percent, operation_selection='x * y', output_name='water-demand[m3]', fill_value_bool='Inner Join')
    # water-consumption[m3] = water-demand[m3] * cooling-consumption-factor[%]
    water_consumption_m3_2 = mcd(input_table_1=water_demand_m3, input_table_2=cooling_consumption_factor_percent, operation_selection='x * y', output_name='water-consumption[m3]', fill_value_bool='Inner Join', fill_value=1.0)
    # Group by  Country, Years, water-use (sum)
    water_consumption_m3_2 = group_by_dimensions(df=water_consumption_m3_2, groupby_dimensions=['Country', 'Years', 'water-use'], aggregation_method='Sum')
    water_consumption_m3 = pd.concat([water_consumption_m3, water_consumption_m3_2.set_index(water_consumption_m3_2.index.astype(str) + '_dup')])
    # water-consumption [m3]
    water_consumption_m3 = export_variable(input_table=water_consumption_m3, selected_variable='water-consumption[m3]')

    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer
    # 
    # - Water consumption [m3] per water-use

    # water-consumption [m3]
    water_consumption_m3 = use_variable(input_table=water_consumption_m3, selected_variable='water-consumption[m3]')

    # Water stress
    # We compare water consumption with water availability.
    # To do so, we first split the consumption according to sub-region (needs are not the same between Norther and Southern France) and season (water needs for irrigation only occurs during summer for example).
    # Then we compare this need to the availability (which also depands on seasons and sub-regions)

    # CP (wat_)region-share [%]
    wat_region_share_percent = import_data(trigram='wat', variable_name='wat_region-share', variable_type='CP')
    # CP (wat_)winter-share [%] (details for sub-region)
    wat_winter_share_percent = import_data(trigram='wat', variable_name='wat_winter-share', variable_type='CP')
    # RCP winter-share [%] (values for full-region)
    winter_share_percent = import_data(trigram='wat', variable_name='winter-share', variable_type='RCP')
    # Group by  Country, water-use, sub-region (sum)  Remove unused columns
    wat_winter_share_percent = group_by_dimensions(df=wat_winter_share_percent, groupby_dimensions=['water-use', 'sub-region', 'Country'], aggregation_method='Sum')
    # winter-share[%] (replace) = winter-share[%] (full-region) * winter-share[%] (details for sub-region)  LEFT JOIN : If missing : set 1 (no split into sub-region)
    winter_share_percent = mcd(input_table_1=winter_share_percent, input_table_2=wat_winter_share_percent, operation_selection='x * y', output_name='winter-share[%]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Missing string = full-region (for sub-region)
    winter_share_percent = missing_value(df=winter_share_percent, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='full-region')
    # Group by  Country, water-use, sub-region (sum)  Remove unused columns
    wat_region_share_percent = group_by_dimensions(df=wat_region_share_percent, groupby_dimensions=['water-use', 'sub-region', 'Country'], aggregation_method='Sum')
    # water-consumption[m3] (replace) = water-consumption[m3] * region-share[%]  LEFT JOIN : If missing : set 1 (no split into sub-region)
    water_consumption_m3_2 = mcd(input_table_1=water_consumption_m3, input_table_2=wat_region_share_percent, operation_selection='x * y', output_name='water-consumption[m3]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Missing string = full-region (for sub-region)
    water_consumption_m3_3 = missing_value(df=water_consumption_m3_2, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='full-region')
    # water-consumption[m3] (replace) (winter) = water-consumption[m3] * winter-share[%]
    water_consumption_m3_2 = mcd(input_table_1=water_consumption_m3_3, input_table_2=winter_share_percent, operation_selection='x * y', output_name='water-consumption[m3]', fill_value_bool='Inner Join', fill_value=1.0)
    # water-consumption[m3] (replace) (summer) = water-consumption[m3] * (1-winter-share[%])
    water_consumption_m3_3 = mcd(input_table_1=water_consumption_m3_3, input_table_2=winter_share_percent, operation_selection='x * (1-y)', output_name='water-consumption[m3]', fill_value_bool='Inner Join')
    # semester = summer
    water_consumption_m3_3['semester'] = "summer"
    water_consumption_m3_2 = pd.concat([water_consumption_m3_2, water_consumption_m3_3.set_index(water_consumption_m3_3.index.astype(str) + '_dup')])
    # Group by Country, Years, semester, sub-region (sum)
    water_consumption_m3_2 = group_by_dimensions(df=water_consumption_m3_2, groupby_dimensions=['Country', 'Years', 'sub-region', 'semester'], aggregation_method='Sum')
    # water-consumption [m3] Split by sub-region and semester
    water_consumption_m3_2 = export_variable(input_table=water_consumption_m3_2, selected_variable='water-consumption[m3]')

    # Apply global warming levers
    # => determine the amount amount of available water according to the RCP level

    # OTS/FTS water-availability [m3]
    water_availability_m3 = import_data(trigram='wat', variable_name='water-availability')
    # water-exploitation-index[%] = water-consumption[m3] / water-availability[m3]
    water_exploitation_index_percent_2 = mcd(input_table_1=water_consumption_m3_2, input_table_2=water_availability_m3, operation_selection='x / y', output_name='water-exploitation-index[%]')
    # water-exploitation-index [%]
    water_exploitation_index_percent = export_variable(input_table=water_exploitation_index_percent_2, selected_variable='water-exploitation-index[%]')

    # Have marklines => REMOVE THIS once marlines are available in graph configuration

    # Group by Country, Years (sum)
    water_exploitation_index_percent_2 = group_by_dimensions(df=water_exploitation_index_percent_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # semester = all-year
    water_exploitation_index_percent_3 = water_exploitation_index_percent_2.assign(**{'semester': "all-year"})
    # water-exploitation-index[%] = 0.1
    water_exploitation_index_percent_2 = water_exploitation_index_percent_3.assign(**{'water-exploitation-index[%]': 0.1})
    # sub-region = moderate-water-scarce-region
    water_exploitation_index_percent_2['sub-region'] = "moderate-water-scarce-region"
    # water-exploitation-index[%] = 0.2
    water_exploitation_index_percent_4 = water_exploitation_index_percent_3.assign(**{'water-exploitation-index[%]': 0.2})
    # sub-region = water-scarce-region
    water_exploitation_index_percent_4['sub-region'] = "water-scarce-region"
    # water-exploitation-index[%] = 0.4
    water_exploitation_index_percent_3['water-exploitation-index[%]'] = 0.4
    # sub-region = severely-water-scarce-region
    water_exploitation_index_percent_3['sub-region'] = "severely-water-scarce-region"
    water_exploitation_index_percent_3 = pd.concat([water_exploitation_index_percent_4, water_exploitation_index_percent_3.set_index(water_exploitation_index_percent_3.index.astype(str) + '_dup')])
    water_exploitation_index_percent_2 = pd.concat([water_exploitation_index_percent_2, water_exploitation_index_percent_3.set_index(water_exploitation_index_percent_3.index.astype(str) + '_dup')])
    water_exploitation_index_percent = pd.concat([water_exploitation_index_percent, water_exploitation_index_percent_2.set_index(water_exploitation_index_percent_2.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Water exploitation index [%] (and mark lines)

    # water-exploitation-index [%]
    water_exploitation_index_percent = use_variable(input_table=water_exploitation_index_percent, selected_variable='water-exploitation-index[%]')
    water = pd.concat([water_consumption_m3, water_exploitation_index_percent.set_index(water_exploitation_index_percent.index.astype(str) + '_dup')])
    out_9493_1 = add_trigram(module_name=module_name, df=water)

    return out_9493_1, cal_rate_water_withdrawal_m3


