import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def compute_wei(name, numerator):
    # Apply region share :
    # ----------------------------------------------------------------------------------- #
    # CP (wat_)region-share [%]
    region_share_percent = import_data(trigram='wat', variable_name='wat_region-share', variable_type='CP')
    # Group by  Country, water-use, sub-region (sum)  Remove unused columns
    region_share_percent = group_by_dimensions(df=region_share_percent,
                                               groupby_dimensions=['water-use', 'sub-region', 'Country'],
                                               aggregation_method='Sum')

    # water-consumption[m3] (replace) = water-consumption[m3] * region-share[%]
    # LEFT JOIN : If missing : set 1 (no split into sub-region)
    numerator = numerator.copy()
    reg_water_consumption_m3 = mcd(input_table_1=numerator, input_table_2=region_share_percent,
                                   operation_selection='x * y', output_name='demand[m3]',
                                   fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Missing string = full-region (for sub-region)
    mask = (reg_water_consumption_m3["sub-region"].isna())
    reg_water_consumption_m3.loc[mask, "sub-region"] = 'full-region'

    # Apply semester share :
    # ----------------------------------------------------------------------------------- #
    # CP (wat_)winter-share [%] (details for sub-region)
    reg_winter_share_percent = import_data(trigram='wat', variable_name='wat_winter-share', variable_type='CP')
    # Group by  Country, water-use, sub-region (sum)  Remove unused columns
    wat_winter_share_percent = group_by_dimensions(df=reg_winter_share_percent,
                                                   groupby_dimensions=['water-use', 'sub-region', 'Country'],
                                                   aggregation_method='Sum')
    # RCP winter-share [%] (values for full-region)
    winter_share_percent = import_data(trigram='wat', variable_name='winter-share', variable_type='RCP')

    # winter-share[%] (replace) = winter-share[%] (full-region) * winter-share[%] (details for sub-region)
    # LEFT JOIN : If missing : set 1 (no split into sub-region)
    winter_share_percent = mcd(input_table_1=winter_share_percent, input_table_2=wat_winter_share_percent,
                               operation_selection='x * y', output_name='winter-share[%]',
                               fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Missing string = full-region (for sub-region)
    mask = (winter_share_percent["sub-region"].isna())
    winter_share_percent.loc[mask, "sub-region"] = 'full-region'

    # water-consumption[m3] (replace) (winter) = water-consumption[m3] * winter-share[%]
    winter_water_consumption_m3 = mcd(input_table_1=reg_water_consumption_m3, input_table_2=winter_share_percent,
                                      operation_selection='x * y', output_name='demand[m3]',
                                      fill_value_bool='Inner Join', fill_value=1.0)
    # water-consumption[m3] (replace) (summer) = water-consumption[m3] * (1-winter-share[%])
    summer_water_consumption_m3 = mcd(input_table_1=reg_water_consumption_m3, input_table_2=winter_share_percent,
                                      operation_selection='x * (1-y)', output_name='demand[m3]',
                                      fill_value_bool='Inner Join')
    summer_water_consumption_m3['semester'] = "summer"
    # Aggregate summer and winter
    reg_water_consumption_m3 = pd.concat([winter_water_consumption_m3, summer_water_consumption_m3])
    # Group by Country, Years, semester, sub-region (sum)
    reg_water_consumption_m3 = group_by_dimensions(df=reg_water_consumption_m3,
                                                   groupby_dimensions=['Country', 'Years', 'sub-region', 'semester'],
                                                   aggregation_method='Sum')

    # Apply global warming levers
    # ----------------------------------------------------------------------------------- #
    # Determine the amount of available water according to the RCP level
    # OTS/FTS water-availability [m3]
    water_availability_m3 = import_data(trigram='wat', variable_name='water-availability')
    # water-exploitation-index[%] = water-consumption[m3] / water-availability[m3]
    wei = mcd(input_table_1=reg_water_consumption_m3, input_table_2=water_availability_m3,
                      operation_selection='x / y', output_name=name)
    return wei


def water_demand(name, activity, requirement, group_by, ots_only=True):
    """

    :param name:
    :param activity:
    :param requirement:
    :param ots_only:
    :param groupby:
    :return:
    """
    # If OTS only : same as last available year
    if ots_only:
        requirement = add_missing_years(df_data=requirement)
    # water-demand[m3] = population[cap] * water-requirement [m3/unit]
    demand = mcd(input_table_1=requirement, input_table_2=activity, operation_selection='x * y',
                 output_name=name)
    # Group by
    if group_by['group']:
        demand = group_by_dimensions(df=demand, groupby_dimensions=group_by['group'],
                                     aggregation_method=group_by['method'])

    return demand



def water(lifestyle, industry, agriculture, power):

    module_name = 'water'

    # ----------------------------------------------------------------------------------- #
    # Water demand
    # ----------------------------------------------------------------------------------- #
    water_demand_m3 = []  # Initial df to aggregate all demands
    demand_name = 'water-demand[m3]'

    # Household :
    # ---------------------------------------------------------------
    # Population[cap]
    population_cap = use_variable(input_table=lifestyle, selected_variable='population[cap]')
    # OTS (only) water-requirement [m3/unit]
    water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='water-requirement',
                                                variable_type='OTS (only)')
    # Compute demand : Population[cap] x water-requirement[m3/cap]
    water_demand_m3.append(water_demand(name=demand_name, activity=population_cap,
                                        requirement=water_requirement_m3_per_unit,
                                        group_by={'group': ['Country', 'Years', 'water-use'], 'method': "Sum"}))

    # Livestock :
    # ---------------------------------------------------------------
    # Livestock-population[lsu]
    livestock_population_lsu = use_variable(input_table=agriculture, selected_variable='livestock-population[lsu]')
    # OTS (only) livestock-water-requirement [m3/unit]
    livestock_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='livestock-water-requirement',
                                                          variable_type='OTS (only)')
    # Compute demand : livestock-population[lsu] x livestock-water-requirement[m3/lsu]
    water_demand_m3.append(water_demand(name=demand_name, activity=livestock_population_lsu,
                                        requirement=livestock_water_requirement_m3_per_unit,
                                        group_by={'group': ['Country', 'Years', 'water-use'], 'method': "Sum"}))

    # Industry production :
    # ---------------------------------------------------------------
    # Material-production[Mt]
    material_production_Mt = use_variable(input_table=industry, selected_variable='material-production[Mt]')
    # Convert Unit Mt to t (*1.000.000)
    material_production_t = material_production_Mt.drop(columns='material-production[Mt]').assign(
        **{'material-production[t]': material_production_Mt['material-production[Mt]'] * 1000000.0})
    # OTS (only) industry-water-requirement [m3/unit]
    industry_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='industry-water-requirement',
                                                         variable_type='OTS (only)')
    # Compute demand : material-production[t] x industry-water-requirement[m3/t]
    water_demand_m3.append(water_demand(name=demand_name, activity=material_production_t,
                                        requirement=industry_water_requirement_m3_per_unit,
                                        group_by={'group': ['Country', 'Years', 'water-use'], 'method': "Sum"}))

    # Irrigation :
    # ---------------------------------------------------------------
    # Crop-production[kcal]
    domestic_crop_production_kcal = use_variable(input_table=agriculture, selected_variable='domestic-crop-production[kcal]')
    # OTS (only) irrigation-water-requirement [m3/unit]
    irrigation_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='irrigation-water-requirement',
                                                           variable_type='OTS (only)')
    # Compute demand : crop-production[kcal] x irrigation-water-requirement[m3/kcal]
    water_demand_m3.append(water_demand(name=demand_name, activity=domestic_crop_production_kcal,
                                        requirement=irrigation_water_requirement_m3_per_unit,
                                        group_by={'group': ['Country', 'Years', 'water-use'], 'method': "Sum"}))

    # Plant cooling :
    # ---------------------------------------------------------------
    # energy-production[TWh]
    energy_production_TWh = use_variable(input_table=power, selected_variable='energy-production[TWh]')
    # Convert Unit TWh to GWh (*1000)
    energy_production_GWh = energy_production_TWh.drop(columns='energy-production[TWh]').assign(
        **{'energy-production[GWh]': energy_production_TWh['energy-production[TWh]'] * 1000.0})
    # OTS (only) technology-share [%]
    technology_share_percent = import_data(trigram='wat', variable_name='technology-share', variable_type='OTS (only)')
    # Same as last available year
    technology_share_percent = add_missing_years(df_data=technology_share_percent)
    # cooling-energy-production[GWh] = energy-production[GWh] * technology-share [%]
    cooling_energy_production_GWh = mcd(input_table_1=technology_share_percent, input_table_2=energy_production_GWh,
                                        operation_selection='x * y', output_name='cooling-energy-production[GWh]')
    # OTS (only) cooling-water-requirement [m3/unit]
    cooling_water_requirement_m3_per_unit = import_data(trigram='wat', variable_name='cooling-water-requirement',
                                                        variable_type='OTS (only)')
    # Compute demand : energy-production[GWh] x cooling-water-requirement[m3/GWh]
    water_demand_m3.append(water_demand(name=demand_name, activity=cooling_energy_production_GWh,
                                        requirement=cooling_water_requirement_m3_per_unit,
                                        group_by={'group': ['Country', 'Years', 'water-use'], 'method': "Sum"}))

    # All demands : apply some corrections
    # ---------------------------------------------------------------
    water_demand_m3 = pd.concat(water_demand_m3, ignore_index=True)
    # Correction of water requirement :
    # For livestock : around 98% of the livestock water footprint is linked to feed production.
    # We want to avoid of double counting with irrigation use.

    # RCP requirement-correction-factor [%]
    requirement_correction_factor_percent = import_data(trigram='wat', variable_name='requirement-correction-factor',
                                                        variable_type='RCP')
    # water-demand[m3] = water-demand[m3] * requirement-corr.-factor[%]
    # LEFT JOIN If missing : set 1 (no correction to be applied)
    water_demand_m3 = mcd(input_table_1=water_demand_m3, input_table_2=requirement_correction_factor_percent,
                          operation_selection='x * y', output_name='water-demand[m3]',
                          fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    # Note : water demand should be calibrated !
    # We don't have water demand data for calibration, only water withdrawal, so we use water-withdrawal cal-rate
    # to calibrate water demand (cfr below !)

    # ----------------------------------------------------------------------------------- #
    # Demand ratio per technology (plant cooling)
    # ----------------------------------------------------------------------------------- #
    # Compute demand : keep more granularity (Country, Years, cooling-technology, way-of-production)
    water_demand_per_tech_m3 = water_demand(name=demand_name, activity=cooling_energy_production_GWh,
                                            requirement=cooling_water_requirement_m3_per_unit,
                                            group_by={'group': ['Country', 'Years', 'cooling-technology', 'water-use',
                                                                'way-of-production'],
                                                      'method': "Sum"})
    # ratio-water-per-tech[%] = water-demand[m3] full details * water-demand[m3] by Country, Years
    ratio_water_per_tech_percent = mcd(input_table_1=water_demand_m3, input_table_2=water_demand_per_tech_m3,
                                       operation_selection='y / x', output_name='ratio-water-per-tech[%]')
    # Division by 0 create NaN => replace them by real 0 to avoid error
    ratio_water_per_tech_percent['ratio-water-per-tech[%]'].fillna(0.0, inplace=True)

    # ----------------------------------------------------------------------------------- #
    # Water withdrawal
    # ----------------------------------------------------------------------------------- #
    # Water withdrawal account for losses and efficiency of water use. Therefore, it is higher than the water demand.
    # Except for :
    # - plant cooling
    # - livestock
    # - industry production
    # where we consider no loss occurs. For these water uses, water demand = water withdrawal
    #
    # Losses for :
    # - Buildings = network leakage
    # - Irrigation = safety margin and conveyance

    # Apply losses :
    # OTS (only) water-losses [%]
    water_losses_percent = import_data(trigram='wat', variable_name='water-losses', variable_type='OTS (only)')
    # Same as last available year
    water_losses_percent = add_missing_years(df_data=water_losses_percent)
    # Group by  Country, Years, water-use (sum) (no need to keep losses details)
    water_losses_percent = group_by_dimensions(df=water_losses_percent,
                                               groupby_dimensions=['Country', 'Years', 'water-use'],
                                               aggregation_method='Sum')
    # water-losses[%] (replace) = water-losses[%] + 1
    water_losses_percent['water-losses[%]'] = water_losses_percent['water-losses[%]'] + 1
    # water-withdrawal[m3] = water-demand[m3] * water-losses[%]
    # LEFT JOIN If missing : set 1 (no losses)
    water_withdrawal_m3 = mcd(input_table_1=water_demand_m3, input_table_2=water_losses_percent,
                              operation_selection='x * y', output_name='water-withdrawal[m3]',
                              fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    # Apply efficiency :
    # OTS (only) water-use-efficiency [%]
    water_use_efficiency_percent = import_data(trigram='wat', variable_name='water-use-efficiency',
                                               variable_type='OTS (only)')
    # Same as last available year
    water_use_efficiency_percent = add_missing_years(df_data=water_use_efficiency_percent)
    # water-withdrawal[m3] (replace) = water-withdrawal[m3] / water-use-efficiency[%]
    # LEFT JOIN If missing : set 1 (no impact of efficiency)
    water_withdrawal_m3_2 = mcd(input_table_1=water_withdrawal_m3, input_table_2=water_use_efficiency_percent,
                                operation_selection='x / y', output_name='water-withdrawal[m3]',
                                fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    # Apply Calibration on water-withdrawal [m3]
    water_withdrawal_m3 = import_data(trigram='wat', variable_name='water-withdrawal', variable_type='Calibration')
    water_withdrawal_m3, cal_rate_water_withdrawal_m3, cal_flags_water_withdrawal_m3 = calibration(
        input_table=water_withdrawal_m3_2, cal_table=water_withdrawal_m3, data_to_be_cal='water-withdrawal[m3]',
        data_cal='water-withdrawal[m3]')

    # Apply Calibration on water-demand [m3]
    # Use cal-rate of water withdrawal to calibrate water-demand
    # It's not perfect but allow to be closer to the reality with data availability
    water_demand_m3 = mcd(input_table_1=cal_rate_water_withdrawal_m3, input_table_2=water_demand_m3,
                          operation_selection='x * y', output_name='water-demand[m3]', fill_value=1.0)

    # ----------------------------------------------------------------------------------- #
    # Water consumption
    # ----------------------------------------------------------------------------------- #
    # Water consumption consider only potable / clean water use that will not return to the environment.
    # By sector :
    # - irrigation : we assume that water poured onto fields is entirely consumed by plants for growth
    # thus, consumption factor = 100%
    # - household : only drinking water is consumed, the rest (water use for washing machine, shower, ...) is returned
    # to the environment
    # - livestock : cfr household - only drinking water is consumed
    # - cooling : the consumption of water depends on cooling technologies and electricity way of production

    # Distinction between plant-cooling and other sectors
    cooling_water_demand_m3 = water_demand_m3.loc[water_demand_m3['water-use'].isin(['plant-cooling'])].copy()
    other_sector_water_demand_m3 = water_demand_m3.loc[~water_demand_m3['water-use'].isin(['plant-cooling'])].copy()

    # A) Other sectors :
    # ----------------------------------------------------------------------------------- #
    # OTS (only) consumption-factor [%]
    consumption_factor_percent = import_data(trigram='wat', variable_name='consumption-factor',
                                             variable_type='OTS (only)')
    # Same as last available year
    consumption_factor_percent = add_missing_years(df_data=consumption_factor_percent)

    # water-consumption[m3] = water-demand[m3] * consumption-factor[%]
    # LEFT JOIN If missing : set 1 (all water demand is consumed)
    other_sector_water_consumption_m3 = mcd(input_table_1=other_sector_water_demand_m3,
                                            input_table_2=consumption_factor_percent, operation_selection='x * y',
                                            output_name='water-consumption[m3]', fill_value_bool='Left [x] Outer Join',
                                            fill_value=1.0)

    # B) Plant cooling sector :
    # ----------------------------------------------------------------------------------- #
    # OTS (only) cooling-consumption-factor [%]
    cooling_consumption_factor_percent = import_data(trigram='wat', variable_name='cooling-consumption-factor',
                                                     variable_type='OTS (only)')
    # Same as last available year
    cooling_consumption_factor_percent = add_missing_years(df_data=cooling_consumption_factor_percent)
    # water-demand[m3] (replace) = water-demand[m3] * ratio-water-per-tech[%]
    cooling_water_demand_m3 = mcd(input_table_1=cooling_water_demand_m3, input_table_2=ratio_water_per_tech_percent,
                                  operation_selection='x * y', output_name='water-demand[m3]',
                                  fill_value_bool='Inner Join')
    # water-consumption[m3] = water-demand[m3] * cooling-consumption-factor[%]
    cooling_water_consumption_m3 = mcd(input_table_1=cooling_water_demand_m3,
                                       input_table_2=cooling_consumption_factor_percent, operation_selection='x * y',
                                       output_name='water-consumption[m3]', fill_value_bool='Inner Join',
                                       fill_value=1.0)
    # Group by  Country, Years, water-use (sum)
    cooling_water_consumption_m3 = group_by_dimensions(df=cooling_water_consumption_m3,
                                                 groupby_dimensions=['Country', 'Years', 'water-use'],
                                                 aggregation_method='Sum')

    # C) All water consumption : aggregate cooling with other sectors
    # ----------------------------------------------------------------------------------- #
    water_consumption_m3 = pd.concat([cooling_water_consumption_m3, other_sector_water_consumption_m3])
    water_consumption_m3 = export_variable(input_table=water_consumption_m3, selected_variable='water-consumption[m3]')

    # ----------------------------------------------------------------------------------- #
    # Water stress
    # ----------------------------------------------------------------------------------- #
    # We compare water consumption with water availability.
    # To do so, we first split the consumption according to sub-region (needs are not the same between Norther and
    # Southern France) and season (water needs for irrigation only occurs during summer for example).
    # Then, we compare this need to the availability (which also depends on seasons and sub-regions)

    wei_percent = compute_wei(name='water-exploitation-index[%]', numerator=water_withdrawal_m3)
    wei_plus_percent = compute_wei(name='water-exploitation-index-plus[%]', numerator=water_consumption_m3)

    # Have marklines
    # ----------------------------------------------------------------------------------- #
    #TODO : Remove markelines here ; should be defined by API/webtool ?
    # /!\ REMOVE THIS PART once marklines are available in graph configuration /!\
    marklines = []  # Store values
    values = {
        "moderate-water-scarce-region": 0.1,
        "water-scarce-region": 0.2,
        "severely-water-scarce-region": 0.4
    }

    # Group by Country, Years (sum)
    wei_marklines = group_by_dimensions(df=wei_plus_percent, groupby_dimensions=['Country', 'Years'],
                                        aggregation_method='Sum')
    # semester = all-year
    wei_marklines["semester"] = "all-year"
    # Assign values
    for i in values.keys():
        wei_marklines['sub-region'] = i
        wei_marklines['water-exploitation-index[%]'] = values[i]
        wei_marklines['water-exploitation-index-plus[%]'] = values[i]
        marklines.append(wei_marklines.copy())

    wei_marklines = pd.concat(marklines, ignore_index=True)

    # Add marklines to wei
    wei_percent = pd.concat([wei_percent, wei_marklines], ignore_index=True)
    del wei_percent['water-exploitation-index-plus[%]']
    wei_plus_percent = pd.concat([wei_plus_percent, wei_marklines], ignore_index=True)
    del wei_plus_percent['water-exploitation-index[%]']

    # ----------------------------------------------------------------------------------- #
    # Collect Calibration Rates
    # ----------------------------------------------------------------------------------- #
    # cal-rate for water-withdrawal [m3]
    cal_flags_water_withdrawal_m3 = use_variable(input_table=cal_flags_water_withdrawal_m3,
                                                 selected_variable='cal_rate_water-withdrawal[m3]')

    # ----------------------------------------------------------------------------------- #
    # Formating data for other modules + Pathway Explorer
    # ----------------------------------------------------------------------------------- #
    # For : Pathway Explorer
    # A) Water consumption [m3] per water-use
    water_consumption_m3 = use_variable(input_table=water_consumption_m3, selected_variable='water-consumption[m3]')
    # B) Water exploitation index [%] (and mark lines)
    #wei_percent = use_variable(input_table=wei_percent, selected_variable='water-exploitation-index[%]')
    wei_plus_percent = use_variable(input_table=wei_plus_percent, selected_variable='water-exploitation-index-plus[%]')
    wei_plus_percent.rename(columns={'water-exploitation-index-plus[%]': 'water-exploitation-index[%]'}, inplace=True)

    # Concatenate all results
    water = pd.concat([water_consumption_m3, wei_plus_percent]) # wei_percent
    wat_to_patex_output = add_trigram(module_name=module_name, df=water)

    # ----------------------------------------------------------------------------------- #
    # Return results (outputs and flags)
    # ----------------------------------------------------------------------------------- #
    return wat_to_patex_output, cal_flags_water_withdrawal_m3
