import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def climate_emissions(buildings, transport, industry, agriculture, land_use, power):
    # Avoid double counting in emissions
    # 
    # We exclude some gaes capture from the emission table to avoid double counting.
    # 
    # How DAC / CCU / CCS / CC are computed ? :
    # * CCU = furnished by DAC and CC
    # * The remaining part of CC is stored (CCS)
    # => DAC + CC = CCU + CCS (CC : one part goes to CCU, another part goes to CCS)
    # 
    # 
    # What is considered as double counting ? :
    # 0. Exclude DAC and CC (we keep the CCS and CCU term of the equation)
    # 1. CCU : excluded
    # As CCU for efuels (used as fuels or embeded in material) is already accounted in emissions (emission factor = 0 when efuels), we remove it. It also includes CO2 embedded in feedstock (CCU-material), as the equivalent negative emissions are in embedded-feedstock (accounted, see below) we remove it. 
    # 2.CO2-embedded-in-feedstock : accounted
    # Negative emissions from CO2 embedded in feedstock (long lasting products, as a share of CO2 embedded in feedstock) are calculated in industry and has to be part of the output of CLM.
    # For biogenic emissions, we have positive emission of CH4. This explain why the emissions of biofuels are not 0.
    # 3. CCS : accounted
    # As CCS = CC - CCU and we could have CC < CCU, CCS could be negative (= positive in the sens of emission and not absorption).



    module_name = 'climate emissions'

    # Data from other modules

    # emissions[Mt]
    emissions_Mt = use_variable(input_table=power, selected_variable='emissions[Mt]')
    # From Power : Top : sector = ind Bottom : rest
    emissions_Mt_2 = emissions_Mt.loc[emissions_Mt['sector'].isin(['ind'])].copy()
    emissions_Mt_excluded = emissions_Mt.loc[~emissions_Mt['sector'].isin(['ind'])].copy()
    # Add origin-module = elc
    emissions_Mt_excluded['origin-module'] = "elc"
    # Add origin-module = ind
    emissions_Mt = emissions_Mt_2.assign(**{'origin-module': "ind"})
    emissions_Mt = pd.concat([emissions_Mt, emissions_Mt_excluded.set_index(emissions_Mt_excluded.index.astype(str) + '_dup')])
    # Add origin-module = bld
    buildings['origin-module'] = "bld"
    # carbon-intensity[gCO2e/kWh]
    carbon_intensity_gCO2eq_per_kWh = use_variable(input_table=power, selected_variable='carbon-intensity[gCO2eq/kWh]')

    # NEW KPI

    # Industry - Specific emissions [MtCO2eq/Mt]

    # carbon-intensity[gCO2e/kWh]
    carbon_intensity_gCO2eq_per_kWh = use_variable(input_table=carbon_intensity_gCO2eq_per_kWh, selected_variable='carbon-intensity[gCO2eq/kWh]')
    # Top: ener-carr.=heat, elec
    carbon_intensity_gCO2eq_per_kWh = carbon_intensity_gCO2eq_per_kWh.loc[carbon_intensity_gCO2eq_per_kWh['energy-carrier'].isin(['electricity', 'heat'])].copy()
    # g/kWh -> Mt/TWh
    carbon_intensity_MtCO2eq_per_TWh = carbon_intensity_gCO2eq_per_kWh.drop(columns='carbon-intensity[gCO2eq/kWh]').assign(**{'carbon-intensity[MtCO2eq/TWh]': carbon_intensity_gCO2eq_per_kWh['carbon-intensity[gCO2eq/kWh]'] * 0.001})
    # Add origin-module = lus
    land_use['origin-module'] = "lus"
    # Add origin-module = tra
    transport['origin-module'] = "tra"
    port = pd.concat([buildings, transport.set_index(transport.index.astype(str) + '_dup')])
    # energy-demand[TWh]
    energy_demand_TWh = use_variable(input_table=industry, selected_variable='energy-demand[TWh]')

    # Industry - CO2 intensity of gross final energy consumption [tCO2eq/kWh]

    # energy-demand[TWh]
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Group by Country, Years, material, route, tech (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # Top: ener-carr.=heat, elec
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[energy_demand_TWh_2['energy-carrier'].isin(['electricity', 'heat'])].copy()
    # Group by Country, Years, material, route, tech (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'energy-carrier'], aggregation_method='Sum')
    # emissions[MtCO2e] = energy-demand[TWh] x carbon-intensity[MtCO2eq/TWh]
    emissions_MtCO2eq = mcd(input_table_1=energy_demand_TWh_2, input_table_2=carbon_intensity_MtCO2eq_per_TWh, operation_selection='x * y', output_name='emissions[MtCO2eq]')
    # Group by Country, Years, material, route, tech (sum)
    emissions_MtCO2eq = group_by_dimensions(df=emissions_MtCO2eq, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # emissions[Mt]
    emissions_Mt_2 = use_variable(input_table=industry, selected_variable='emissions[Mt]')
    # Add origin-module = ind
    emissions_Mt_2['origin-module'] = "ind"
    out_9198_1 = pd.concat([port, emissions_Mt_2.set_index(emissions_Mt_2.index.astype(str) + '_dup')])
    # OTS/FTS emissions
    emissions = import_data(trigram='clt', variable_name='emissions')
    emissions = pd.concat([emissions_Mt, emissions.set_index(emissions.index.astype(str) + '_dup')])

    # From GHG emissions to CO2-equivalent emissions

    # CP Global Warming Potential GWP
    clt_gwp = import_data(trigram='clt', variable_name='clt_gwp', variable_type='CP')

    # QUICK FIX

    # Switch  variable to double
    gwp_100 = math_formula(df=clt_gwp, convert_to_int=False, replaced_column='gwp-100[-]', splitted='$gwp-100[-]$')
    # Add origin-module = agr
    agriculture['origin-module'] = "agr"
    port = pd.concat([agriculture, land_use.set_index(land_use.index.astype(str) + '_dup')])
    out_9200_1 = pd.concat([port, emissions.set_index(emissions.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9198_1, out_9200_1.set_index(out_9200_1.index.astype(str) + '_dup')])
    # emissions[MtCO2e] = emissions[Mt] x gwp[unit]
    emissions_MtCO2e_3 = mcd(input_table_1=out_1, input_table_2=gwp_100, operation_selection='x * y', output_name='emissions[MtCO2e]')

    # Contribution of reduction and removals to the net reduction [%]
    # For IND, we take the negative emissions, thus the category "embedded-emissions" and not the use of CO2 (category "CCU"), thus not the same as the KPI above

    # emissions[MtCO2eq]
    emissions_MtCO2e = use_variable(input_table=emissions_MtCO2e_3, selected_variable='emissions[MtCO2e]')
    # Top: keep embedded-feedstock, CCS, emissions
    emissions_MtCO2e_2 = emissions_MtCO2e.loc[emissions_MtCO2e['emissions-or-capture'].isin(['emissions', 'embedded-feedstock', 'CCS'])].copy()
    # Top: origini-modul =ind
    emissions_MtCO2e_4 = emissions_MtCO2e_2.loc[emissions_MtCO2e_2['origin-module'].isin(['ind'])].copy()
    emissions_MtCO2e_excluded = emissions_MtCO2e_2.loc[~emissions_MtCO2e_2['origin-module'].isin(['ind'])].copy()
    # Top: keep lus
    emissions_MtCO2e_excluded = emissions_MtCO2e_excluded.loc[emissions_MtCO2e_excluded['origin-module'].isin(['lus'])].copy()
    # Group by country, year, origin-module, emissions
    emissions_MtCO2e_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded, groupby_dimensions=['Country', 'Years', 'origin-module'], aggregation_method='Sum')
    # keep only 2015
    emissions_MtCO2e_excluded_2, _ = filter_dimension(df=emissions_MtCO2e_excluded, dimension='Years', operation_selection='=', value_years='2015')
    # Group by country, emissions
    emissions_MtCO2e_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded_2, groupby_dimensions=['Country'], aggregation_method='Sum')
    # delta-capture[MtCO2e] = - emissions + emissions
    delta_capture_MtCO2e = mcd(input_table_1=emissions_MtCO2e_excluded_2, input_table_2=emissions_MtCO2e_excluded, operation_selection='y - x', output_name='delta-capture[MtCO2e]')
    # Top: keep embedded-feedstock, CCS
    emissions_MtCO2e_4 = emissions_MtCO2e_4.loc[emissions_MtCO2e_4['emissions-or-capture'].isin(['embedded-feedstock', 'CCS'])].copy()
    # Group by country, year, origin-module, emissions
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years', 'origin-module'], aggregation_method='Sum')
    # keep only 2015
    emissions_MtCO2e_5, _ = filter_dimension(df=emissions_MtCO2e_4, dimension='Years', operation_selection='=', value_years='2015')
    # Group by country, emissions
    emissions_MtCO2e_5 = group_by_dimensions(df=emissions_MtCO2e_5, groupby_dimensions=['Country'], aggregation_method='Sum')
    # delta-capture[MtCO2e] = -emissions + emissions
    delta_capture_MtCO2e_2 = mcd(input_table_1=emissions_MtCO2e_5, input_table_2=emissions_MtCO2e_4, operation_selection='y - x', output_name='delta-capture[MtCO2e]')
    delta_capture_MtCO2e = pd.concat([delta_capture_MtCO2e_2, delta_capture_MtCO2e.set_index(delta_capture_MtCO2e.index.astype(str) + '_dup')])
    # Group by country, year, emissions
    emissions_MtCO2e_2 = group_by_dimensions(df=emissions_MtCO2e_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # keep only 2015
    emissions_MtCO2e_4, _ = filter_dimension(df=emissions_MtCO2e_2, dimension='Years', operation_selection='=', value_years='2015')
    # Group by country, emissions
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country'], aggregation_method='Sum')
    # delta-emissions[MtCO2e] = emissions - emissions
    delta_emissions_MtCO2e = mcd(input_table_1=emissions_MtCO2e_4, input_table_2=emissions_MtCO2e_2, operation_selection='x - y', output_name='delta-emissions[MtCO2e]')
    # emissions[MtCO2e] = emissions[Mt] x gwp[unit]
    clm_ratio_delta_emissions_over_delta_stored_percent = mcd(input_table_1=delta_capture_MtCO2e, input_table_2=delta_emissions_MtCO2e, operation_selection='x / y', output_name='clm_ratio-delta-emissions-over-delta-stored[%]')
    # remove divided by zero
    clm_ratio_delta_emissions_over_delta_stored_percent = missing_value(df=clm_ratio_delta_emissions_over_delta_stored_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # clm_ratio-delta-emissions-over-delta-stored[%]
    clm_ratio_delta_emissions_over_delta_stored_percent = export_variable(input_table=clm_ratio_delta_emissions_over_delta_stored_percent, selected_variable='clm_ratio-delta-emissions-over-delta-stored[%]')
    # Keep emission-or-capture = Top = CCU
    emissions_MtCO2e_2 = emissions_MtCO2e.loc[emissions_MtCO2e['emissions-or-capture'].isin(['CCU'])].copy()
    # Keep way-of-production = carbstone, e-MTO, e-dehydration, dry-kiln
    emissions_MtCO2e_2 = emissions_MtCO2e_2.loc[emissions_MtCO2e_2['way-of-production'].isin(['carbstone', 'dry-kiln', 'e-MTO', 'e-dehydration'])].copy()

    # Industry  - Specific sequestration of CCU materials [MtCO2eq/Mt]
    # CO2 use for each CCU-mat --> not equivalent to the equivalent negative emissions. Eg : 1t of plastic will use/sequestrate ~4.5t of CO2 (higher value comes from the fact that it doesn't sequestrate O2), but only 3% (rcp value, ccu-emissions-factor) is assumed to be negative (long lasting plastic products) while the rest is assumed to be eventually burned, thus emitted.

    # Group by year, country, emission-or-capture way-of-production (sum)
    emissions_MtCO2e_2 = group_by_dimensions(df=emissions_MtCO2e_2, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'way-of-production'], aggregation_method='Sum')

    # OLD KPI

    # Keep only year 2015
    emissions_MtCO2e_4, emissions_MtCO2e_excluded = filter_dimension(df=emissions_MtCO2e_3, dimension='Years', operation_selection='=', value_years='2015')
    # Keep only year 2050
    emissions_MtCO2e_excluded, _ = filter_dimension(df=emissions_MtCO2e_excluded, dimension='Years', operation_selection='=', value_years='2050')
    emissions_MtCO2e_4 = pd.concat([emissions_MtCO2e_4, emissions_MtCO2e_excluded.set_index(emissions_MtCO2e_excluded.index.astype(str) + '_dup')])
    # Group by Country, Years, origin-module and emissions-or-capture (sum)
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'origin-module'], aggregation_method='Sum')
    emissions_MtCO2e_5 = emissions_MtCO2e_4.loc[~emissions_MtCO2e_4['origin-module'].isin(['lus'])].copy()
    # Keep emissions
    emissions_MtCO2e_6 = emissions_MtCO2e_5.loc[emissions_MtCO2e_5['emissions-or-capture'].isin(['emissions'])].copy()

    # Emissions 2050 (vs 2015) no LULUCF no BECCS

    # Group by Country, Years (sum)
    emissions_MtCO2e_6 = group_by_dimensions(df=emissions_MtCO2e_6, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # pivot the table to have years as columns
    out_9216_1, _, _ = pivoting(df=emissions_MtCO2e_6, agg_dict={'emissions[MtCO2e]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Aggregation method (column name)', list_group_columns=['Country'], list_pivots=['Years'])
    # remove unwanted names
    out_9216_1 = column_rename_regex(df=out_9216_1, search_string='\\+.*', replace_string='')
    # emissions 2050 vs 2015
    clm_kpi_2050_emissions_no_lulucf_no_beccs_percent = out_9216_1.assign(**{'clm_kpi_2050_emissions-no-lulucf-no-beccs[%]': out_9216_1['2050']/out_9216_1['2015']-1.0})
    clm_kpi_2050_emissions_no_lulucf_no_beccs_percent = column_filter(df=clm_kpi_2050_emissions_no_lulucf_no_beccs_percent, pattern='^Country$|^.*kpi.*$')

    # Emissions 2050 (vs 2015) No LULUCF

    # Group by Country, Years (sum)
    emissions_MtCO2e_5 = group_by_dimensions(df=emissions_MtCO2e_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # pivot the table to have years as columns
    out_9212_1, _, _ = pivoting(df=emissions_MtCO2e_5, agg_dict={'emissions[MtCO2e]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Aggregation method (column name)', list_group_columns=['Country'], list_pivots=['Years'])
    # remove unwanted names
    out_9212_1 = column_rename_regex(df=out_9212_1, search_string='\\+.*', replace_string='')
    # emissions 2050 vs 2015
    clm_kpi_2050_emissions_no_lulucf_percent = out_9212_1.assign(**{'clm_kpi_2050_emissions-no-lulucf[%]': out_9212_1['2050']/out_9212_1['2015']-1.0})
    clm_kpi_2050_emissions_no_lulucf_percent = column_filter(df=clm_kpi_2050_emissions_no_lulucf_percent, pattern='^Country$|^.*kpi.*$')

    # Emissions 2050 (vs 2015)

    # Group by Country, Years (sum)
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # pivot the table to have years as columns
    out_3742_1, _, _ = pivoting(df=emissions_MtCO2e_4, agg_dict={'emissions[MtCO2e]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Aggregation method (column name)', list_group_columns=['Country'], list_pivots=['Years'])
    # remove unwanted names
    out_3742_1 = column_rename_regex(df=out_3742_1, search_string='\\+.*', replace_string='')
    # emissions 2050 vs 2015
    clm_kpi_2050_emissions_percent = out_3742_1.assign(**{'clm_kpi_2050_emissions[%]': out_3742_1['2050']/out_3742_1['2015']-1.0})
    clm_kpi_2050_emissions_percent = column_filter(df=clm_kpi_2050_emissions_percent, pattern='^Country$|^.*kpi.*$')
    out_3773_1 = joiner(df_left=clm_kpi_2050_emissions_percent, df_right=clm_kpi_2050_emissions_no_lulucf_percent, joiner='inner', left_input=['Country'], right_input=['Country'])
    out_3774_1 = joiner(df_left=out_3773_1, df_right=clm_kpi_2050_emissions_no_lulucf_no_beccs_percent, joiner='inner', left_input=['Country'], right_input=['Country'])
    # Keep emission-or-capture = CCS, CCU, DAC, CC, embedded-feedstock
    emissions_MtCO2e_4 = emissions_MtCO2e_3.loc[emissions_MtCO2e_3['emissions-or-capture'].isin(['embedded-feedstock', 'CCS', 'CCU', 'DAC', 'CC'])].copy()
    emissions_MtCO2e_excluded = emissions_MtCO2e_3.loc[~emissions_MtCO2e_3['emissions-or-capture'].isin(['embedded-feedstock', 'CCS', 'CCU', 'DAC', 'CC'])].copy()
    # Keep emission-or-capture = Top : CCS, CCU, embedded-feedstock Bottom : DAC, CC
    emissions_MtCO2e_3 = emissions_MtCO2e_4.loc[emissions_MtCO2e_4['emissions-or-capture'].isin(['embedded-feedstock', 'CCS', 'CCU'])].copy()
    emissions_MtCO2e_excluded_2 = emissions_MtCO2e_4.loc[~emissions_MtCO2e_4['emissions-or-capture'].isin(['embedded-feedstock', 'CCS', 'CCU'])].copy()
    # Keep CCS, CCU Remove embedded-feedstock (already accounted in  CCU-ind and CCU-elc)
    emissions_MtCO2e_3 = emissions_MtCO2e_3.loc[emissions_MtCO2e_3['emissions-or-capture'].isin(['CCS', 'CCU'])].copy()

    # Formating data for other modules + Pathway Explorer

    # For : Pathway Explorer
    # 
    # - sum of all carbon capture / CCU / CCS and co (CO2 absorption)

    # Group by Country, Years,  origin-module, emission-or-capture, way-of-production (sum)
    emissions_MtCO2e_3 = group_by_dimensions(df=emissions_MtCO2e_3, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'origin-module', 'way-of-production'], aggregation_method='Sum')
    # replace  missing values by  empty cell
    emissions_MtCO2e_3 = missing_value(df=emissions_MtCO2e_3, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='total')
    # Rename variable to clm_CO2-need-by-way-of-prod[MtCO2e]
    out_9409_1 = emissions_MtCO2e_3.rename(columns={'emissions[MtCO2e]': 'clm_CO2-need-by-way-of-prod[MtCO2e]'})

    # For : Pathway Explorer
    # 
    # - Industry

    # emissions[Mt] (DAC and CC only)
    emissions_MtCO2e_3 = use_variable(input_table=emissions_MtCO2e_excluded_2, selected_variable='emissions[MtCO2e]')
    # Keep CC
    emissions_MtCO2e_3 = emissions_MtCO2e_3.loc[emissions_MtCO2e_3['emissions-or-capture'].isin(['CC'])].copy()
    # Group by Country, Years,  origin-module, emission-or-capture, primary-energy-carrier, way-of-production (sum)
    emissions_MtCO2e_5 = group_by_dimensions(df=emissions_MtCO2e_3, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'origin-module', 'way-of-production'], aggregation_method='Sum')
    # include  industry 
    emissions_MtCO2e_3 = emissions_MtCO2e_5.loc[emissions_MtCO2e_5['origin-module'].isin(['ind'])].copy()
    emissions_MtCO2e_excluded_3 = emissions_MtCO2e_5.loc[~emissions_MtCO2e_5['origin-module'].isin(['ind'])].copy()

    # For : Pathway Explorer
    # 
    # - Power

    # emissions[Mt] (DAC and CC only)
    emissions_MtCO2e_5 = use_variable(input_table=emissions_MtCO2e_excluded_3, selected_variable='emissions[MtCO2e]')
    # include  power
    emissions_MtCO2e_5 = emissions_MtCO2e_5.loc[emissions_MtCO2e_5['origin-module'].isin(['elc'])].copy()
    # Remove origin-module
    emissions_MtCO2e_5 = column_filter(df=emissions_MtCO2e_5, columns_to_drop=['origin-module'])
    # Rename variable to elc_emissions-by- em-or-capt-way-of-prod[MtCO2e]
    out_9447_1 = emissions_MtCO2e_5.rename(columns={'emissions[MtCO2e]': 'elc_emissions-by-em-or-capt-way-of-prod[MtCO2e]'})
    # Remove origin-module
    emissions_MtCO2e_3 = column_filter(df=emissions_MtCO2e_3, columns_to_drop=['origin-module'])
    # Rename variable to ind_emissions-by- em-or-capt-material[MtCO2e]
    out_9441_1 = emissions_MtCO2e_3.rename(columns={'emissions[MtCO2e]': 'ind_emissions-by-em-or-capt-material[MtCO2e]'})
    # Group by Country, Years,  origin-module, emission-or-capture, primary-energy-carrier (sum)
    emissions_MtCO2e_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded_2, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'origin-module', 'primary-energy-carrier'], aggregation_method='Sum')
    # replace  missing values by  empty cell
    emissions_MtCO2e_excluded_2 = missing_value(df=emissions_MtCO2e_excluded_2, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='total')
    # Rename variable to clm_CO2-prod-by-energy-carrier[MtCO2e]
    out_9426_1 = emissions_MtCO2e_excluded_2.rename(columns={'emissions[MtCO2e]': 'clm_CO2-prod-by-energy-carrier[MtCO2e]'})
    out_1 = pd.concat([out_9409_1, out_9426_1.set_index(out_9426_1.index.astype(str) + '_dup')])
    # Keep CCS and  embedded-feedstock
    emissions_MtCO2e_3 = emissions_MtCO2e_4.loc[emissions_MtCO2e_4['emissions-or-capture'].isin(['embedded-feedstock', 'CCS'])].copy()
    # Add emissions to capture
    emissions_MtCO2e_3 = pd.concat([emissions_MtCO2e_excluded, emissions_MtCO2e_3.set_index(emissions_MtCO2e_3.index.astype(str) + '_dup')])
    # include  buildings
    emissions_MtCO2e_4 = emissions_MtCO2e_3.loc[emissions_MtCO2e_3['origin-module'].isin(['bld'])].copy()
    emissions_MtCO2e_excluded_2 = emissions_MtCO2e_3.loc[~emissions_MtCO2e_3['origin-module'].isin(['bld'])].copy()
    # include  transport
    emissions_MtCO2e_excluded = emissions_MtCO2e_excluded_2.loc[emissions_MtCO2e_excluded_2['origin-module'].isin(['tra'])].copy()
    emissions_MtCO2e_excluded_excluded = emissions_MtCO2e_excluded_2.loc[~emissions_MtCO2e_excluded_2['origin-module'].isin(['tra'])].copy()
    # include  industry 
    emissions_MtCO2e_excluded_excluded_2 = emissions_MtCO2e_excluded_excluded.loc[emissions_MtCO2e_excluded_excluded['origin-module'].isin(['ind'])].copy()
    emissions_MtCO2e_excluded_excluded_excluded = emissions_MtCO2e_excluded_excluded.loc[~emissions_MtCO2e_excluded_excluded['origin-module'].isin(['ind'])].copy()
    # include  agriculture
    emissions_MtCO2e_excluded_excluded_excluded_2 = emissions_MtCO2e_excluded_excluded_excluded.loc[emissions_MtCO2e_excluded_excluded_excluded['origin-module'].isin(['agr'])].copy()
    emissions_MtCO2e_excluded_excluded_excluded_excluded = emissions_MtCO2e_excluded_excluded_excluded.loc[~emissions_MtCO2e_excluded_excluded_excluded['origin-module'].isin(['agr'])].copy()
    # include  land-use
    emissions_MtCO2e_excluded_excluded_excluded_excluded_2 = emissions_MtCO2e_excluded_excluded_excluded_excluded.loc[emissions_MtCO2e_excluded_excluded_excluded_excluded['origin-module'].isin(['lus'])].copy()
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded = emissions_MtCO2e_excluded_excluded_excluded_excluded.loc[~emissions_MtCO2e_excluded_excluded_excluded_excluded['origin-module'].isin(['lus'])].copy()
    # include  power
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2 = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded.loc[emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded['origin-module'].isin(['elc'])].copy()
    # include others (sector)
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2.loc[emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2['sector'].isin(['others'])].copy()
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_excluded = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2.loc[~emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2['sector'].isin(['others'])].copy()
    # Group by Country, Years,  ets, sector (sum)
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years', 'ets-or-not', 'sector'], aggregation_method='Sum')
    # Rename variable to elc_emissions-by- ets-sector[MtCO2e]
    out_9389_1 = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_excluded.rename(columns={'emissions[MtCO2e]': 'elc_emissions-by-ets-sector[MtCO2e]'})
    # Group by Country, Years,  ets, way-of-prod (sum)
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years', 'ets-or-not', 'way-of-production'], aggregation_method='Sum')
    # Rename variable to elc_emissions-by- ets-way-of-prod[MtCO2e]
    out_9390_1 = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded_2.rename(columns={'emissions[MtCO2e]': 'elc_emissions-by-ets-way-of-prod[MtCO2e]'})
    out_1_3 = pd.concat([out_9390_1, out_9447_1.set_index(out_9447_1.index.astype(str) + '_dup')])
    # Group by Country, Years,  way-of-prod (sum)
    emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Rename variable to elc_emissions-by- way-of-prod[MtCO2e]
    out_9387_1 = emissions_MtCO2e_excluded_excluded_excluded_excluded_excluded.rename(columns={'emissions[MtCO2e]': 'elc_emissions-by-way-of-prod[MtCO2e]'})
    out_1_2 = pd.concat([out_9387_1, out_9389_1.set_index(out_9389_1.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_1_3.set_index(out_1_3.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Land-Use

    # Group by Country, Years,  gaes, land-use carbon-stock (sum)
    emissions_MtCO2e_excluded_excluded_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_excluded_excluded_2, groupby_dimensions=['Country', 'Years', 'gaes', 'land-use', 'carbon-stock'], aggregation_method='Sum')
    # Rename variable to agr_emissions-by- carbon-stock-gaes-land-use[MtCO2e]
    out_9386_1 = emissions_MtCO2e_excluded_excluded_excluded_excluded.rename(columns={'emissions[MtCO2e]': 'agr_emissions-by-carbon-stock-gaes-land-use[MtCO2e]'})
    out_1_2 = pd.concat([out_9386_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Agriculture

    # Group by Country, Years,  gaes,  emissions-type (sum)
    emissions_MtCO2e_excluded_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_excluded_2, groupby_dimensions=['Country', 'Years', 'gaes', 'emission-type'], aggregation_method='Sum')
    # Rename variable to agr_emissions-by- emissions-type-gaes[MtCO2e]
    out_9384_1 = emissions_MtCO2e_excluded_excluded_excluded.rename(columns={'emissions[MtCO2e]': 'agr_emissions-by-emissions-type-gaes[MtCO2e]'})
    out_1_2 = pd.concat([out_9384_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # Group by Country, Years,  material technology, feedstock, energy-carrier-category, ets-or-not (sum)
    emissions_MtCO2e_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded_2, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'material', 'technology', 'feedstock-type', 'energy-carrier-category', 'ets-or-not'], aggregation_method='Sum')
    # Group by Country, Years,  material emissions-or-capture, ets-or-not (sum)
    emissions_MtCO2e_excluded_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'material', 'ets-or-not'], aggregation_method='Sum')
    # Keep emissions emissions-or-capture
    emissions_MtCO2e_excluded_excluded_2 = emissions_MtCO2e_excluded_excluded_2.loc[emissions_MtCO2e_excluded_excluded_2['emissions-or-capture'].isin(['emissions'])].copy()
    # Rename variable to ind_emissions-by- em-or-capt-ets-material[MtCO2e]
    out_9377_1 = emissions_MtCO2e_excluded_excluded_2.rename(columns={'emissions[MtCO2e]': 'ind_emissions-by-em-or-capt-ets-material[MtCO2e]'})
    # Group by Country, Years,  material emissions-or-capture (sum)
    emissions_MtCO2e_excluded_excluded = group_by_dimensions(df=emissions_MtCO2e_excluded_excluded, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'material'], aggregation_method='Sum')
    # Rename variable to ind_emissions-by- em-or-capt-material[MtCO2e]
    out_9376_1 = emissions_MtCO2e_excluded_excluded.rename(columns={'emissions[MtCO2e]': 'ind_emissions-by-em-or-capt-material[MtCO2e]'})
    out_1_3 = pd.concat([out_9376_1, out_9377_1.set_index(out_9377_1.index.astype(str) + '_dup')])
    out_1_3 = pd.concat([out_1_3, out_9441_1.set_index(out_9441_1.index.astype(str) + '_dup')])
    out_1_3 = pd.concat([out_1_3, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # Include domestic
    emissions_MtCO2e_excluded_2 = emissions_MtCO2e_excluded.loc[emissions_MtCO2e_excluded['domestic-type'].isin(['domestic'])].copy()
    emissions_MtCO2e_excluded_excluded = emissions_MtCO2e_excluded.loc[~emissions_MtCO2e_excluded['domestic-type'].isin(['domestic'])].copy()
    emissions_MtCO2e_excluded_excluded = emissions_MtCO2e_excluded_excluded.loc[~emissions_MtCO2e_excluded_excluded['ets-or-not'].isin(['non-ETS'])].copy()
    emissions_MtCO2e_excluded_3 = pd.concat([emissions_MtCO2e_excluded_2, emissions_MtCO2e_excluded_excluded.set_index(emissions_MtCO2e_excluded_excluded.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Transport

    # Group by Country, Years,  transport-user, ets-or-not (sum)
    emissions_MtCO2e_excluded_3 = group_by_dimensions(df=emissions_MtCO2e_excluded_3, groupby_dimensions=['Country', 'Years', 'ets-or-not', 'transport-user'], aggregation_method='Sum')
    # Rename variable to tra_emissions-by- ets-transport-user[MtCO2e]
    out_9367_1 = emissions_MtCO2e_excluded_3.rename(columns={'emissions[MtCO2e]': 'tra_emissions-by-ets-transport-user[MtCO2e]'})
    # Group by Country, Years,  transport-user, (sum)
    emissions_MtCO2e_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded_2, groupby_dimensions=['Country', 'Years', 'transport-user'], aggregation_method='Sum')
    # Rename variable to tra_emissions-by-transport-user[MtCO2e]
    out_9364_1 = emissions_MtCO2e_excluded_2.rename(columns={'emissions[MtCO2e]': 'tra_emissions-by-transport-user[MtCO2e]'})
    # Group by Country, Years,  vehicle-type, domestic-type (sum)
    emissions_MtCO2e_excluded_2 = group_by_dimensions(df=emissions_MtCO2e_excluded, groupby_dimensions=['Country', 'Years', 'vehicule-type', 'domestic-type'], aggregation_method='Sum')
    # Rename variable to tra_emissions-by- vehicule-type[MtCO2e]
    out_9366_1 = emissions_MtCO2e_excluded_2.rename(columns={'emissions[MtCO2e]': 'tra_emissions-by-vehicule-type[MtCO2e]'})
    out_1_4 = pd.concat([out_9364_1, out_9366_1.set_index(out_9366_1.index.astype(str) + '_dup')])

    # For : Pathway Explorer
    # 
    # - Buildings

    # Group by Country, Years energy-vector(sum)
    emissions_MtCO2e_5 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to bld_emissions-by-carrier[MtCO2e]
    out_9354_1 = emissions_MtCO2e_5.rename(columns={'emissions[MtCO2e]': 'bld_emissions-by-carrier[MtCO2e]'})
    # Group by Country, Years,  building-type (sum)
    emissions_MtCO2e_5 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years', 'building-type'], aggregation_method='Sum')
    # Rename variable to bld_emissions-by-building-type[MtCO2e]
    out_9355_1 = emissions_MtCO2e_5.rename(columns={'emissions[MtCO2e]': 'bld_emissions-by-building-type[MtCO2e]'})
    # Group by Country, Years end-use (sum)
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_4, groupby_dimensions=['Country', 'Years', 'end-use'], aggregation_method='Sum')
    # Rename variable to bld_emissions-by-end-use[MtCO2e]
    out_9353_1 = emissions_MtCO2e_4.rename(columns={'emissions[MtCO2e]': 'bld_emissions-by-end-use[MtCO2e]'})
    out_1_2 = pd.concat([out_9353_1, out_9354_1.set_index(out_9354_1.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_9355_1.set_index(out_9355_1.index.astype(str) + '_dup')])
    emissions_MtCO2e_3 = emissions_MtCO2e_3.loc[~emissions_MtCO2e_3['way-of-production'].isin(['BioSyn'])].copy()
    emissions_MtCO2e_3 = emissions_MtCO2e_3.loc[~emissions_MtCO2e_3['vehicule-type'].isin(['aviation', 'marine'])].copy()

    # For : Pathway Explorer
    # 
    # - sum by module

    # Group by Country, Years,  origin-module (sum)
    emissions_MtCO2e_3 = group_by_dimensions(df=emissions_MtCO2e_3, groupby_dimensions=['Country', 'Years', 'emissions-or-capture', 'origin-module', 'sector'], aggregation_method='Sum')
    # replace  missing values by  empty cell
    emissions_MtCO2e_3 = missing_value(df=emissions_MtCO2e_3, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='total')
    # Set to 0
    emissions_MtCO2e_3 = missing_value(df=emissions_MtCO2e_3, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by Country, Years  (sum)
    emissions_MtCO2e_4 = group_by_dimensions(df=emissions_MtCO2e_3, groupby_dimensions=['Country', 'Years', 'emissions-or-capture'], aggregation_method='Sum')
    # Rename variable to clm_emissions[MtCO2e]
    out_9360_1 = emissions_MtCO2e_4.rename(columns={'emissions[MtCO2e]': 'clm_total-emissions[MtCO2e]'})
    # Rename variable to clm_emissions-by- em-or-capt-module-sector[MtCO2e]
    out_9359_1 = emissions_MtCO2e_3.rename(columns={'emissions[MtCO2e]': 'clm_emissions-by-em-or-capt-module-sector[MtCO2e]', 'origin-module': 'module'})
    out_1_5 = pd.concat([out_9359_1, out_9360_1.set_index(out_9360_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1_5, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Top: origin-module=ind
    emissions_MtCO2e = emissions_MtCO2e.loc[emissions_MtCO2e['emissions-or-capture'].isin(['emissions'])].copy()
    # Top: origin-module=ind
    emissions_MtCO2e = emissions_MtCO2e.loc[emissions_MtCO2e['origin-module'].isin(['ind'])].copy()
    # Group by Country, Years, material, route, tech (sum)
    emissions_MtCO2e = group_by_dimensions(df=emissions_MtCO2e, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # emissions[MtCO2e] = emissions[MtCO2e] + emissions[MtCO2e]
    emissions_MtCO2eq = mcd(input_table_1=emissions_MtCO2e, input_table_2=emissions_MtCO2eq, operation_selection='x + y', output_name='emissions[MtCO2eq]')
    # emissions[MtCO2e] (including scope 2)
    emissions_MtCO2eq_2 = use_variable(input_table=emissions_MtCO2eq, selected_variable='emissions[MtCO2eq]')
    # ind_emissions-per-energy-demand[MtCO2eq/TWh] = emissions[MtCO2e] / energy-demand[TWh]
    ind_emissions_per_energy_demand_MtCO2eq_per_TWh = mcd(input_table_1=emissions_MtCO2eq_2, input_table_2=energy_demand_TWh, operation_selection='x / y', output_name='ind_emissions-per-energy-demand[MtCO2eq/TWh]')
    # remove divided by zero
    ind_emissions_per_energy_demand_MtCO2eq_per_TWh = missing_value(df=ind_emissions_per_energy_demand_MtCO2eq_per_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Convert Unit MtCO2eq/TWh  to tCO2eq/kWh
    ind_emissions_per_energy_demand_tCO2eq_per_kWh = ind_emissions_per_energy_demand_MtCO2eq_per_TWh.drop(columns='ind_emissions-per-energy-demand[MtCO2eq/TWh]').assign(**{'ind_emissions-per-energy-demand[tCO2eq/kWh]': ind_emissions_per_energy_demand_MtCO2eq_per_TWh['ind_emissions-per-energy-demand[MtCO2eq/TWh]'] * 0.001})
    # ind_emissions-per-energy-demand[tCO2eq/kWh]
    ind_emissions_per_energy_demand_tCO2eq_per_kWh = export_variable(input_table=ind_emissions_per_energy_demand_tCO2eq_per_kWh, selected_variable='ind_emissions-per-energy-demand[tCO2eq/kWh]')
    # material-production[Mt]
    material_production_Mt = use_variable(input_table=industry, selected_variable='material-production[Mt]')
    # material-production[Mt]
    material_production_Mt = use_variable(input_table=material_production_Mt, selected_variable='material-production[Mt]')
    # Keep way-of-production = carbstone, e-MTO, e-dehydration, dry-kiln
    material_production_Mt_2 = material_production_Mt.loc[material_production_Mt['technology'].isin(['dry-kiln', 'carbstone', 'e-MTO', 'e-dehydration'])].copy()
    # Group by year, country, technology (sum)
    material_production_Mt_2 = group_by_dimensions(df=material_production_Mt_2, groupby_dimensions=['Country', 'Years', 'technology'], aggregation_method='Sum')
    # Rename variable technology to way-of-production
    out_9481_1 = material_production_Mt_2.rename(columns={'technology': 'way-of-production'})
    # ind_specific-sequestration[MtCO2eq/Mt] = emissions[MtCO2e] / material-production[Mt]
    ind_specific_sequestration_MtCO2eq_per_Mt = mcd(input_table_1=emissions_MtCO2e_2, input_table_2=out_9481_1, operation_selection='x / y', output_name='ind_specific-sequestration[MtCO2eq/Mt]')
    # remove divided by zero
    ind_specific_sequestration_MtCO2eq_per_Mt = missing_value(df=ind_specific_sequestration_MtCO2eq_per_Mt, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # ind_specific-sequestration[MtCO2eq/Mt]
    ind_specific_sequestration_MtCO2eq_per_Mt = export_variable(input_table=ind_specific_sequestration_MtCO2eq_per_Mt, selected_variable='ind_specific-sequestration[MtCO2eq/Mt]')
    # Group by Country, Years, material, route, tech (sum)
    material_production_Mt = group_by_dimensions(df=material_production_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # specific-emission[MtCO2eq/Mt] = emissions[MtCO2e] / material-production[Mt]
    specific_emission_MtCO2eq_per_Mt = mcd(input_table_1=emissions_MtCO2eq, input_table_2=material_production_Mt, operation_selection='x / y', output_name='specific-emission[MtCO2eq/Mt]')
    # remove divided by zero
    specific_emission_MtCO2eq_per_Mt = missing_value(df=specific_emission_MtCO2eq_per_Mt, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Rename variable to ind_specific-emissions[MtCO2eq/Mt]
    out_9472_1 = specific_emission_MtCO2eq_per_Mt.rename(columns={'specific-emission[MtCO2eq/Mt]': 'ind_specific-emission[MtCO2eq/Mt]'})
    # Convert Unit MtCO2eq/Mt to tCO2eq/t
    ind_specific_emission_tCO2eq_per_t = out_9472_1.drop(columns='ind_specific-emission[MtCO2eq/Mt]').assign(**{'ind_specific-emission[tCO2eq/t]': out_9472_1['ind_specific-emission[MtCO2eq/Mt]'] * 1.0})
    # ind_specific-emissions[MtCO2eq/Mt]
    ind_specific_emission_tCO2eq_per_t = export_variable(input_table=ind_specific_emission_tCO2eq_per_t, selected_variable='ind_specific-emission[tCO2eq/t]')
    ind_tCO2eq_per = pd.concat([ind_specific_emission_tCO2eq_per_t, ind_emissions_per_energy_demand_tCO2eq_per_kWh.set_index(ind_emissions_per_energy_demand_tCO2eq_per_kWh.index.astype(str) + '_dup')])
    out_9515_1 = pd.concat([ind_tCO2eq_per, clm_ratio_delta_emissions_over_delta_stored_percent.set_index(clm_ratio_delta_emissions_over_delta_stored_percent.index.astype(str) + '_dup')])
    out_9485_1 = pd.concat([ind_specific_sequestration_MtCO2eq_per_Mt, out_9515_1.set_index(out_9515_1.index.astype(str) + '_dup')])
    # Aggregation table for carrier
    out_9368_1 = pd.DataFrame(columns=['energy-carrier', 'energy-carrier-agg'], data=[['liquid-syn-kerosene', 'liquid-syn-agg'], ['liquid-ff-kerosene', 'liquid-ff-agg'], ['liquid-bio-kerosene', 'liquid-bio-agg'], ['gaseous-syn', 'gaseous-syn'], ['liquid-syn-diesel', 'liquid-syn-agg'], ['liquid-syn-marinefueloil', 'liquid-syn-agg'], ['liquid-syn-gasoline', 'liquid-syn-agg'], ['gaseous-ff-natural', 'gaseous-ff-natural'], ['liquid-ff-diesel', 'liquid-ff-agg'], ['liquid-ff-marinefueloil', 'liquid-ff-agg'], ['liquid-ff-gasoline', 'liquid-ff-agg'], ['gaseous-bio', 'gaseous-bio'], ['liquid-bio-diesel', 'liquid-bio-agg'], ['liquid-bio-marinefueloil', 'liquid-bio-agg'], ['liquid-bio-gasoline', 'liquid-bio-agg'], ['hydrogen', 'hydrogen']])
    # Add energy-carrier-agg
    out_9394_1 = joiner(df_left=emissions_MtCO2e_excluded, df_right=out_9368_1, joiner='inner', left_input=['energy-carrier'], right_input=['energy-carrier'])
    # Remove energy-carrier
    out_9394_1 = column_filter(df=out_9394_1, columns_to_drop=['energy-carrier'])
    # energy-carrier-agg to energy-carrier
    out_9369_1 = out_9394_1.rename(columns={'energy-carrier-agg': 'energy-carrier'})
    # Group by Country, Years,  energy-carrier, ets-or-not (sum)
    out_9369_1 = group_by_dimensions(df=out_9369_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'ets-or-not'], aggregation_method='Sum')
    # Rename variable to tra_emissions-by- ets-carrier[MtCO2e]
    out_9373_1 = out_9369_1.rename(columns={'emissions[MtCO2e]': 'tra_emissions-by-ets-carrier[MtCO2e]'})
    out_1_5 = pd.concat([out_9367_1, out_9373_1.set_index(out_9373_1.index.astype(str) + '_dup')])
    out_1_4 = pd.concat([out_1_4, out_1_5.set_index(out_1_5.index.astype(str) + '_dup')])
    out_1_3 = pd.concat([out_1_4, out_1_3.set_index(out_1_3.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_1_3.set_index(out_1_3.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_9485_1, out_1.set_index(out_1.index.astype(str) + '_dup')])

    return out_1, out_3774_1


