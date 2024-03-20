import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


# Bioenergy Balance module
# (module-name = agriculture)
def bioenergy_balance(industry, agriculture, land_use, power):
    # Emissions : Not used in old module !
    # 
    # Should be done here ? Or send to Climate Module ? (not send to Climate neither in old module) !
    # 
    # Emission factors used (hard coded in old version) :
    # - solid biomass : 388 ktCO2/TWh
    # - liquid biomass : 256 ktCO2/TWh
    # - gas biomass : 197 ktCO2/TWh


    # Warning :
    # Potential production of energy from wood biomass is very complicated to estimate as it depends on several parameters that are not taken into account in this model.
    #  
    # The estimation of this potential in the future is done by multiplying the quantity of wood which is harvested annually by the share of the harvested wood which was used to produce bioenergy in 2021. As this share was relatively stable in previous years, it is considered that it stays stable until 2050.


    # For : Pathway Explorer
    # 
    # - Land management [ha]
    # => Detailled by type of land-use


    # TEMP > delete when previous has been migrated



    module_name = 'agriculture'

    # Formating data for other modules + Pathway Explorer

    # Pathway Explorer
    # - Energy demand [TWh] => by vector and sector

    # Aggregation table for carrier
    out_9323_1 = pd.DataFrame(columns=['energy-carrier', 'energy-carrier-agg'], data=[['liquid-bio', 'liquid-bio-agg'], ['liquid-bio-diesel', 'liquid-bio-agg'], ['liquid-bio-gasoline', 'liquid-bio-agg'], ['liquid-bio-kerosene', 'liquid-bio-agg'], ['liquid-bio-marinefueloil', 'liquid-bio-agg'], ['solid-biomass', 'solid-biomass'], ['gaseous-bio', 'gaseous-bio']])

    # Adapt data from other module => pivot and co

    # Energy and material - demand and production

    # Bioenergy demand
    # 
    # Comes from power supply module
    # => Should we keep waste ? What to do with them then ?

    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=power, selected_variable='energy-demand[TWh]')
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['energy-carrier'].isin(['solid-waste', 'solid-waste-nonres', 'solid-waste-res'])].copy()
    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # GASEOUS, LIQUID and SOLID bioenergy balance
    # 
    # We compare the demand with the production
    # If demand > production => We need to import the extra demand / Else, we export biomass

    # energy-demand [TWh]
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Bioenergy and biomaterial production
    # 
    # Comes from industry and land-use modules

    # potential-production-for-industry [t]
    potential_production_for_industry_t = use_variable(input_table=land_use, selected_variable='potential-production-for-industry[t]')
    # energy-production [TWh]
    energy_production_TWh_3 = use_variable(input_table=land_use, selected_variable='energy-production[TWh]')
    # OTS (only) energy-production [TWh] (additionnal production not modelised = sludges / agro-food-industrial wastes)
    energy_production_TWh = import_data(trigram='agr', variable_name='energy-production', variable_type='OTS (only)')
    # Same as last available year
    energy_production_TWh_2 = add_missing_years(df_data=energy_production_TWh)
    # wood-production [m3]
    wood_production_m3 = use_variable(input_table=land_use, selected_variable='wood-production[m3]')
    # OTS (only) forest-harvest-for-bioenergy [%]
    forest_harvest_for_bioenergy_percent = import_data(trigram='agr', variable_name='forest-harvest-for-bioenergy', variable_type='OTS (only)')
    # Same as last available year
    forest_harvest_for_bioenergy_percent = add_missing_years(df_data=forest_harvest_for_bioenergy_percent)
    # potential-production-for-industry[m3] = wood-production[m3] * (1 - forest-harvest-for-bioenergy[%])
    potential_production_for_industry_m3 = mcd(input_table_1=wood_production_m3, input_table_2=forest_harvest_for_bioenergy_percent, operation_selection='x * (1-y)', output_name='potential-production-for-industry[m3]')
    # RCP : forest-m3-to-t-conv-factor [t/m3]
    forest_m3_to_t_conv_factor_t_per_m3 = import_data(trigram='agr', variable_name='forest-m3-to-t-conv-factor', variable_type='RCP')
    # potential-production-for-industry[t] = potential-energy-production[m3] * forest-m3-to-t-conv-factor [t/m3]
    potential_production_for_industry_t_2 = mcd(input_table_1=potential_production_for_industry_m3, input_table_2=forest_m3_to_t_conv_factor_t_per_m3, operation_selection='x * y', output_name='potential-production-for-industry[t]')
    potential_production_for_industry_t = pd.concat([potential_production_for_industry_t_2, potential_production_for_industry_t])
    # potential-production-for-industry [t]
    potential_production_for_industry_t = export_variable(input_table=potential_production_for_industry_t, selected_variable='potential-production-for-industry[t]')

    # SOLID bio-material ; demand and production
    # 
    # Note : 
    # As we do not have the total demand (only wood and paper are modeled in industry), we can not compute balance here
    # So, we only aggregate all demand and production

    # potential-production-for-industry [t]
    potential_production_for_industry_t = use_variable(input_table=potential_production_for_industry_t, selected_variable='potential-production-for-industry[t]')
    # Group by  Country, Years, origin (sum)
    potential_production_for_industry_t = group_by_dimensions(df=potential_production_for_industry_t, groupby_dimensions=['Country', 'Years', 'origin'], aggregation_method='Sum')

    # Pathway Explorer
    # - Bio-material production [t]

    # potential-production-for-industry [t]
    potential_production_for_industry_t = use_variable(input_table=potential_production_for_industry_t, selected_variable='potential-production-for-industry[t]')
    # potential-energy-production[m3] = wood-production[m3] * forest-harvest-for-bioenergy[%]
    potential_energy_production_m3 = mcd(input_table_1=wood_production_m3, input_table_2=forest_harvest_for_bioenergy_percent, operation_selection='x * y', output_name='potential-energy-production[m3]')
    # RCP : forest-m3-to-TWh-conv-factor [TWh/m3]
    forest_m3_to_TWh_conv_factor_TWh_per_m3 = import_data(trigram='agr', variable_name='forest-m3-to-TWh-conv-factor', variable_type='RCP')
    # energy-production[TWh] = potential-energy-production[m3] * forest-m3-to-TWh-conv-factor [TWh/m3]
    energy_production_TWh = mcd(input_table_1=potential_energy_production_m3, input_table_2=forest_m3_to_TWh_conv_factor_TWh_per_m3, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by Country, Years, energy-carrier, origin (sum)
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'origin', 'energy-carrier'], aggregation_method='Sum')
    # energy-production [TWh]
    energy_production_TWh_4 = use_variable(input_table=agriculture, selected_variable='energy-production[TWh]')
    energy_production_TWh_3 = pd.concat([energy_production_TWh_4, energy_production_TWh_3])
    energy_production_TWh_2 = pd.concat([energy_production_TWh_3, energy_production_TWh_2])
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh])
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # % imported vs produced

    # Group by  Country, Years (sum)
    energy_production_TWh_2 = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # energy-production [TWh]
    energy_production_TWh_3 = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_3 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # buffer[TWh] = energy-demand[TWh] - energy-production[TWh]  LEFT JOIN => if missing set 0 (no productionà
    buffer_TWh = mcd(input_table_1=energy_demand_TWh_2, input_table_2=energy_production_TWh_3, operation_selection='x - y', output_name='buffer[TWh]', fill_value_bool='Left [x] Outer Join')
    # energy-demand[TWh] = 0 if buffer > 0 else : abs(buffer)  (EXPORT)
    energy_demand_TWh_2 = buffer_TWh.copy()
    mask = energy_demand_TWh_2['buffer[TWh]']>0
    energy_demand_TWh_2.loc[mask, 'energy-demand[TWh]'] = 0
    energy_demand_TWh_2.loc[~mask, 'energy-demand[TWh]'] = abs(energy_demand_TWh_2.loc[~mask, 'buffer[TWh]'])
    # sector = export
    energy_demand_TWh_2['sector'] = "export"
    # energy-demand [TWh]
    energy_demand_TWh_2 = export_variable(input_table=energy_demand_TWh_2, selected_variable='energy-demand[TWh]')
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh])

    # Aggregate all energy demand (including export)

    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Join of inputs
    out_693_1 = joiner(df_left=energy_demand_TWh, df_right=out_9323_1, joiner='outer', left_input=['energy-carrier'], right_input=['energy-carrier'])
    # energy-carrier-new to energy-carrier
    out_9325_1 = out_693_1.rename(columns={'energy-carrier-agg': 'energy-carrier', 'energy-carrier': 'energy-carrier_old'})
    # Group by  carrier (sum)
    out_9325_1_2 = group_by_dimensions(df=out_9325_1, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to energy-demand-by-carrier[TWh]
    out_9009_1 = out_9325_1_2.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier[TWh]'})
    # Group by  carrier, sector (sum)
    out_9325_1 = group_by_dimensions(df=out_9325_1, groupby_dimensions=['Country', 'Years', 'sector', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to energy-demand-by-carrier-sector[TWh]
    out_9506_1 = out_9325_1.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier-sector[TWh]'})
    # Group by  sector (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['sector'].isin(['dhg'])].copy()
    # Rename variable to energy-demand-by-sector[TWh]
    out_9007_1 = energy_demand_TWh.rename(columns={'energy-demand[TWh]': 'energy-demand-by-sector[TWh]'})
    out_1 = pd.concat([out_9007_1, out_9009_1])
    out_1_2 = pd.concat([out_1, out_9506_1])
    # energy-production[TWh] = buffer If < 0 => set 0  (IMPORT)
    energy_production_TWh_3 = buffer_TWh.copy()
    mask = energy_production_TWh_3['buffer[TWh]']<0
    energy_production_TWh_3.loc[mask, 'energy-production[TWh]'] = 0
    energy_production_TWh_3.loc[~mask, 'energy-production[TWh]'] = energy_production_TWh_3.loc[~mask, 'buffer[TWh]']
    # origin = import
    energy_production_TWh_3['origin'] = "import"
    # energy-production [TWh]
    energy_production_TWh_3 = export_variable(input_table=energy_production_TWh_3, selected_variable='energy-production[TWh]')

    # Pathway Explorer
    # - Bio-material demand [t]

    # energy-production [TWh] (only import)
    energy_production_TWh_4 = use_variable(input_table=energy_production_TWh_3, selected_variable='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_4, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # energy-imported [TWh]
    out_9509_1 = energy_production_TWh_4.rename(columns={'energy-production[TWh]': 'energy-imported[TWh]'})
    # Module = Scope 2/3
    out_9509_1 = column_filter(df=out_9509_1, pattern='^.*$')
    # Group by  Country, Years (sum)
    energy_production_TWh_4 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # total-energy-supply[TWh] = energy-production[TWh] (other than import) + energy-production[TWh] (import)
    total_energy_supply_TWh = mcd(input_table_1=energy_production_TWh_2, input_table_2=energy_production_TWh_4, operation_selection='x + y', output_name='total-energy-supply[TWh]')
    # pct-bioenergy-produced[%] = energy-production[TWh] (other than import) / total-energy-supply[TWh]
    pct_bioenergy_produced_percent = mcd(input_table_1=energy_production_TWh_2, input_table_2=total_energy_supply_TWh, operation_selection='x / y', output_name='pct-bioenergy-produced[%]')

    # Pathway Explorer
    # - % energy import and % energy produced locally

    # pct-bioenergy-produced [%]
    pct_bioenergy_produced_percent = use_variable(input_table=pct_bioenergy_produced_percent, selected_variable='pct-bioenergy-produced[%]')
    # pct-bioenergy-imported[%] = energy-production[TWh] (import) / total-energy-supply[TWh]
    pct_bioenergy_imported_percent = mcd(input_table_1=energy_production_TWh_4, input_table_2=total_energy_supply_TWh, operation_selection='x / y', output_name='pct-bioenergy-imported[%]')
    # pct-bioenergy-imported [%]
    pct_bioenergy_imported_percent = use_variable(input_table=pct_bioenergy_imported_percent, selected_variable='pct-bioenergy-imported[%]')
    pct_bioenergy_percent = pd.concat([pct_bioenergy_imported_percent, pct_bioenergy_produced_percent])
    energy_production_TWh = pd.concat([energy_production_TWh_3, energy_production_TWh])

    # Aggregate all energy production (including export)

    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # Pathway Explorer
    # - Energy production [TWh]

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Rename variable to bio-energy-production- by-energy-carrier-origin[TWh]
    out_9431_1 = energy_production_TWh.rename(columns={'energy-production[TWh]': 'bio-energy-production-by-energy-carrier-origin[TWh]'})

    # Bio-material demand
    # 
    # Comes from industrial material demand

    # material-production [Mt]
    material_production_Mt = use_variable(input_table=industry, selected_variable='material-production[Mt]')
    # Convert Unit Mt to t
    material_production_t = material_production_Mt.drop(columns='material-production[Mt]').assign(**{'material-production[t]': material_production_Mt['material-production[Mt]'] * 1000000.0})
    # material-production [t] (from industry)
    material_production_t = export_variable(input_table=material_production_t, selected_variable='material-production[t]')
    # material-production [t] (from industry)
    material_production_t = use_variable(input_table=material_production_t, selected_variable='material-production[t]')
    # Group by  Country, Years, material (sum)
    material_production_t = group_by_dimensions(df=material_production_t, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Include wood, paper (material)
    material_production_t = material_production_t.loc[material_production_t['material'].isin(['paper', 'wood'])].copy()

    # Pathway Explorer
    # - Bio-material demand [t]

    # material-production [t]
    material_production_t = use_variable(input_table=material_production_t, selected_variable='material-production[t]')
    # Rename variable to bio-material-demand[t]
    out_9434_1 = material_production_t.rename(columns={'material-production[t]': 'bio-material-demand[t]'})
    out_9339_1 = pd.concat([out_9434_1, potential_production_for_industry_t])
    out_9436_1 = pd.concat([pct_bioenergy_percent, out_9339_1])
    out_1 = pd.concat([out_9431_1, out_9436_1])
    out_1 = pd.concat([out_1_2, out_1])
    out_9342_1 = add_trigram(module_name=module_name, df=out_1)
    # Module = Pathway Explorer
    out_9342_1 = column_filter(df=out_9342_1, pattern='^.*$')
    # Keep Country, Years only  (temp solution)
    out_9342_1_2 = column_filter(df=out_9342_1, pattern='^Country$|^Years$')
    # Module = Climate  SHOULD BE DONE ??!!
    out_9342_1_2 = column_filter(df=out_9342_1_2, pattern='^.*$')
    # Years
    out_7498_1 = out_9342_1_2.assign(Years=out_9342_1_2['Years'].astype(str))

    return out_9342_1, out_7498_1, out_9509_1


