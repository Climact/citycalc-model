import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def scope_2_3(power, industry, agriculture, bioenergy_balance):

    module_name = 'scope_2_3'

    # From Energy (incl bio) import to CO2-equivalent emissions

    # from ELC energy-imported[TWh]
    energy_imported_TWh = use_variable(input_table=power, selected_variable='energy-imported[TWh]')
    # OTS/FTS metric-name [unit]
    indirect_emission_factor_unit = import_data(trigram='sco', variable_name='indirect-emission-factor')
    # Keep EF energy
    indirect_emission_factor_unit_2 = indirect_emission_factor_unit.loc[indirect_emission_factor_unit['category'].isin(['energy-fugitive', 'energy-losses', 'energy-production', 'energy-scope2', 'bioenergy-production'])].copy()
    indirect_emission_factor_unit_excluded_2 = indirect_emission_factor_unit.loc[~indirect_emission_factor_unit['category'].isin(['energy-fugitive', 'energy-losses', 'energy-production', 'energy-scope2', 'bioenergy-production'])].copy()
    # Convert Unit tCO2/MWh  to  MtCO2/TWh
    indirect_emission_factor_MtCO2e_per_TWh = indirect_emission_factor_unit_2.drop(columns='indirect-emission-factor[tCO2e/unit]').assign(**{'indirect-emission-factor[MtCO2e/TWh]': indirect_emission_factor_unit_2['indirect-emission-factor[tCO2e/unit]'] * 1.0})
    # subcategory to energy-carrier
    out_9600_1 = indirect_emission_factor_MtCO2e_per_TWh.rename(columns={'subcategory': 'energy-carrier'})
    # keep energy-scope2
    out_9600_1_2 = out_9600_1.loc[out_9600_1['category'].isin(['energy-scope2'])].copy()
    out_9600_1_excluded = out_9600_1.loc[~out_9600_1['category'].isin(['energy-scope2'])].copy()
    # keep energy-losses
    out_9600_1_excluded_2 = out_9600_1_excluded.loc[out_9600_1_excluded['category'].isin(['energy-losses'])].copy()
    out_9600_1_excluded_excluded = out_9600_1_excluded.loc[~out_9600_1_excluded['category'].isin(['energy-losses'])].copy()
    # keep energy-production
    out_9600_1_excluded_excluded_2 = out_9600_1_excluded_excluded.loc[out_9600_1_excluded_excluded['category'].isin(['energy-production'])].copy()
    out_9600_1_excluded_excluded_excluded = out_9600_1_excluded_excluded.loc[~out_9600_1_excluded_excluded['category'].isin(['energy-production'])].copy()
    # top energy-fugitive bottom bioenergy-production
    out_9600_1_excluded_excluded_excluded_2 = out_9600_1_excluded_excluded_excluded.loc[out_9600_1_excluded_excluded_excluded['category'].isin(['energy-fugitive'])].copy()
    out_9600_1_excluded_excluded_excluded_excluded = out_9600_1_excluded_excluded_excluded.loc[~out_9600_1_excluded_excluded_excluded['category'].isin(['energy-fugitive'])].copy()
    # keep EF agr
    indirect_emission_factor_unit_excluded = indirect_emission_factor_unit_excluded_2.loc[indirect_emission_factor_unit_excluded_2['category'].isin(['food-product', 'food-raw'])].copy()
    indirect_emission_factor_unit_excluded_excluded_2 = indirect_emission_factor_unit_excluded_2.loc[~indirect_emission_factor_unit_excluded_2['category'].isin(['food-product', 'food-raw'])].copy()
    # top food-raw bottom food-product
    indirect_emission_factor_unit_excluded_2 = indirect_emission_factor_unit_excluded.loc[indirect_emission_factor_unit_excluded['category'].isin(['food-raw'])].copy()
    indirect_emission_factor_unit_excluded_excluded = indirect_emission_factor_unit_excluded.loc[~indirect_emission_factor_unit_excluded['category'].isin(['food-raw'])].copy()
    # subcategory to raw-material
    out_9617_1 = indirect_emission_factor_unit_excluded_2.rename(columns={'subcategory': 'raw-material'})
    # subcategory to product
    out_9618_1 = indirect_emission_factor_unit_excluded_excluded.rename(columns={'subcategory': 'product'})
    # Keep EF industry
    indirect_emission_factor_unit_excluded_excluded = indirect_emission_factor_unit_excluded_excluded_2.loc[indirect_emission_factor_unit_excluded_excluded_2['category'].isin(['industry-material', 'industry-product', 'industry-subproduct'])].copy()
    # keep industry-material
    indirect_emission_factor_unit_excluded_excluded_2 = indirect_emission_factor_unit_excluded_excluded.loc[indirect_emission_factor_unit_excluded_excluded['category'].isin(['industry-material'])].copy()
    indirect_emission_factor_unit_excluded_excluded_excluded = indirect_emission_factor_unit_excluded_excluded.loc[~indirect_emission_factor_unit_excluded_excluded['category'].isin(['industry-material'])].copy()
    # subcategory to material
    out_9612_1 = indirect_emission_factor_unit_excluded_excluded_2.rename(columns={'subcategory': 'material'})
    # top industry-product bottom industry-subproduct
    indirect_emission_factor_unit_excluded_excluded_excluded_2 = indirect_emission_factor_unit_excluded_excluded_excluded.loc[indirect_emission_factor_unit_excluded_excluded_excluded['category'].isin(['industry-product'])].copy()
    indirect_emission_factor_unit_excluded_excluded_excluded_excluded = indirect_emission_factor_unit_excluded_excluded_excluded.loc[~indirect_emission_factor_unit_excluded_excluded_excluded['category'].isin(['industry-product'])].copy()
    # subcategory to subproduct
    out_9614_1 = indirect_emission_factor_unit_excluded_excluded_excluded_excluded.rename(columns={'subcategory': 'subproduct'})
    # subcategory to product
    out_9613_1 = indirect_emission_factor_unit_excluded_excluded_excluded_2.rename(columns={'subcategory': 'product'})
    # subcategory to energy-carrier
    out_9628_1 = energy_imported_TWh.rename(columns={'sector': 'sector-scope2'})
    # keep electricity [TWh]
    out_9628_1 = out_9628_1.loc[out_9628_1['energy-carrier'].isin(['electricity'])].copy()
    # emissions-indirect[MtCO2e] (energy-scope2) = indirect-emission-factor[MtCO2e/TWh] x imported-energy[TWh]
    emissions_indirect_MtCO2e_2 = mcd(input_table_1=out_9600_1_2, input_table_2=out_9628_1, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e = group_by_dimensions(df=emissions_indirect_MtCO2e_2, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    # emissions-indirect to emissions-scope2
    out_9629_1 = emissions_indirect_MtCO2e_2.rename(columns={'emissions-indirect[MtCO2e]': 'emissions-scope2[MtCO2e]'})

    # For : Pathway Explorer
    # - emissions scope 2 by sector-scope2 (which demands electricity)

    # emissions scope 2 by sector-scope2
    emissions_scope2_MtCO2e = use_variable(input_table=out_9629_1, selected_variable='emissions-scope2[MtCO2e]')
    # Group by year, country,  sector-scope2
    emissions_scope2_MtCO2e = group_by_dimensions(df=emissions_scope2_MtCO2e, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector-scope2'], aggregation_method='Sum')
    # keep only bld, tra, ind  and agr
    emissions_scope2_MtCO2e_2 = emissions_scope2_MtCO2e.loc[emissions_scope2_MtCO2e['sector-scope2'].isin(['agr', 'bld', 'ind', 'tra'])].copy()
    emissions_scope2_MtCO2e_excluded = emissions_scope2_MtCO2e.loc[~emissions_scope2_MtCO2e['sector-scope2'].isin(['agr', 'bld', 'ind', 'tra'])].copy()
    # Group By:  Sum the following sectors  DAC, efuels, heat hydrogen, losses, refineries  to a single sector = elc
    emissions_scope2_MtCO2e_excluded = group_by_dimensions(df=emissions_scope2_MtCO2e_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Add Constant colum sector = elc
    out_9524_1 = emissions_scope2_MtCO2e_excluded.copy()
    out_9524_1['sector-scope2'] = 'elc'
    # Contact sectors  with elc
    out_9525_1 = pd.concat([emissions_scope2_MtCO2e_2, out_9524_1])
    # emissions-indirect[MtCO2e] (energy-losses) = indirect-emission-factor[MtCO2e/TWh] x imported-energy[TWh]
    emissions_indirect_MtCO2e_2 = mcd(input_table_1=out_9600_1_excluded_2, input_table_2=out_9628_1, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_2 = group_by_dimensions(df=emissions_indirect_MtCO2e_2, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    emissions_indirect_MtCO2e = pd.concat([emissions_indirect_MtCO2e, emissions_indirect_MtCO2e_2])
    # Group by country, years,  energy-carrier (sum)
    energy_imported_TWh = group_by_dimensions(df=energy_imported_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # emissions-indirect[MtCO2e] (energy-fugitive) = indirect-emission-factor[MtCO2e/TWh] x imported-energy[TWh]
    emissions_indirect_MtCO2e_2 = mcd(input_table_1=out_9600_1_excluded_excluded_excluded_2, input_table_2=energy_imported_TWh, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_2 = group_by_dimensions(df=emissions_indirect_MtCO2e_2, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    # emissions-indirect[MtCO2e] (energy-production) = indirect-emission-factor[MtCO2e/TWh] x imported-energy[TWh]
    emissions_indirect_MtCO2e_3 = mcd(input_table_1=out_9600_1_excluded_excluded_2, input_table_2=energy_imported_TWh, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_3 = group_by_dimensions(df=emissions_indirect_MtCO2e_3, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_2 = pd.concat([emissions_indirect_MtCO2e_3, emissions_indirect_MtCO2e_2])
    emissions_indirect_MtCO2e = pd.concat([emissions_indirect_MtCO2e, emissions_indirect_MtCO2e_2])
    # set  "sector-import" to elc
    emissions_indirect_MtCO2e['sector-import'] = "elc"

    # For : Pathway Explorer
    # - emissions indirect (scope 2 + 3) by sector-import (which demand induces indirect emissions)

    # Group by country, years (sum)
    emissions_indirect_MtCO2e_2 = group_by_dimensions(df=emissions_indirect_MtCO2e, groupby_dimensions=['Years', 'Country', 'sector-import'], aggregation_method='Sum')
    emissions_indirect_MtCO2e = pd.concat([emissions_indirect_MtCO2e, emissions_indirect_MtCO2e_2])

    # From Industry import to CO2-equivalent emissions

    # from IND material-import[t]
    material_import_t = use_variable(input_table=industry, selected_variable='material-import[t]')
    # Group by country, years,  category (sum)
    material_import_t = group_by_dimensions(df=material_import_t, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # emissions-indirect[tCO2e] (industry-material) = indirect-emission-factor[tCO2e/t] x material-import[t]
    emissions_indirect_tCO2e = mcd(input_table_1=out_9612_1, input_table_2=material_import_t, operation_selection='x * y', output_name='emissions-indirect[tCO2e]')
    # Convert Unit tCO2e to  MtCO2e
    emissions_indirect_MtCO2e_2 = emissions_indirect_tCO2e.drop(columns='emissions-indirect[tCO2e]').assign(**{'emissions-indirect[MtCO2e]': emissions_indirect_tCO2e['emissions-indirect[tCO2e]'] * 1e-06})
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_2 = group_by_dimensions(df=emissions_indirect_MtCO2e_2, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    # from IND product-import[unit]
    product_import_unit = use_variable(input_table=industry, selected_variable='product-import[unit]')
    # Group by country, years,  category (sum)
    product_import_unit = group_by_dimensions(df=product_import_unit, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # emissions-indirect[tCO2e] (industry-product) = indirect-emission-factor[tCO2e/t] x industry-product[t]
    emissions_indirect_MtCO2e_3 = mcd(input_table_1=out_9613_1, input_table_2=product_import_unit, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Convert Unit tCO2e to  MtCO2e
    emissions_indirect_MtCO2e_3 = emissions_indirect_MtCO2e_3.drop(columns='emissions-indirect[MtCO2e]').assign(**{'emissions-indirect[MtCO2e]': emissions_indirect_MtCO2e_3['emissions-indirect[MtCO2e]'] * 1e-06})
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_3 = group_by_dimensions(df=emissions_indirect_MtCO2e_3, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_2 = pd.concat([emissions_indirect_MtCO2e_2, emissions_indirect_MtCO2e_3])

    # From Agriculture import to CO2-equivalent emissions

    # from AGR food-net-import-product[kcal]
    food_net_import_product_kcal = use_variable(input_table=agriculture, selected_variable='food-net-import-product[kcal]')
    # Convert Unit kcal to  Mkcal
    food_net_import_product_Mkcal = food_net_import_product_kcal.drop(columns='food-net-import-product[kcal]').assign(**{'food-net-import-product[Mkcal]': food_net_import_product_kcal['food-net-import-product[kcal]'] * 1e-06})
    # emissions-indirect[tCO2e] (food-product) = indirect-emission-factor[tCO2e/Mkcal] x food-product[Mkcal]
    emissions_indirect_tCO2e = mcd(input_table_1=out_9618_1, input_table_2=food_net_import_product_Mkcal, operation_selection='x * y', output_name='emissions-indirect[tCO2e]')
    # Convert Unit tCO2e to  MtCO2e
    emissions_indirect_MtCO2e_3 = emissions_indirect_tCO2e.drop(columns='emissions-indirect[tCO2e]').assign(**{'emissions-indirect[MtCO2e]': emissions_indirect_tCO2e['emissions-indirect[tCO2e]'] * 1e-06})
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_3 = group_by_dimensions(df=emissions_indirect_MtCO2e_3, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    # from BIO energy-imported[TWh]
    energy_imported_TWh = use_variable(input_table=bioenergy_balance, selected_variable='energy-imported[TWh]')
    # emissions-indirect[MtCO2e] (bioenergy-production) = energy-imported[TWh]  x  indirect-emission-factor[MtCO2e/TWh]
    emissions_indirect_MtCO2e_4 = mcd(input_table_1=out_9600_1_excluded_excluded_excluded_excluded, input_table_2=energy_imported_TWh, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_4 = group_by_dimensions(df=emissions_indirect_MtCO2e_4, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    # from AGR food-net-import[kcal]
    food_net_import_kcal = use_variable(input_table=agriculture, selected_variable='food-net-import[kcal]')
    # Convert Unit kcal to  Mkcal
    food_net_import_Mkcal = food_net_import_kcal.drop(columns='food-net-import[kcal]').assign(**{'food-net-import[Mkcal]': food_net_import_kcal['food-net-import[kcal]'] * 1e-06})
    # emissions-indirect[tCO2e] (food-raw) = indirect-emission-factor[tCO2e/Mkcal] x food-raw[Mkcal]
    emissions_indirect_tCO2e = mcd(input_table_1=out_9617_1, input_table_2=food_net_import_Mkcal, operation_selection='x * y', output_name='emissions-indirect[tCO2e]')
    # Convert Unit tCO2e to  MtCO2e
    emissions_indirect_MtCO2e_5 = emissions_indirect_tCO2e.drop(columns='emissions-indirect[tCO2e]').assign(**{'emissions-indirect[MtCO2e]': emissions_indirect_tCO2e['emissions-indirect[tCO2e]'] * 1e-06})
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_5 = group_by_dimensions(df=emissions_indirect_MtCO2e_5, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_4 = pd.concat([emissions_indirect_MtCO2e_4, emissions_indirect_MtCO2e_5])
    emissions_indirect_MtCO2e_3 = pd.concat([emissions_indirect_MtCO2e_4, emissions_indirect_MtCO2e_3])
    # set  "sector-import" to agr
    emissions_indirect_MtCO2e_3['sector-import'] = "agr"
    # Group by country, years (sum)
    emissions_indirect_MtCO2e_4 = group_by_dimensions(df=emissions_indirect_MtCO2e_3, groupby_dimensions=['Years', 'Country', 'sector-import'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_3 = pd.concat([emissions_indirect_MtCO2e_3, emissions_indirect_MtCO2e_4])
    # from IND subproduct-import[unit]
    subproduct_import_unit = use_variable(input_table=industry, selected_variable='subproduct-import[unit]')
    # Group by country, years,  category (sum)
    subproduct_import_unit = group_by_dimensions(df=subproduct_import_unit, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # emissions-indirect[tCO2e] (industry-subproduct) = indirect-emission-factor[tCO2e/t] x industry-subproduct[t]
    emissions_indirect_MtCO2e_4 = mcd(input_table_1=out_9614_1, input_table_2=subproduct_import_unit, operation_selection='x * y', output_name='emissions-indirect[MtCO2e]')
    # Convert Unit tCO2e to  MtCO2e
    emissions_indirect_MtCO2e_4 = emissions_indirect_MtCO2e_4.drop(columns='emissions-indirect[MtCO2e]').assign(**{'emissions-indirect[MtCO2e]': emissions_indirect_MtCO2e_4['emissions-indirect[MtCO2e]'] * 1e-06})
    # Group by country, years,  category (sum)
    emissions_indirect_MtCO2e_4 = group_by_dimensions(df=emissions_indirect_MtCO2e_4, groupby_dimensions=['Years', 'Country', 'category'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_2 = pd.concat([emissions_indirect_MtCO2e_2, emissions_indirect_MtCO2e_4])
    # set  "sector-import" to ind
    emissions_indirect_MtCO2e_2['sector-import'] = "ind"
    # Group by country, years (sum)
    emissions_indirect_MtCO2e_4 = group_by_dimensions(df=emissions_indirect_MtCO2e_2, groupby_dimensions=['Years', 'Country', 'sector-import'], aggregation_method='Sum')
    emissions_indirect_MtCO2e_2 = pd.concat([emissions_indirect_MtCO2e_2, emissions_indirect_MtCO2e_4])
    emissions_indirect_MtCO2e_2 = pd.concat([emissions_indirect_MtCO2e_3, emissions_indirect_MtCO2e_2])
    emissions_indirect_MtCO2e = pd.concat([emissions_indirect_MtCO2e, emissions_indirect_MtCO2e_2])
    out_9626_1 = pd.concat([out_9525_1, emissions_indirect_MtCO2e])
    out_8244_1 = add_trigram(module_name=module_name, df=out_9626_1)
    # Module = Pathways Explorer
    out_8244_1 = column_filter(df=out_8244_1, pattern='^.*$')

    return out_8244_1


