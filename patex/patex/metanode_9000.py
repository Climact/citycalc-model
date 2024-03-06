import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_9000(port_01, port_02, port_03, port_04, port_05):

    # sector = tra
    port_02['sector'] = "tra"
    # sector = bld
    port_01['sector'] = "bld"
    port = pd.concat([port_01, port_02.set_index(port_02.index.astype(str) + '_dup')])

    # Sankey diagram preparation
    # OU EST-IL UTILISE ??

    # elc_net-energy-production-by- carrier-primary-carrier-way-of-prod [TWh]
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh = use_variable(input_table=port_05, selected_variable='elc_net-energy-production-by-carrier-primary-carrier-way-of-prod[TWh]')
    # Group by  Country, Years, primary-energy-carrier (sum)
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2 = group_by_dimensions(df=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh, groupby_dimensions=['Country', 'Years', 'primary-energy-carrier'], aggregation_method='Sum')
    # Remove primary-energy-carrier with no values ("")
    out_8986_1 = row_filter(df=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2, filter_type='StringComp_RowFilter', that_column='primary-energy-carrier', include=True, pattern='..*', case_sensitive=True, is_reg_exp=True)
    # metric as energy_sankey_values[TWh]  primary-energy-carrier as energy_sankey_destination
    out_8982_1 = out_8986_1.rename(columns={'primary-energy-carrier': 'energy_sankey_destination', 'elc_net-energy-production-by-carrier-primary-carrier-way-of-prod[TWh]': 'energy_sankey_values[TWh]'})
    # Add energy_sankey_origin = elc
    out_8982_1['energy_sankey_origin'] = "elc"

    # Join Sectors

    # elc_energy-demand-by- energy-carrier[TWh]
    elc_energy_demand_by_energy_carrier_TWh = use_variable(input_table=port_05, selected_variable='elc_energy-demand-by-energy-carrier[TWh]')
    # Variable to energy-demand[TWh]
    out_8998_1 = elc_energy_demand_by_energy_carrier_TWh.rename(columns={'elc_energy-demand-by-energy-carrier[TWh]': 'energy-demand[TWh]'})
    # sector = elc
    out_8998_1['sector'] = "elc"
    # Keep energy-carrier = electricity
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2 = elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh.loc[elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh['energy-carrier'].isin(['electricity'])].copy()
    # Group by Country, Years, energy-carrier (SUM)
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh = group_by_dimensions(df=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # Keep way-of-prod = elec-plant-with : - nuclear - liquid - solid-coal - gaz (= non-RES)  rest = other way-of-prod (=RES)
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_3 = elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2.loc[elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2['way-of-production'].isin(['elec-plant-with-gas', 'elec-plant-with-liquid', 'elec-plant-with-nuclear', 'elec-plant-with-solid-coal'])].copy()
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded = elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2.loc[~elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2['way-of-production'].isin(['elec-plant-with-gas', 'elec-plant-with-liquid', 'elec-plant-with-nuclear', 'elec-plant-with-solid-coal'])].copy()
    # Group by Country, Years, energy-carrier (SUM)
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded = group_by_dimensions(df=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # res-type = RES
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded['res-type'] = "RES"
    # Group by Country, Years, energy-carrier (SUM)
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2 = group_by_dimensions(df=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # res-type = nonRES
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2['res-type'] = "nonRES"
    elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2 = pd.concat([elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2, elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded.set_index(elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_excluded.index.astype(str) + '_dup')])
    # pct-RES[%] = net-prod-xx[TWh] / net-prod-xx[TWh] total
    pct_RES_percent = mcd(input_table_1=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh, input_table_2=elc_net_energy_production_by_carrier_primary_carrier_way_of_prod_TWh_2, operation_selection='y / x', output_name='pct-RES[%]')
    # sector = agr
    port_03['sector'] = "agr"
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=port_04, selected_variable='energy-demand[TWh]')
    # sector = ind
    energy_demand_TWh['sector'] = "ind"
    out_3929_1 = pd.concat([energy_demand_TWh, port_03.set_index(port_03.index.astype(str) + '_dup')])
    out_3930_1 = pd.concat([port, out_3929_1.set_index(out_3929_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_3930_1, out_8998_1.set_index(out_8998_1.index.astype(str) + '_dup')])
    # Remove : - heat - ambiant ?! - marinefueoil (bio, syn, ff) - kersoene (bio, syn, ff)
    out_3931_1 = row_filter(df=out_1, filter_type='StringComp_RowFilter', that_column='energy-carrier', include=False, pattern='heat|.*kerosene.*|.*marinefueloil.*|ambiant', case_sensitive=True, is_reg_exp=True)
    # Keep : - marinefueoil (bio, syn, ff) - kersoene (bio, syn, ff)
    out_3946_1 = row_filter(df=out_1, filter_type='StringComp_RowFilter', that_column='energy-carrier', include=True, pattern='.*kerosene.*|.*marinefueloil.*', case_sensitive=True, is_reg_exp=True)
    # sector = tra-bunkers
    out_3946_1['sector'] = "tra-bunkers"
    out_1 = pd.concat([out_3946_1, out_3931_1.set_index(out_3931_1.index.astype(str) + '_dup')])
    # Group by  Country, Years, energy-carrier, sector (sum)
    out_1 = group_by_dimensions(df=out_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # Remove energy-carrier = eletctricity
    out_1_excluded = out_1.loc[out_1['energy-carrier'].isin(['electricity'])].copy()
    out_1 = out_1.loc[~out_1['energy-carrier'].isin(['electricity'])].copy()
    # energy-demand[TWh] (replace) = energy-demand[TWh] * pct-RES[%]
    energy_demand_TWh = mcd(input_table_1=out_1_excluded, input_table_2=pct_RES_percent, operation_selection='x * y', output_name='energy-demand[TWh]')
    # Group by  Country, Years, energy-carrier, sector, res-type (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector', 'res-type'], aggregation_method='Sum')
    # RES or NOT depending on energy-carrier SHOULD BE SET IN GOOGLE SHEET ?!
    out_3937_1 = pd.DataFrame(columns=['energy-carrier', 'res-type'], data=[['gaseous-bio', 'RES'], ['gaseous-ff', 'nonRES'], ['gaseous-ff-natural', 'nonRES'], ['gaseous-syn', 'RES'], ['hydrogen', 'RES'], ['liquid-bio', 'RES'], ['liquid-bio-diesel', 'RES'], ['liquid-bio-gasoline', 'RES'], ['liquid-bio-kerosene', 'RES'], ['liquid-bio-marinefueloil', 'RES'], ['liquid-ff', 'nonRES'], ['liquid-ff-crudeoil', 'nonRES'], ['liquid-ff-diesel', 'nonRES'], ['liquid-ff-gasoline', 'nonRES'], ['liquid-ff-kerosene', 'nonRES'], ['liquid-ff-marinefueloil', 'nonRES'], ['liquid-ff-oil', 'nonRES'], ['liquid-syn', 'RES'], ['liquid-syn-diesel', 'RES'], ['liquid-syn-gasoline', 'RES'], ['liquid-syn-kerosene', 'RES'], ['liquid-syn-marinefueloil', 'RES'], ['solid-biomass', 'RES'], ['solid-ff-coal', 'nonRES'], ['solid-syn', 'RES'], ['solid-waste', 'RES']])
    out_3936_1 = joiner(df_left=out_1, df_right=out_3937_1, joiner='left', left_input=['energy-carrier'], right_input=['energy-carrier'])
    # Group by  Country, Years, sector, energy-carrier (sum)
    out_3936_1_2 = group_by_dimensions(df=out_3936_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    out_8968_1 = out_3936_1_2.rename(columns={'energy-demand[TWh]': 'energy_sankey_values[TWh]', 'sector': 'energy_sankey_origin', 'energy-carrier': 'energy_sankey_destination'})
    out_1 = pd.concat([out_8968_1, out_8982_1.set_index(out_8982_1.index.astype(str) + '_dup')])
    # Years
    out_8984_1 = out_1.assign(Years=out_1['Years'].astype(str))
    out_3950_1 = pd.concat([out_3936_1, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # Group by  Country, Years, sector, res-type (sum)
    out_3950_1_2 = group_by_dimensions(df=out_3950_1, groupby_dimensions=['Country', 'Years', 'sector', 'res-type'], aggregation_method='Sum')
    # res_energy-demand-by-sector[TWh]
    out_8987_1 = out_3950_1_2.rename(columns={'energy-demand[TWh]': 'res_energy-demand-by-sector[TWh]'})
    # Group by  Country, Years, res-type (sum)
    out_3950_1_2 = group_by_dimensions(df=out_3950_1, groupby_dimensions=['Country', 'Years', 'res-type'], aggregation_method='Sum')
    # Group by  Country, Years, (sum)
    out_3950_1 = group_by_dimensions(df=out_3950_1, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # res_energy-demand[%] = energy-demand[TWh] by res-type / energy-demand[TWh] total
    res_energy_demand_percent = mcd(input_table_1=out_3950_1_2, input_table_2=out_3950_1, operation_selection='x / y', output_name='res_energy-demand[%]')
    out_8988_1 = pd.concat([out_8987_1, res_energy_demand_percent.set_index(res_energy_demand_percent.index.astype(str) + '_dup')])

    return out_8988_1, out_8984_1


