import pandas as pd

from patex.nodes.node import Globals, PythonNode, FlowVars
from patex.nodes import *


def metanode_9763(port_01, port_02):
    # For WARNING !
    # If < 0 => We export RES electricity production !


    # In future :
    # 
    # Account for these savings in costs (// industry)


    # Note : new-capacities by timestep
    # => should be yearly capacities ?
    # If yes : add missing years (timestep should be = to 1) and divide new-capapcities / timestep to get value for these years


    # Capacity-factor for RES
    # 
    # We assume capacity-factor of biogas = capacity-factor of gas-natural


    # Note : other-biogas disapear as there is no lifespan value for it !
    # => lifespan for biogas = lifespan for gas ??


    # Total Capacities


    # Concatenate of a same variable
    # coming from differents sources
    # 
    # Here : net-energy-production[TWh]



    module_name = 'electricity_supply'

    # ELECTRICITY Production

    # Electricity production (and primary energy demand)

    # Apply fuel-switch levers 
    # => Change the use of different fuels :
    # Ex : 
    # solid-ff to lsolid-bio
    # liquid-ff to liquid-bio
    # gas-ff to gas-bio
    # waste non-res to waste-res and solid-bio

    # Note : we don't take account of the efficiency ratio here as the switch of fuel is done in the same "fuel category". We don't expect any changes of efficiency from one fuel to another inside a same category.
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # 
    # Hard codé ici ==> à terme, introduire dans une google sheet !

    # Correlation for fuel mix
    out_9507_1 = pd.DataFrame(columns=['category-from', 'primary-energy-carrier-from', 'category-to', 'primary-energy-carrier-to'], data=[['fffuels', 'liquid-ff', 'biofuel', 'liquid-bio'], ['fffuels', 'solid-ff-coal', 'biofuel', 'solid-biomass'], ['fffuels', 'gaseous-ff-natural', 'biofuel', 'gaseous-bio'], ['nonres', 'solid-waste-nonres', 'res', 'solid-waste-res'], ['nonres', 'solid-waste-nonres', 'biofuel', 'solid-biomass']])
    # ratio[-] = 1
    ratio = out_9507_1.assign(**{'ratio[-]': 1.0})

    # Electricity demand (from other sectors)

    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=port_02, selected_variable='energy-demand[TWh]')
    # Keep only energy-carrier electricity
    energy_demand_TWh = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['electricity'])].copy()
    # All sector except losses / refineries
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['losses', 'refineries'])].copy()
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['sector'].isin(['losses', 'refineries'])].copy()
    # Group by  country, years, energy-carrier
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Add electricity demand due to losses / refineries

    # energy-demand [TWh] coming from losses / refineries
    energy_demand_TWh_2 = use_variable(input_table=energy_demand_TWh_excluded, selected_variable='energy-demand[TWh]')
    # Group by  country, years, energy-carrier
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Apply net-import levers 
    # => determine level of production required to fill the demand
    # => Done on all elec demand (except demand dues to losses / refineries)

    # OTS / FTS energy-net-import
    energy_net_import = import_data(trigram='elc', variable_name='energy-net-import')
    # energy-imported[TWh] = energy-demand[TWh] * energy-net-import[%]
    energy_imported_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_net_import, operation_selection='x * y', output_name='energy-imported[TWh]')

    # Determine extra import / export
    # of electricity
    # 
    # Extra import / export = 
    # energy demand (after RES) - net production
    # 
    # If > 0 => import
    # If < 0 => export

    # energy-imported [TWh]
    energy_imported_TWh_2 = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # energy-producted[TWh] = energy-demand[TWh] - energy-imported[TWh]
    energy_producted_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_imported_TWh, operation_selection='x - y', output_name='energy-producted[TWh]')

    # Total elec required
    # [TWh]

    # energy-to-be-produced[TWh] = energy-producted[TWh] + energy-demand[TWh] coming from losses / refineries
    energy_to_be_produced_TWh = mcd(input_table_1=energy_producted_TWh, input_table_2=energy_demand_TWh_2, operation_selection='x + y', output_name='energy-to-be-produced[TWh]')

    # Apply capacity levers on RES and other way of prod (nuclear / fossil)
    # => determine level of electricity production we can achieve with national RES technologies
    # => determine level of electricity production we can achieve with national fossil and nuclear technologies
    # => Gives GROSS PRODUCTION of ELECTRICITY

    # OTS / FTS capacity[GW]
    capacity_GW = import_data(trigram='elc', variable_name='capacity')
    # Set 0 (cfr Poland 2020 for solid-bio-waste)
    capacity_GW_2 = missing_value(df=capacity_GW, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # HTS capacity-factor [%]
    capacity_factor_percent = import_data(trigram='elc', variable_name='capacity-factor', variable_type='HTS')

    # Calculate non-self-consumption[%]
    # We want to account for the self consumption of each plants BUT as well of the consumption linked to Carbon Capture (this should be done before the end of this sub-module as CC will required Electricity demand and we want to avoid loop inside the model).
    # 1. We compute CC consumption ( = CC specific energy consumption [MWh/tCO2] x emission-factor [tCO2 x MWh] x CC rate [%])
    # 2. We compute non-self-consumption for each plant (= 1 - self-consumption)
    # 3. We compute non-self-consumption for CC (= 1 - CC-self-consumption)
    # 4. We compute final non-self-consumption ( = 2. x 3.)

    # OTS (only) self-consumption [%]
    self_consumption_percent = import_data(trigram='elc', variable_name='self-consumption', variable_type='OTS (only)')
    # RCP elc-emission-factor [g/kWh]
    elc_emission_factor_g_per_kWh = import_data(trigram='elc', variable_name='elc-emission-factor', variable_type='RCP')
    # RCP CC-specific-energy-consumption [TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt = import_data(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')
    # Keep co2-concentration = low (for power production concentration is always low)
    CC_specific_energy_consumption_TWh_per_Mt = CC_specific_energy_consumption_TWh_per_Mt.loc[CC_specific_energy_consumption_TWh_per_Mt['co2-concentration'].isin(['low'])].copy()
    # energy-carrier as primary-energy-carrier
    out_9455_1 = CC_specific_energy_consumption_TWh_per_Mt.rename(columns={'energy-carrier': 'primary-energy-carrier'})

    # Apply CC energy efficiency levers 
    # (linked to ind energy efficiency !)
    # => determine the quantity of energy saving when CC is applied thanks to improvement of energy efficiency

    # OTS / FTS CC-energy-efficiency [%]  FROM INDUSTRY (we should transfer data to TECHNOLOGY)
    CC_energy_efficiency_percent = import_data(trigram='ind', variable_name='CC-energy-efficiency')
    # CC-specific-energy-consumption[TWh/Mt] (replace) = CC-specific-energy-consumption[TWh/Mt] * (1 - CC-energy-efficiency[%])
    CC_specific_energy_consumption_TWh_per_Mt = mcd(input_table_1=out_9455_1, input_table_2=CC_energy_efficiency_percent, operation_selection='x * (1-y)', output_name='CC-specific-energy-consumption[TWh/Mt]')

    # GHG emissions
    # For carbon-capture and energy required for carbon capture : apply emission factor of fossil fuels !
    # As emission factor of bio- and syn-fuels are set to 0 to represent the fact that CO2 emissions were previously captured by vegetation / H2 syn, it is not possible to determine CO2 captured based on these emissions.
    # In order to have these values : we assume bio- / syn-fuels emit the same way as there fossil fuel equivalent and apply, then, carbon capture and energy required for CC on these emissions
    # => Liquid fuels (bio / syn / ff) : liquid fossil emission factor
    # => Gaseous fuels (bio / syn / ff) : gaseous fossil emission factor
    # => Solid fuels (bio / syn / ff) : solid fossil emission factor

    # Energy demand for CC
    # This is already accounted in the primary energy demand of the sector through self-consumption factor.
    # This is only required to create a graph.

    # CC-specific-energy-consumption[TWh/Mt]
    CC_specific_energy_consumption_TWh_per_Mt_2 = use_variable(input_table=CC_specific_energy_consumption_TWh_per_Mt, selected_variable='CC-specific-energy-consumption[TWh/Mt]')
    # primary-energy-carrier to energy-carrier
    out_9504_1 = CC_specific_energy_consumption_TWh_per_Mt_2.rename(columns={'primary-energy-carrier': 'energy-carrier'})
    # Convert Unit g/kWh to Mt/TWh (* 0.001)
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_g_per_kWh.drop(columns='elc-emission-factor[g/kWh]').assign(**{'elc-emission-factor[Mt/TWh]': elc_emission_factor_g_per_kWh['elc-emission-factor[g/kWh]'] * 0.001})
    # CC-specific-consumption[TWh/TWh] = elc-emission-factor[Mt/TWh] * CC-specific-energy-consumption [TWh/Mt]
    CC_specific_consumption_TWh_per_TWh = mcd(input_table_1=elc_emission_factor_Mt_per_TWh, input_table_2=CC_specific_energy_consumption_TWh_per_Mt, operation_selection='x * y', output_name='CC-specific-consumption[TWh/TWh]')
    # Group by  Country (SUM)
    CC_specific_consumption_TWh_per_TWh = group_by_dimensions(df=CC_specific_consumption_TWh_per_TWh, groupby_dimensions=['Country'], aggregation_method='Sum')
    # OTS / FTS : CCS-ratio-elec [%]
    CCS_ratio_elec_percent = import_data(trigram='elc', variable_name='CCS-ratio-elec')
    # Group by  Country, Years (SUM)
    CCS_ratio_elec_percent_2 = group_by_dimensions(df=CCS_ratio_elec_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # CC-self-consumption[%] = CC-specific-consumption[TWh/TWh] * CCS-ratio-elec[%]
    CC_self_consumption_percent = mcd(input_table_1=CC_specific_consumption_TWh_per_TWh, input_table_2=CCS_ratio_elec_percent_2, operation_selection='x * y', output_name='CC-self-consumption[%]')
    # CC-non-self-consumption[%] = 1 - CC-self-consumption[%]
    CC_non_self_consumption_percent = CC_self_consumption_percent.assign(**{'CC-non-self-consumption[%]': 1.0-CC_self_consumption_percent['CC-self-consumption[%]']})
    # CC-non-self-consumption [%]
    CC_non_self_consumption_percent = use_variable(input_table=CC_non_self_consumption_percent, selected_variable='CC-non-self-consumption[%]')
    # Same as last available year
    self_consumption_percent = add_missing_years(df_data=self_consumption_percent)
    # non-self-consumption[%] = 1 - self-consumption[%]
    non_self_consumption_percent = self_consumption_percent.assign(**{'non-self-consumption[%]': 1.0-self_consumption_percent['self-consumption[%]']})
    # non-self-consumption [%]
    non_self_consumption_percent = use_variable(input_table=non_self_consumption_percent, selected_variable='non-self-consumption[%]')
    # non-self-consumption[%] (replace) = non-self-consumption[%] * CC-non-self-consumption[%]
    non_self_consumption_percent = mcd(input_table_1=non_self_consumption_percent, input_table_2=CC_non_self_consumption_percent, operation_selection='x * y', output_name='non-self-consumption[%]')
    # Same as last available year
    capacity_factor_percent = add_missing_years(df_data=capacity_factor_percent)

    def helper_6726(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        return output_table
    # Join capacity and capacity-factor in one table 
    out_6726_1 = helper_6726(input_table_1=capacity_GW_2, input_table_2=capacity_factor_percent)
    # gross-production[GW] = capacity[GW] * capacity-factor[%]
    gross_production_GW = mcd(input_table_1=capacity_GW, input_table_2=capacity_factor_percent, operation_selection='x * y', output_name='gross-production[GW]')
    # Unit conversion GW to TWh
    gross_production_TWh = gross_production_GW.drop(columns='gross-production[GW]').assign(**{'gross-production[TWh]': gross_production_GW['gross-production[GW]'] * 8.766})
    # Keep way-of-prod RES and elec-plant
    gross_production_TWh = gross_production_TWh.loc[gross_production_TWh['way-of-production'].isin(['elec-plant-with-solid-bio-waste', 'RES-solar-pv', 'RES-geothermal', 'RES-hydroelectric', 'RES-marine', 'RES-solar-csp', 'elec-plant-with-gas', 'elec-plant-with-liquid', 'elec-plant-with-solid-coal', 'elec-plant-with-nuclear', 'RES-wind-offshore', 'RES-wind-onshore'])].copy()
    # Set 0 (cfr Poland 2020 for solid-bio-waste)
    gross_production_TWh = missing_value(df=gross_production_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Change capacity-factor and/or capacity (fossil / nuclear) to reach the demand
    # And compute production (gross) with these new parameters


    def helper_6727(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        return output_table
    # Join gross-production to capacity and capacity-factor in one table
    out_6727_1 = helper_6727(input_table_1=out_6726_1, input_table_2=gross_production_TWh)

    def helper_6729(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='left', on=common_col)
        
        output_table['non-self-consumption[%]'] = output_table['non-self-consumption[%]'].fillna(1.0)
        return output_table
    # Join LEFT self-consumption to one table  If Nan for  non-self-consumption set to 1 
    out_6729_1 = helper_6729(input_table_1=out_6727_1, input_table_2=non_self_consumption_percent)

    # Remove electricity already produced by CHP

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=port_01, selected_variable='energy-production[TWh]')
    # Group by  country, years, energy-carrier
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # Total elec required
    # after CHP
    # [TWh]

    # energy-to-be-produced[TWh] (replace) = energy-to-be-produced[TWh] - energy-production[TWh] coming from CHP  Set 0 if no CHP production
    energy_to_be_produced_TWh = mcd(input_table_1=energy_to_be_produced_TWh, input_table_2=energy_production_TWh, operation_selection='x - y', output_name='energy-to-be-produced[TWh]', fill_value_bool='Left [x] Outer Join')

    def helper_6696(input_table_1, input_table_2) -> pd.DataFrame:
        import numpy as np
        
        # Get usefull flow variable
        baseyear = Globals.get().base_year
        
        # Input tables
        energy_to_produced_ini = input_table_1.copy()
        capacity_table_ini = input_table_2.copy()
        
        # Energy column :
        col_energy = 'energy-to-be-produced[TWh]'
        col_net_production = 'net-production[TWh]'
        col_gross_production = 'gross-production[TWh]'
        col_remaining = 'remaining-demand[TWh]'
        col_capacity_factor = 'capacity-factor[%]'
        col_capacity = 'capacity[GW]'
        col_non_self_conso = 'non-self-consumption[%]'
        
        # Order for increase capacity-factor: first = nuclear, then = gas, then = oil, then = coal
        way_of_prod_list = ["elec-plant-with-nuclear", "elec-plant-with-gas", "elec-plant-with-liquid", "elec-plant-with-solid-coal"] 
        
        # Constant values
        GW2TWh = 8.766 # Gigawatt years to TWh (*365*24/1000)
        max_capacity_factor = 0.85 #max capacity factor in %
        min_capacity_factor = 0
        
        
        ## ----------------------------------------- JOIN TABLE on COMMON COLS  ----------------------------------- ##
        # Get common columns
        common_cols = [col for col in input_table_1.head() if col in input_table_2.head()]
        # Merge on common cols
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_cols)
        
        ## ----------------------------------------- RECOMPUTE NET- and GROSS- PROD  ------------------------------- ##
        # Needed to have production by way-of-prod and not total value
        output_table[col_gross_production] = GW2TWh * output_table[col_capacity] * output_table[col_capacity_factor]
        output_table[col_net_production] = output_table[col_gross_production] * output_table[col_non_self_conso]
        
        ## ----------------------------------------- COMPUTE REMAINING  -------------------------------------------- ##
        # Group by Country / Years : sum net-production - mean energy-to-be-produced
        production_tot = output_table.groupby(['Country','Years'],as_index = False)[col_net_production].sum()
        production_tot.rename(columns={col_net_production:"total-net-prod"}, inplace=True)
        output_table = output_table.merge(production_tot, on=['Country','Years'], how='left')
        output_table[col_remaining] = output_table[col_energy] - output_table["total-net-prod"]
        output_table[col_remaining] = output_table[col_remaining].round(8) # Round to 8 number after decimal
        del output_table["total-net-prod"]
        
        
        ## ----------------------------------------- LOOP on WAY-OF-PROD  ---------------------------------------------------------- ##
        for i in range(0, len(way_of_prod_list)):
        
            # Get way of production
            way_of_prod = way_of_prod_list[i]
        
            # For current way of production :
            ## Keep only this way of prod (apply only on year > baseyear and remaining demand > 0)
            mask = (output_table[col_remaining] > 0) & (output_table["way-of-production"] == way_of_prod) & (output_table["Years"].astype(int) > int(Globals.get().base_year))
            table_to_keep = output_table.loc[mask, :]
            table_others = output_table.loc[~mask, :]
            ## Recompute capacity-factor
            table_to_keep[col_capacity_factor] = (table_to_keep[col_net_production] + table_to_keep[col_remaining]) / (GW2TWh * table_to_keep[col_capacity] * table_to_keep[col_non_self_conso])
            ## If capacity-factor > max => set to max value
            mask_max = (table_to_keep[col_capacity_factor] > max_capacity_factor)
            table_to_keep.loc[mask_max, col_capacity_factor] = max_capacity_factor
            ## Recompute net- and gross- production according to this new capacity-factor
            table_to_keep[col_gross_production] = GW2TWh * table_to_keep[col_capacity] * table_to_keep[col_capacity_factor]
            table_to_keep[col_net_production] = table_to_keep[col_gross_production] * table_to_keep[col_non_self_conso]
            ## Concat new-table with other table
            output_table = pd.concat([table_to_keep, table_others], ignore_index = True)
        
            # Recompute total net production > determine the level of remaining demand
            ## Group By : Country | Years : Sum net-prod
            production_tot = output_table.groupby(['Country', 'Years'], as_index = False)[col_net_production].agg('sum')
            production_tot = production_tot.rename(columns = {col_net_production : "total-net-prod"}, inplace = False)
            ## Join net prod to output_table
            output_table = output_table.merge(production_tot, how='left', on=["Country", "Years"])
            ## Recompute remaining demand = initial demand - total net production
            output_table[col_remaining] = output_table[col_energy] - output_table["total-net-prod"]
            output_table[col_remaining] = output_table[col_remaining].round(8) # Round to 8 number after decimal
            del output_table["total-net-prod"]
        return output_table
    # If remaining-demand > 0 Increase capacity-factor 1er : nuclear - 2er : gas 3er : oil - 4er : coal  if remaining > 0 : capacity-factor =  (net-production + remaining) / (capacity * GW2TWh * non-self-cons.)  max_value = 0.85  gross-production = capacity * capacity-factor (new) * GW2TWh net-production = gross-production * non-self-consumption 
    out_6696_1 = helper_6696(input_table_1=energy_to_be_produced_TWh, input_table_2=out_6729_1)

    def helper_6732(input_table) -> pd.DataFrame:
        import numpy as np
        
        # Get usefull flow variable
        baseyear = Globals.get().base_year
        
        # Input tables
        output_table = input_table.copy()
        
        # Energy column :
        col_energy = 'energy-to-be-produced[TWh]'
        col_net_production = 'net-production[TWh]'
        col_gross_production = 'gross-production[TWh]'
        col_remaining = 'remaining-demand[TWh]'
        col_capacity_factor = 'capacity-factor[%]'
        col_capacity = 'capacity[GW]'
        col_non_self_conso = 'non-self-consumption[%]'
        
        # Order for increase capacity : first (and only one) = gas
        way_of_prod_list = ["elec-plant-with-gas"] 
        
        # Constant values
        GW2TWh = 8.766 # Gigawatt years to TWh (*365*24/1000)
        max_capacity_factor = 0.85 #max capacity factor in %
        min_capacity_factor = 0
        
        
        ## ----------------------------------------- LOOP on WAY-OF-PROD  ---------------------------------------------------------- ##
        for i in range(0, len(way_of_prod_list)):
        
            # Get way of production
            way_of_prod = way_of_prod_list[i]
        
            # For current way of production :
            ## Keep only this way of prod (apply only on year > baseyear and remaining demand > 0)
            mask = (output_table[col_remaining] > 0) & (output_table["way-of-production"] == way_of_prod) & (output_table["Years"].astype(int) > int(Globals.get().base_year))
            table_to_keep = output_table.loc[mask, :]
            table_others = output_table.loc[~mask, :]
            ## Recompute capacity
            table_to_keep[col_capacity] = (table_to_keep[col_net_production] + table_to_keep[col_remaining]) / (GW2TWh * table_to_keep[col_capacity_factor] * table_to_keep[col_non_self_conso])
            ## Recompute net- and gross- production according to this new capacity-factor
            table_to_keep[col_gross_production] = GW2TWh * table_to_keep[col_capacity] * table_to_keep[col_capacity_factor]
            table_to_keep[col_net_production] = table_to_keep[col_gross_production] * table_to_keep[col_non_self_conso]
            ## Concat new-table with other table
            output_table = pd.concat([table_to_keep, table_others], ignore_index = True)
        
            # Recompute total net production > determine the level of remaining demand
            ## Group By : Country | Years : Sum net-prod
            production_tot = output_table.groupby(['Country', 'Years'], as_index = False)[col_net_production].agg('sum')
            production_tot = production_tot.rename(columns = {col_net_production : "total-net-prod"}, inplace = False)
            ## Join net prod to output_table
            output_table = output_table.merge(production_tot, how='left', on=["Country", "Years"])
            ## Recompute remaining demand = initial demand - total net production
            output_table[col_remaining] = output_table[col_energy] - output_table["total-net-prod"]
            output_table[col_remaining] = output_table[col_remaining].round(8) # Round to 8 number after decimal
            del output_table["total-net-prod"]
        return output_table
    # If still remaining-demand > 0 Increase capacity 1er : gas  if remaining > 0 : capacity =  (net-production + remaining) / (capacity-factor * GW2TWh * non-self-cons.)  gross-production = capacity * capacity-factor (new) * GW2TWh net-production = gross-production * non-self-consumption 
    out_6732_1 = helper_6732(input_table=out_6696_1)

    def helper_6733(input_table) -> pd.DataFrame:
        import numpy as np
        
        # Get usefull flow variable
        baseyear = Globals.get().base_year
        
        # Input tables
        output_table = input_table.copy()
        
        # Energy column :
        col_energy = 'energy-to-be-produced[TWh]'
        col_net_production = 'net-production[TWh]'
        col_gross_production = 'gross-production[TWh]'
        col_remaining = 'remaining-demand[TWh]'
        col_capacity_factor = 'capacity-factor[%]'
        col_capacity = 'capacity[GW]'
        col_non_self_conso = 'non-self-consumption[%]'
        
        # Order for decreasing capacity-factor : first = coal, then = oil, then = gas, then = nuclear
        way_of_prod_list = ["elec-plant-with-solid-coal", "elec-plant-with-liquid", "elec-plant-with-gas", "elec-plant-with-nuclear"] 
        
        # Constant values
        GW2TWh = 8.766 # Gigawatt years to TWh (*365*24/1000)
        max_capacity_factor = 0.85 #max capacity factor in %
        min_capacity_factor = 0
        
        
        ## ----------------------------------------- LOOP on WAY-OF-PROD  ---------------------------------------------------------- ##
        for i in range(0, len(way_of_prod_list)):
        
            # Get way of production
            way_of_prod = way_of_prod_list[i]
        
            # For current way of production :
            ## Keep only this way of prod (apply only on year > baseyear and remaining demand < 0)
            mask = (output_table[col_remaining] < 0) & (output_table["way-of-production"] == way_of_prod) & (output_table["Years"].astype(int) > int(Globals.get().base_year))
            table_to_keep = output_table.loc[mask, :]
            table_others = output_table.loc[~mask, :]
            ## Recompute capacity factor
            table_to_keep[col_capacity_factor] = (table_to_keep[col_net_production] + table_to_keep[col_remaining]) / (GW2TWh * table_to_keep[col_capacity] * table_to_keep[col_non_self_conso])
            ## If capacity-factor < min => set to min value
            mask_min = (table_to_keep[col_capacity_factor] < min_capacity_factor)
            table_to_keep.loc[mask_min, col_capacity_factor] = min_capacity_factor
            ## Recompute net- and gross- production according to this new capacity-factor
            table_to_keep[col_gross_production] = GW2TWh * table_to_keep[col_capacity] * table_to_keep[col_capacity_factor]
            table_to_keep[col_net_production] = table_to_keep[col_gross_production] * table_to_keep[col_non_self_conso]
            ## Concat new-table with other table
            output_table = pd.concat([table_to_keep, table_others], ignore_index = True)
        
            # Recompute total net production > determine the level of remaining demand
            ## Group By : Country | Years : Sum net-prod
            production_tot = output_table.groupby(['Country', 'Years'], as_index = False)[col_net_production].agg('sum')
            production_tot = production_tot.rename(columns = {col_net_production : "total-net-prod"}, inplace = False)
            ## Join net prod to output_table
            output_table = output_table.merge(production_tot, how='left', on=["Country", "Years"])
            ## Recompute remaining demand = initial demand - total net production
            output_table[col_remaining] = output_table[col_energy] - output_table["total-net-prod"]
            output_table[col_remaining] = output_table[col_remaining].round(8) # Round to 8 number after decimal
            del output_table["total-net-prod"]
        
        
        ## ----------------------------------------- FOR all WAY-OF-PROD  ---------------------------------------------------------- ##
        ## If production = 0 => capacity = 0 (if years > baseyear)
        mask = (output_table[col_gross_production] == 0)
        output_table.loc[mask, col_capacity] = 0
        ## If capacity = 0 => set capacity-factor to 0 as well
        mask = (output_table[col_capacity] == 0)
        output_table.loc[mask, col_capacity_factor] = 0
        return output_table
    # If remaining-demand < 0 Decrease capacity-factor 1er : coal - 2er : oil 3er : gas - 4er : nuclear  if remaining < 0 : capacity-factor =  (net-production + remaining) / (capacity * GW2TWh * non-self-cons.)  min_value = 0  gross-production = capacity * capacity-factor (new) * GW2TWh net-production = gross-production * non-self-consumption  If gross production = 0 => set capacity to 0 (applied on all way-of-production)
    out_6733_1 = helper_6733(input_table=out_6732_1)
    # non-self-consumption [%]
    non_self_consumption_percent = export_variable(input_table=out_6733_1, selected_variable='non-self-consumption[%]')

    # gross production
    # [TWh]
    # (non-RES + RES)
    # without fuel mix

    # gross-production [TWh]
    gross_production_TWh = export_variable(input_table=out_6733_1, selected_variable='gross-production[TWh]')

    # Production after self-consumption
    # => Gives NET PRODUCTION of ELECTRICITY

    # net-energy-production[TWh] = gross-production[TWh] * non-self-consumption[%]
    net_energy_production_TWh = mcd(input_table_1=non_self_consumption_percent, input_table_2=gross_production_TWh, operation_selection='x * y', output_name='net-energy-production[TWh]', fill_value_bool='Inner Join')

    # net production
    # [TWh]
    # (non-RES + RES)

    # net-energy-production [TWh]
    net_energy_production_TWh = export_variable(input_table=net_energy_production_TWh, selected_variable='net-energy-production[TWh]')

    # Compute extra demand => leading to extra import / export
    # => Compare energy-production and adapt import /export :
    # - If production > demand : export
    # - If production < demand : import
    # (Apply only for year > baseyear !)

    # Aggregation based on some columns
    # 
    # Here : Country / Years

    # SUM net-energy-production [TWh]
    net_energy_production_TWh_2 = group_by(df=net_energy_production_TWh, to_group=['Country', 'Years'], pattern=[], pattern_aggregation_method=[], method_list_manual=['Sum_V2.5.2'], aggregation_list_manual=['net-energy-production[TWh]'], to_aggregate_manual=['net-energy-production[TWh]'], name_policy='Keep original name(s)')

    # Formating data for other modules + Pathway Explorer

    # Energy - production (net) => we apply fuel mix here as we want to have the detail by primary-energy-carrier

    # net-energy-production [TWh]
    net_energy_production_TWh = use_variable(input_table=net_energy_production_TWh, selected_variable='net-energy-production[TWh]')

    # Determine primary energy-demand for electricity production
    # 
    # Primary energy demand [TWh] = fuel-mix [%] * gross production [TWh] / efficiency [%]

    # gross-production [TWh]
    gross_production_TWh = use_variable(input_table=gross_production_TWh, selected_variable='gross-production[TWh]')
    # OTS (only) fuel-mix-elec [%]
    fuel_mix_elec_percent = import_data(trigram='elc', variable_name='fuel-mix-elec', variable_type='OTS (only)')
    # Same as last available year
    fuel_mix_elec_percent = add_missing_years(df_data=fuel_mix_elec_percent)
    # gross-production[TWh] (replace) = gross-production[TWh] * fuel-mix[%]  LEFT Join If missing set to 1 
    gross_production_TWh = mcd(input_table_1=gross_production_TWh, input_table_2=fuel_mix_elec_percent, operation_selection='x * y', output_name='gross-production[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # OTS (only) energy-efficiency-elec [%]
    energy_efficiency_elec_percent = import_data(trigram='elc', variable_name='energy-efficiency-elec', variable_type='OTS (only)')
    # Same as last available year
    energy_efficiency_elec_percent = add_missing_years(df_data=energy_efficiency_elec_percent)
    # primary-energy-demand[TWh] = gross-production[TWh] / energy-efficiency-elec[%] 
    primary_energy_demand_TWh = mcd(input_table_1=gross_production_TWh, input_table_2=energy_efficiency_elec_percent, operation_selection='x / y', output_name='primary-energy-demand[TWh]')
    # Set 0 if divide by 0
    primary_energy_demand_TWh = missing_value(df=primary_energy_demand_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Calibration

    # Group by primary-energy-carrier (sum)
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'primary-energy-carrier'], aggregation_method='Sum')
    # Calibration: primary-energy-demand [TWh]
    elec_primary_energy_demand_TWh = import_data(trigram='elc', variable_name='elec-primary-energy-demand', variable_type='Calibration')
    # Group by primary-energy-carrier (sum)  We don't want to have the way of production because data are calibrated on the totals.
    elec_primary_energy_demand_TWh = group_by_dimensions(df=elec_primary_energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'primary-energy-carrier'], aggregation_method='Sum')
    # Apply Calibration on primary-energy-demand [TWh]
    _, out_7489_2, out_7489_3 = calibration(input_table=primary_energy_demand_TWh_2, cal_table=elec_primary_energy_demand_TWh, data_to_be_cal='primary-energy-demand[TWh]', data_cal='elec-primary-energy-demand[TWh]')
    # primary-energy-demand[TWh] (replace) = primary-energy-demand[TWh] * cal_rate  If missing cal_rate set to 1 
    primary_energy_demand_TWh = mcd(input_table_1=primary_energy_demand_TWh, input_table_2=out_7489_2, operation_selection='x * y', output_name='primary-energy-demand[TWh]')

    # Primary energy demand
    # (fossil / nuclear / RES)
    # [TWh]
    # before fuel switch

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = export_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # OTS / FTS fuel-switch [%]
    fuel_switch_percent = import_data(trigram='elc', variable_name='fuel-switch')
    # energy-carrier to primary-energy-carrier SHOULD BE DONE IN INPUT DATA INSTEAD !!!
    out_7565_1 = fuel_switch_percent.rename(columns={'energy-carrier-from': 'primary-energy-carrier-from', 'energy-carrier-to': 'primary-energy-carrier-to'})
    # Fuel Switch ff to bio
    out_9519_1 = x_switch(demand_table=primary_energy_demand_TWh, switch_table=out_7565_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='biofuel')
    # Fuel Switch non-res to res (waste)
    out_7567_1 = x_switch(demand_table=out_9519_1, switch_table=out_7565_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')
    # Fuel Switch non-res to solid-bio (waste)
    out_7568_1 = x_switch(demand_table=out_7567_1, switch_table=out_7565_1, correlation_table=ratio, col_energy='primary-energy-demand[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='biofuel')

    # Primary energy demand
    # (fossil / nuclear / RES)
    # [TWh]
    # after fuel switch

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = export_variable(input_table=out_7568_1, selected_variable='primary-energy-demand[TWh]')
    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=primary_energy_demand_TWh, selected_variable='primary-energy-demand[TWh]')
    # Group by  country, years, way-of-prod
    primary_energy_demand_TWh = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # primary-energy-demand to total-demand
    out_6762_1 = primary_energy_demand_TWh.rename(columns={'primary-energy-demand[TWh]': 'total-demand[TWh]'})
    # Group by  country, years, way-of-prod, primary-energy-carrier
    primary_energy_demand_TWh = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier'], aggregation_method='Sum')
    # pct_fuels[-] = primary-energy-demand[TWh] / total-demand[TWh] 
    pct_fuels = mcd(input_table_1=primary_energy_demand_TWh, input_table_2=out_6762_1, operation_selection='x / y', output_name='pct_fuels[-]')
    # Set to 0
    pct_fuels = missing_value(df=pct_fuels, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # net-energy-production[TWh] (replace) = pct_fuels[-] * net-energy-production[TWh]  Left Join If missing, set to 1
    net_energy_production_TWh_3 = mcd(input_table_1=net_energy_production_TWh, input_table_2=pct_fuels, operation_selection='x * y', output_name='net-energy-production[TWh]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # If missing txt set to "" (for RES plant no energy-carrier are given)
    net_energy_production_TWh_3 = missing_value(df=net_energy_production_TWh_3, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')

    # Primary energy demand for electricity production

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh = use_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')
    # sector = electricity
    primary_energy_demand_TWh['sector'] = "electricity"
    # emissions[Mt] = primary-energy-demand[TWh] * emission-factor[Mt/TWh]
    emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh_2, input_table_2=elc_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, gaes
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')

    # Calibration

    # Calibration: elec-emissions-Mt
    elec_emissions_Mt = import_data(trigram='elc', variable_name='elec-emissions-Mt', variable_type='Calibration')
    # Group by gaes (sum)  We don't want to have the way of production because data are calibrated on the totals.
    elec_emissions_Mt = group_by_dimensions(df=elec_emissions_Mt, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # Apply Calibration on emissions[Mt]
    _, out_7490_2, out_7490_3 = calibration(input_table=emissions_Mt_2, cal_table=elec_emissions_Mt, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')

    # Keep calibration rate

    # cal rate for emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out_7490_3, selected_variable='cal_rate_emissions[Mt]')
    # Add way-of-production = elec-plant
    cal_rate_emissions_Mt['way-of-production'] = "elec-plant"
    # emissions[Mt] (replace) = emissions[Mt] * cal_rate
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_7490_2, operation_selection='x * y', output_name='emissions[Mt]')
    elc_emission_factor_Mt_per_TWh = elc_emission_factor_Mt_per_TWh.loc[~elc_emission_factor_Mt_per_TWh['primary-energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'liquid-bio'])].copy()

    def helper_8017(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        values_needed = {  # LEFT = to be add if missing - RIGHT = values to use when missing
        "solid-biomass": "solid-ff",
        "liquid-bio": "liquid-ff",
        "liquid-syn": "liquid-ff",
        "gaseous-bio": "gaseous-ff",
        "gaseous-syn": "gaseous-ff",
        }
        
        # Get existing energy-carrier list
        enegy_carrier_list = output_table["primary-energy-carrier"].unique()
        
        # For values needed : if not in list => copy values from corresponding
        for val in values_needed.keys():
            if val not in enegy_carrier_list:
                val_mask = values_needed[val]
                mask = (output_table["primary-energy-carrier"].str.find(val_mask) >= 0)
                temp = output_table.loc[mask,:]
                temp["primary-energy-carrier"] = val
                output_table = pd.concat([output_table,temp], ignore_index=True)
        return output_table
    # syn / bio : add them with ff values
    out_8017_1 = helper_8017(input_table=elc_emission_factor_Mt_per_TWh)
    # insitu-emissions[Mt] = primary-energy-demand[TWh] * elc-emission-factor [Mt/TWh]
    insitu_emissions_Mt = mcd(input_table_1=primary_energy_demand_TWh_2, input_table_2=out_8017_1, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Apply cal-rate on insitu-emissions[Mt] 
    insitu_emissions_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=out_7490_2, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # cal rate for primary-energy-demand[TWh]
    cal_rate_primary_energy_demand_TWh = use_variable(input_table=out_7489_3, selected_variable='cal_rate_primary-energy-demand[TWh]')
    # Add way-of-production = elec-plant
    cal_rate_primary_energy_demand_TWh['way-of-production'] = "elec-plant"
    cal_rate = pd.concat([cal_rate_primary_energy_demand_TWh, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])

    # Calibration

    # If missing txt set to ""
    cal_rate = missing_value(df=cal_rate, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # Top way-of-production = elec-plant-with-nuclear
    gross_production_TWh_2 = gross_production_TWh.loc[gross_production_TWh['way-of-production'].isin(['elec-plant-with-nuclear'])].copy()
    gross_production_TWh_excluded = gross_production_TWh.loc[~gross_production_TWh['way-of-production'].isin(['elec-plant-with-nuclear'])].copy()
    # primary-energy-carrier = res
    gross_production_TWh_excluded = missing_value(df=gross_production_TWh_excluded, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='res')
    # primary-energy-carrier = non-res
    gross_production_TWh = missing_value(df=gross_production_TWh_2, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='non-res')
    gross_production_TWh = pd.concat([gross_production_TWh, gross_production_TWh_excluded.set_index(gross_production_TWh_excluded.index.astype(str) + '_dup')])

    # gross production
    # [TWh]
    # (non-RES + RES)
    # with fuel mix
    # before fuel switch

    # gross-production [TWh]
    gross_production_TWh = export_variable(input_table=gross_production_TWh, selected_variable='gross-production[TWh]')
    # Fuel Switch ff to elec-hp
    out_9520_1 = x_switch(demand_table=gross_production_TWh, switch_table=out_7565_1, correlation_table=ratio, col_energy='gross-production[TWh]', col_energy_carrier='primary-energy-carrier', category_to_selected='biofuel')
    # Fuel Switch non-res to res (waste)
    out_9512_1 = x_switch(demand_table=out_9520_1, switch_table=out_7565_1, correlation_table=ratio, col_energy='gross-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='res')
    # Fuel Switch non-res to solid-bio (waste)
    out_9513_1 = x_switch(demand_table=out_9512_1, switch_table=out_7565_1, correlation_table=ratio, col_energy='gross-production[TWh]', col_energy_carrier='primary-energy-carrier', category_from_selected='nonres', category_to_selected='biofuel')

    # gross production
    # [TWh]
    # (non-RES + RES)
    # with fuel mix
    # after fuel switch

    # gross-production [TWh]
    gross_production_TWh = export_variable(input_table=out_9513_1, selected_variable='gross-production[TWh]')

    # Energy - production (gross)

    # gross-production [TWh]
    gross_production_TWh = use_variable(input_table=gross_production_TWh, selected_variable='gross-production[TWh]')
    # energy-production [TWh]
    out_6436_1 = gross_production_TWh.rename(columns={'gross-production[TWh]': 'energy-production[TWh]'})
    # sector = electricity
    out_6436_1['sector'] = "electricity"

    # capacity and capacity-factor

    # capacity [GW]
    capacity_GW = export_variable(input_table=out_6733_1, selected_variable='capacity[GW]')

    # Electricity production Infrastructure

    # Compare needed capacities vs existing capacities => build new capacities if required
    # 
    # If needed capacities [TWh] > installed/existing-capacities [TWh] :
    # => Diff = needed capacities [TWh] - existing-capacities [TWh]
    # => capacities-installed (for year i => i + lifespan) = capacities-installed + diff
    # => capacities-cumulated (for year i => i + lifespan) = capacities-cumulated + diff
    # => new capacities (for year i) = diff

    # capacity [GW]
    capacity_GW = use_variable(input_table=capacity_GW, selected_variable='capacity[GW]')
    # HTS lifespan [years]
    lifespan_years = import_data(trigram='elc', variable_name='lifespan', variable_type='HTS')

    # Compute electricity infrastruture + compare needed capacities vs existing capacities => build new capacities if required
    # 
    # If needed capacities [TWh] > installed/existing-capacities [TWh]
    # => Diff = needed capacities [TWh] - existing-capacities [TWh]
    # => capacities-installed (for year i => i + lifespan) = capacities-installed + diff
    # => capacities-cumulated (for year i => i + lifespan) = capacities-cumulated + diff
    # => new capacities (for year i) = diff

    # lifespan [years]
    lifespan_years_2 = use_variable(input_table=lifespan_years, selected_variable='lifespan[years]')
    # Keep electricity-infrastructure
    lifespan_years_2 = lifespan_years_2.loc[lifespan_years_2['way-of-production'].isin(['electricity-infrastructure'])].copy()
    # HTS existing-capacities [GW]
    existing_capacities_GW = import_data(trigram='elc', variable_name='existing-capacities', variable_type='HTS')
    # Left Join existing-capacity and lifespan in one table  common_col = Country, Years, way-of-prod.
    out_6712_1 = joiner(df_left=lifespan_years, df_right=existing_capacities_GW, joiner='left', left_input=['Country', 'Years', 'way-of-production'], right_input=['Country', 'Years', 'way-of-production'])
    # Set to 0
    out_6712_1 = missing_value(df=out_6712_1, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by  country, years, way-of-prod
    capacity_GW_2 = group_by_dimensions(df=capacity_GW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')

    def helper_9560(input_table_1, input_table_2) -> pd.DataFrame:
        import numpy as np
        
        # Join on common columns
        common_col = []
        merge_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
                if col != "Years":
                    merge_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        
        # Years values
        years = output_table["Years"].unique()
        start_year = min(years)
        end_year = max(years)
        base_year = Globals.get().base_year
        
        ## Columns names
        needed_cap = 'capacity[GW]' # Needed capacities
        existing_cap = 'existing-capacities[GW]'
        lifespan = 'lifespan[years]'
        new_cap = 'new-capacities[GW]' # New required capacities
        cum_new_cap = 'cum-new-capacities[GW]'
        
        # If needed capacities is Nan => set 0
        mask = (output_table[needed_cap].isna())
        output_table.loc[mask,needed_cap] = 0
        
        # ---------------------------------------------------------------------
        # Interpolation between years (to avoid having 5 years steps)
        values_col = [needed_cap, existing_cap, lifespan] # Col on which interpolation should applied
        years_range = [i for i in range(start_year, end_year+1)] # All needed years
        for year in years_range:
            if year not in years: # Check if year already exists in input_table
                # Get list of years > year and < year
                greather_years = [i for i in years if i >= year] # in column years
                smaller_years = [i for i in years_range if i < year] # In year range
                # Get next years
                prev_year = min(smaller_years, key=lambda x:abs(x-year)) # previous year with value
                next_year = min(greather_years, key=lambda x:abs(x-year)) # next year with value
                # Get diff between next years and previous years
                col_to_sort = ["Country", "way-of-production", "Years"]
                df = output_table.sort_values(by=col_to_sort)
                for col in values_col:
                    df[col] = df[col] + (df[col].shift(-1) - df[col]) / (next_year - prev_year)
                    #df["diff_" + col] = df[col].shift(-1) - df[col]
                # Get value for this year and add new lines
                mask_prev_year = (df["Years"] == prev_year)
                new_year_table = df.loc[mask_prev_year, :]
                new_year_table["Years"] = year
                output_table = pd.concat([output_table, new_year_table], ignore_index=True)
        
        # ---------------------------------------------------------------------
        
        # Set default values for cumulated and new capacities ( = 0)
        output_table[cum_new_cap] = 0.0
        output_table[new_cap] = 0.0
        
        # If existing > needed => existing = needed => in previous version => new version = we keep it like this
        # in order to have the caapcities installed even if they are not used !
        #mask = (output_table[existing_cap] > output_table[needed_cap])
        #output_table.loc[mask, existing_cap] = output_table.loc[mask, needed_cap]
        
        ## Compute new and cumulated capacities => year by year for year > baseyear
        for year in range(int(base_year) + 1, int(end_year) + 1):
            # Check difference between needed and existing + new capacities
            output_table["diff"] = output_table[needed_cap] - (output_table[existing_cap] + output_table[cum_new_cap])
            # Get value for year + lifespan
            output_table["yr_lifespan"] = output_table["Years"] + output_table[lifespan]
            # If diff < 0 => set diff = 0
            mask = (output_table["diff"] < 0)
            output_table.loc[mask, "diff"] = 0
            # If differences > 0 => build new capacities
            mask = (output_table["Years"] == year)
            output_table.loc[mask, new_cap] = output_table.loc[mask, "diff"]
            # Columns to keep => copy a new table
            kept_cols = merge_col + ["diff", "yr_lifespan"]
            table_temp = output_table.loc[mask, kept_cols].copy()
            del output_table["diff"]
            del output_table["yr_lifespan"]
            # Join Left : output_table - table_temp => on merge_col
            output_table = output_table.merge(table_temp, on = merge_col, how = "left")
            # if Years >= year or < yr_lifespan => add diff to new-cum-sum
            mask = (output_table["Years"] >= year) & (output_table["Years"] < output_table["yr_lifespan"])
            output_table.loc[mask, cum_new_cap] = output_table.loc[mask, cum_new_cap] + output_table.loc[mask, "diff"]
        
        
        # Keep only required Years
        dim_cols = [c for c in output_table.select_dtypes(exclude=[float])]
        ## For existing capacity : Keep value for years (not need to do anything)
        kept_cols = dim_cols.copy()
        kept_cols.append(existing_cap)
        kept_cols.append(cum_new_cap)
        df_year = input_table_1["Years"].drop_duplicates().reset_index(drop=True).to_frame()
        df_existing = output_table.merge(df_year, on=["Years"], how="inner")
        df_existing = df_existing[kept_cols]
        # existing_cap
        ## For new and cum-new => Sum values between 2 required years
        kept_cols = dim_cols.copy()
        kept_cols.append(new_cap)
        df_year["Year_tmp"] = df_year["Years"] # Duplicates values to apply computation on it
        df_new = output_table.merge(df_year, on=["Years"], how="left")
        df_new = df_new.sort_values(by=["Years"])
        df_new = df_new.bfill()
        del df_new["Years"]
        df_new.rename(columns={"Year_tmp":"Years"}, inplace=True)
        df_new["Years"] = df_new["Years"].astype(int)
        df_new = df_new.groupby(dim_cols).sum().reset_index()
        df_new = df_new[kept_cols]
        ## Join both outputs
        output_table = df_new.merge(df_existing, on=dim_cols, how="inner")
        
        ## Recompute cum-new-capacities > should be equal to needed - existing => No need ; already working like this !
        #output_table[cum_new_cap] = output_table[needed_cap] - output_table[existing_cap]
        
        ## Force 0 if negative value + round to 5 decimals
        #for col in [existing_cap, needed_cap, new_cap, cum_new_cap]:
        #    output_table[col][output_table[col] < 0] = 0
        return output_table
    # Compute new and cumulated capacities  If existing capacities > required capacities then leave it like this We want to know how much capacities are installed even if there are not used !
    out_9560_1 = helper_9560(input_table_1=capacity_GW_2, input_table_2=out_6712_1)

    # New and total capacities
    # for power plants
    # [GW]
    # (detailled for all years)

    # new-capacities [GW]
    new_capacities_GW = export_variable(input_table=out_9560_1, selected_variable='new-capacities[GW]')

    # Costs CAPEX / OPEX

    # new-capacities [GW]
    new_capacities_GW = use_variable(input_table=new_capacities_GW, selected_variable='new-capacities[GW]')
    # OTS/FTS wacc [%] from TEC
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = elc
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['elc'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Unit conversion GW to kW
    new_capacities_kW = new_capacities_GW.drop(columns='new-capacities[GW]').assign(**{'new-capacities[kW]': new_capacities_GW['new-capacities[GW]'] * 1000000.0})
    # Group by  country, years, way-of-prod
    new_capacities_kW = group_by_dimensions(df=new_capacities_kW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # existing-capacities [GW]
    existing_capacities_GW = use_variable(input_table=out_9560_1, selected_variable='existing-capacities[GW]')
    # existing-capacities [GW]
    existing_capacities_GW_2 = export_variable(input_table=existing_capacities_GW, selected_variable='existing-capacities[GW]')

    # Existing capacities

    # existing-capacities [GW]
    existing_capacities_GW_2 = use_variable(input_table=existing_capacities_GW_2, selected_variable='existing-capacities[GW]')
    # cum-new-capacities [GW]
    cum_new_capacities_GW = use_variable(input_table=out_9560_1, selected_variable='cum-new-capacities[GW]')

    # Determine ETS / non-ETS status of total capacity

    # total-capacities[GW] = new-cum-capacities[GW] + existing-capacities[GW]
    total_capacities_GW = mcd(input_table_1=existing_capacities_GW, input_table_2=cum_new_capacities_GW, operation_selection='x + y', output_name='total-capacities[GW]')
    # RCP power-ets-share [%] from TEC
    power_ets_share_percent = import_data(trigram='tec', variable_name='power-ets-share', variable_type='RCP')
    # Convert Unit % to - (* 0.01)
    power_ets_share_ = power_ets_share_percent.drop(columns='power-ets-share[%]').assign(**{'power-ets-share[-]': power_ets_share_percent['power-ets-share[%]'] * 0.01})
    # total-capacities[GW] (replace) = total-capacities[GW] * power-ets-share-]  LEFT Join If missing set to 1
    total_capacities_GW = mcd(input_table_1=total_capacities_GW, input_table_2=power_ets_share_, operation_selection='x * y', output_name='total-capacities[GW]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9209(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)
    out_9209_1 = helper_9209(input_table=total_capacities_GW)
    # total-capacities [GW]
    total_capacities_GW = export_variable(input_table=out_9209_1, selected_variable='total-capacities[GW]')
    # total-capacities [GW]
    total_capacities_GW = use_variable(input_table=total_capacities_GW, selected_variable='total-capacities[GW]')
    # Unit conversion GW to kW
    total_capacities_kW = total_capacities_GW.drop(columns='total-capacities[GW]').assign(**{'total-capacities[kW]': total_capacities_GW['total-capacities[GW]'] * 1000000.0})
    # Group by  country, years, way-of-prod
    total_capacities_kW = group_by_dimensions(df=total_capacities_kW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # cum-new-capacities [GW]
    cum_new_capacities_GW = export_variable(input_table=cum_new_capacities_GW, selected_variable='cum-new-capacities[GW]')

    # New Capacities

    # new-cum-capacities [GW]
    cum_new_capacities_GW = use_variable(input_table=cum_new_capacities_GW, selected_variable='cum-new-capacities[GW]')
    # capacity (needed) [GW]
    capacity_GW = use_variable(input_table=capacity_GW, selected_variable='capacity[GW]')
    # Group by  country, years
    capacity_GW = group_by_dimensions(df=capacity_GW, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # capacity-factor [%]
    capacity_factor_percent = export_variable(input_table=out_6733_1, selected_variable='capacity-factor[%]')

    # Capacity-factor

    # capacity-factor [%]
    capacity_factor_percent = use_variable(input_table=capacity_factor_percent, selected_variable='capacity-factor[%]')
    # energy-to-be-produced [TWh]
    energy_to_be_produced_TWh = use_variable(input_table=energy_to_be_produced_TWh, selected_variable='energy-to-be-produced[TWh]')
    # remaining-demand[TWh] = energy-to-be-produced[TWh] - net-energy-production[TWh]
    remaining_demand_TWh = mcd(input_table_1=energy_to_be_produced_TWh, input_table_2=net_energy_production_TWh_2, operation_selection='x - y', output_name='remaining-demand[TWh]')
    # remaining-demand [TWh]
    remaining_demand_TWh = use_variable(input_table=remaining_demand_TWh, selected_variable='remaining-demand[TWh]')
    # energy-imported[TWh] (replace) = energy-imported[TWh] + remaining-demand[TWh]
    energy_imported_TWh = mcd(input_table_1=energy_imported_TWh_2, input_table_2=remaining_demand_TWh, operation_selection='x + y', output_name='energy-imported[TWh]')

    # Import / Export
    # [TWh]

    # energy-imported[TWh]
    energy_imported_TWh = export_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')

    # Energy - imported

    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # sector = electricity
    energy_imported_TWh_2 = energy_imported_TWh.assign(**{'sector': "electricity"})

    # Transmission integration
    # 
    # Sum net electricity production (after self consumption) and import (export is set to 0) : make difference between intermittent type and non intermittent type
    # => Compute intermitency-share = production via intermittent type / production via non intermittent type
    # => Compute transmission capacity = 
    # 	degree_integration * (
    # 	+263.69 * intermitency-share ^2
    # 	-13.252 * intermitency-share
    # 	+4.3033
    # 	)
    # where degree_integration = 0.5 (fix value here) => could be change in the future ?
    # 
    # => Correction of transmission capacity :  if intermittency share is low (<15%), we won’t need new lines (capacity = set to 8 GW)
    # => Compute new need : if the need for transmission capacity reduces, we need to keep the current infrastructure (if capacity decrease, set value from previous year)  - negative set to zero

    # energy-imported [TWh]
    energy_imported_TWh = use_variable(input_table=energy_imported_TWh, selected_variable='energy-imported[TWh]')
    # If < 0 set to 0
    energy_imported_TWh_3 = energy_imported_TWh.copy()
    mask = energy_imported_TWh_3['energy-imported[TWh]'] < 0
    energy_imported_TWh_3.loc[mask, 'energy-imported[TWh]'] =  0
    energy_imported_TWh_3.loc[~mask, 'energy-imported[TWh]'] =  energy_imported_TWh_3.loc[~mask, 'energy-imported[TWh]']
    # energy-imported to net-energy-production
    out_6300_1 = energy_imported_TWh_3.rename(columns={'energy-imported[TWh]': 'net-energy-production[TWh]'})
    # way-of-production = elec-import
    out_6300_1['way-of-production'] = "elec-import"
    # Node 5876
    out_6301_1 = pd.concat([out_6300_1, net_energy_production_TWh.set_index(net_energy_production_TWh.index.astype(str) + '_dup')])
    # Unit conversion TWh to GW x 1000 / (365 x 24)
    energy_imported_GW = energy_imported_TWh.drop(columns='energy-imported[TWh]').assign(**{'energy-imported[GW]': energy_imported_TWh['energy-imported[TWh]'] * 0.114077116130504})
    # Group by  country, years
    energy_imported_GW = group_by_dimensions(df=energy_imported_GW, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # import = abs(import) if export, does not mean we require less  capacity
    energy_imported_GW = math_formula(df=energy_imported_GW, convert_to_int=False, replaced_column='energy-imported[GW]', splitted='abs($energy-imported[GW]$)')
    # elec-infrastructure-capacity[GW] = capacity[GW] + energy-imported[GW]
    elec_infrastructure_capacity_GW = mcd(input_table_1=capacity_GW, input_table_2=energy_imported_GW, operation_selection='x + y', output_name='elec-infrastructure-capacity[GW]')

    def helper_6272(input_table_1, input_table_2) -> pd.DataFrame:
        # Join on common columns
        common_col = []
        for col in input_table_1.head():
            if col in input_table_2.head():
                common_col.append(col)
        
        output_table = input_table_1.merge(input_table_2, how='inner', on=common_col)
        
        ## Columns names
        needed_cap = 'elec-infrastructure-capacity[GW]'
        lifespan = 'lifespan[years]'
        cum_cap = 'cum-infra-capacities[GW]'
        new_cap = 'new-infra-capacities[GW]'
        
        # If needed capacities is Nan => set 0
        mask = (output_table[needed_cap].isna())
        output_table.loc[mask,needed_cap] = 0
        
        # Set default values for cumulated and new capacities ( = 0)
        output_table[cum_cap] = 0
        output_table[new_cap] = 0
        
        # Get possible values
        def get_values(column_name):
            list = []
            for i in output_table[column_name]:
                if i not in list:
                    list.append(i)
            return list
        
        country_list = get_values('Country')
        year_list = get_values('Years')
        year_list.sort()
        
        ## Compute new and cumulated capacities => year by year
        for country in country_list:
            mask_in = (output_table['Country'] == country)
            for year in year_list:
                # Set mask
                mask_year = mask_in & (output_table['Years'] == year)
                lifespan_val = output_table.loc[mask_year, lifespan].values[0]
                mask_lifespan = mask_in & (output_table['Years'] > year) & (output_table['Years'] <= year + lifespan_val)
                ## DIFF : needed - existing => if < 0 => new capacities required
                # Recompute diff
                output_table["diff"] = output_table[cum_cap] - output_table[needed_cap]
                diff = output_table.loc[mask_year, "diff"].values[0]
                if diff < 0:
                    ## NEW CAPACITIES = diff
                    output_table.loc[mask_year, new_cap] = abs(diff)
                    ## CUM CAPACITIES = cum_cap + diff
                    output_table.loc[mask_lifespan, cum_cap] = output_table.loc[mask_lifespan, cum_cap] + abs(diff)
        return output_table
    # Compute new and cumulated capacities
    out_6272_1 = helper_6272(input_table_1=elec_infrastructure_capacity_GW, input_table_2=lifespan_years_2)

    # New and total capacities
    # for elec infrastructure
    # [GW]

    # elec-infrastructure-capacity[GW] correspond to total-capacities
    elec_infrastructure_capacity_GW = export_variable(input_table=out_6272_1, selected_variable='elec-infrastructure-capacity[GW]')
    # elec-infrastructure-capacity [GW]
    elec_infrastructure_capacity_GW = use_variable(input_table=elec_infrastructure_capacity_GW, selected_variable='elec-infrastructure-capacity[GW]')
    # RCP costs-for-energy-by-way-of-prod [MEUR/kW] from TEC
    costs_for_energy_by_way_of_prod_MEUR_per_kW = import_data(trigram='tec', variable_name='costs-for-energy-by-way-of-prod', variable_type='RCP')
    # RCP price-indices [-] from TEC
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')
    # Compute capex for heat energy-production[kW]
    out_9530_1 = compute_costs(df_activity=new_capacities_kW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='new-capacities[kW]')
    # Compute opex for total-capacities [kW]
    out_9527_1 = compute_costs(df_activity=total_capacities_kW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='total-capacities[kW]', cost_type='OPEX')
    # Group by  country, years, way-of-prod
    elec_infrastructure_capacity_GW = group_by_dimensions(df=elec_infrastructure_capacity_GW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Compute opex for total-capacities [kW]
    out_9538_1 = compute_costs(df_activity=elec_infrastructure_capacity_GW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='elec-infrastructure-capacity[GW]', cost_type='OPEX')
    # new-infra-capacities [GW]
    new_infra_capacities_GW = export_variable(input_table=out_6272_1, selected_variable='new-infra-capacities[GW]')
    # new-infra-capacities [GW]
    new_infra_capacities_GW = use_variable(input_table=new_infra_capacities_GW, selected_variable='new-infra-capacities[GW]')
    # Group by  country, years, way-of-prod
    new_infra_capacities_GW = group_by_dimensions(df=new_infra_capacities_GW, groupby_dimensions=['Country', 'Years', 'way-of-production'], aggregation_method='Sum')
    # Compute capex for heat energy-production[kW]
    out_9537_1 = compute_costs(df_activity=new_infra_capacities_GW, df_unit_costs=costs_for_energy_by_way_of_prod_MEUR_per_kW, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='new-infra-capacities[GW]')
    # opex_backup[MEUR/GW] = 9
    opex_backup_MEUR_per_GW = 9.0
    # capex_backup[MEUR/GW] = 350
    capex_backup_MEUR_per_GW = 350.0
    # loadfactor-backup = 0.05
    loadfactor_backup = 0.05
    # transmission_cost[MEUR/GWkm] = 1.05
    transmission_cost_MEUR_per_GWkm = 1.05
    # degree_integration = 0.5
    degre_integration = 0.5

    # Hard code here = should be in google sheet

    # Intermittent categories
    out_6290_1 = pd.DataFrame(columns=['intermittent-category', 'way-of-production'], data=[['non-intermittent', 'elec-plant-with-solid-bio-waste'], ['non-intermittent', 'RES-geothermal'], ['non-intermittent', 'RES-hydroelectric'], ['intermittent', 'RES-marine'], ['intermittent', 'RES-solar-csp'], ['intermittent', 'RES-solar-pv'], ['intermittent', 'RES-wind-offshore'], ['intermittent', 'RES-wind-onshore'], ['non-intermittent', 'elec-plant-with-nuclear'], ['non-intermittent', 'elec-plant-with-solid-coal'], ['non-intermittent', 'elec-plant-with-gas'], ['non-intermittent', 'elec-plant-with-liquid'], ['non-intermittent', 'elec-import']])
    # LEFTJOIN on way-of-production Add intermittent category
    out_6293_1 = joiner(df_left=out_6301_1, df_right=out_6290_1, joiner='left', left_input=['way-of-production'], right_input=['way-of-production'])
    # Group by  country, years, intermitent-category
    out_6293_1_2 = group_by_dimensions(df=out_6293_1, groupby_dimensions=['Country', 'Years', 'intermittent-category'], aggregation_method='Sum')
    # Keep only intermittent
    out_6293_1_2 = out_6293_1_2.loc[out_6293_1_2['intermittent-category'].isin(['intermittent'])].copy()
    # net-energy-production to intermittent-net-production
    out_6297_1 = out_6293_1_2.rename(columns={'net-energy-production[TWh]': 'intermittent-net-production[TWh]'})
    # Group by  country, years
    out_6293_1 = group_by_dimensions(df=out_6293_1, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # intermitency-share[%] = intermittent-net-production[TWh] / net-energy-production[TWh]
    intermitency_share_percent = mcd(input_table_1=out_6293_1, input_table_2=out_6297_1, operation_selection='y / x', output_name='intermitency-share[%]')

    # Backup to cover intermittency production
    # 
    # Compute backup capacity =
    # 	if total electrcity production * (0.2812 * intermitency-share[%] - 0.0228) < 0 => 0,
    # 	else => total electrcity production * (0.2812 * intermitency-share[%] - 0.0228
    # 
    # => Compute new need : if the need for transmission capacity reduces, we need to keep the current infrastructure (if capacity decrease, set value from previous year)  - negative set to zero
    # => Gas consumption (for backup) = backup-capacity * loadfactor_backupplant[%]
    # where loadfactor_backupplant[%] = 0.05

    # intermitency-share [%]
    intermitency_share_percent_2 = use_variable(input_table=intermitency_share_percent, selected_variable='intermitency-share[%]')
    # Compute transmission-capacity [GW]
    transmission_capacity_GW = math_formula(df=intermitency_share_percent, convert_to_int=False, replaced_column='transmission-capacity[GW]', splitted=['', 'NEW_COLUMN', '=', '', '@degre_integration', '', '*(+263.69*', 'intermitency-share[%]', '*', 'intermitency-share[%]', '-13.252*', 'intermitency-share[%]', '+4.3033)'], degre_integration=degre_integration)
    # Correction : if the intermittency share  is low we won’t need  new lines IS[x] < 15%  then   = 8 GW 
    mask = transmission_capacity_GW['intermitency-share[%]']<0.15
    transmission_capacity_GW.loc[mask, 'transmission-capacity[GW]'] = 8
    transmission_capacity_GW.loc[~mask, 'transmission-capacity[GW]'] = transmission_capacity_GW.loc[~mask, 'transmission-capacity[GW]']
    # transmission-capacity [GW]
    transmission_capacity_GW = use_variable(input_table=transmission_capacity_GW, selected_variable='transmission-capacity[GW]')
    # Lag  transmission-capacity[GW]
    out_7557_1, out_7557_2 = lag_variable(df=transmission_capacity_GW, in_var='transmission-capacity[GW]')
    # transmission-capacity -lagged [GW]
    transmission_capacity_lagged_GW = use_variable(input_table=out_7557_1, selected_variable='transmission-capacity_lagged[GW]')
    # transmission-growth[GW] = transmission-capacity[GW] - transmission-capacity-lagged[GW]
    transmission_growth_GW = mcd(input_table_1=transmission_capacity_GW, input_table_2=transmission_capacity_lagged_GW, operation_selection='x - y', output_name='transmission-growth[GW]')
    # Timestep
    Timestep = use_variable(input_table=out_7557_2, selected_variable='Timestep')
    # annual-transmission-growth[GW] = transmission-growth[GW] / timestep
    annual_transmission_growth_GW = mcd(input_table_1=transmission_growth_GW, input_table_2=Timestep, operation_selection='x / y', output_name='annual-transmission-growth[GW]')
    # Set to 0
    annual_transmission_growth_GW = missing_value(df=annual_transmission_growth_GW, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # If annual-transmission-growth < 0 set to 0
    mask = annual_transmission_growth_GW['annual-transmission-growth[GW]'] < 0
    annual_transmission_growth_GW.loc[mask, 'annual-transmission-growth[GW]'] =  0
    annual_transmission_growth_GW.loc[~mask, 'annual-transmission-growth[GW]'] =  annual_transmission_growth_GW.loc[~mask, 'annual-transmission-growth[GW]']
    # annual-transmission-growth [GW]
    annual_transmission_growth_GW = use_variable(input_table=annual_transmission_growth_GW, selected_variable='annual-transmission-growth[GW]')
    # Unit conversion GW to GW*km
    annual_transmission_growth_GW_km = annual_transmission_growth_GW.drop(columns='annual-transmission-growth[GW]').assign(**{'annual-transmission-growth[GW.km]': annual_transmission_growth_GW['annual-transmission-growth[GW]'] * 243.0})
    # CAPEX for new transmission capacity
    capex_MEUR = math_formula(df=annual_transmission_growth_GW_km, convert_to_int=False, replaced_column='capex[MEUR]', splitted=['', 'NEW_COLUMN', '=', 'annual-transmission-growth[GW.km]', '*', '', '@transmission_cost_MEUR_per_GWkm', '', ''], transmission_cost_MEUR_per_GWkm=transmission_cost_MEUR_per_GWkm)
    # Assumption lifetime = 25 years
    out_9567_1 = spread_capital(output_table=capex_MEUR, df_wacc=wacc_percent)
    # capex [MEUR]
    capex_MEUR = export_variable(input_table=out_9567_1, selected_variable='capex[MEUR]')
    # way-of-production = transmission-integration
    capex_MEUR['way-of-production'] = "transmission-integration"
    # Keep Years >= baseyear
    capex_MEUR, _ = filter_dimension(df=capex_MEUR, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # transmission-capacity [GW]
    transmission_capacity_GW = use_variable(input_table=transmission_capacity_GW, selected_variable='transmission-capacity[GW]')
    # Unit conversion GW to GW*km
    transmission_capacity_GW_km = transmission_capacity_GW.drop(columns='transmission-capacity[GW]').assign(**{'transmission-capacity[GW.km]': transmission_capacity_GW['transmission-capacity[GW]'] * 243.0})
    # OPEX for transmission capacity
    opex_MEUR = math_formula(df=transmission_capacity_GW_km, convert_to_int=False, replaced_column='opex[MEUR]', splitted=['', 'NEW_COLUMN', '=', 'transmission-capacity[GW.km]', '*0.01*', '', '@transmission_cost_MEUR_per_GWkm', '', ''], transmission_cost_MEUR_per_GWkm=transmission_cost_MEUR_per_GWkm)
    # opex [MEUR]
    opex_MEUR = export_variable(input_table=opex_MEUR, selected_variable='opex[MEUR]')
    # way-of-production = transmission-integration
    opex_MEUR['way-of-production'] = "transmission-integration"
    # Keep Years >= baseyear
    opex_MEUR, _ = filter_dimension(df=opex_MEUR, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # net-energy-production [TWh]
    net_energy_production_TWh = use_variable(input_table=out_6293_1, selected_variable='net-energy-production[TWh]')
    # Join on common cols
    out_6320_1 = joiner(df_left=intermitency_share_percent_2, df_right=net_energy_production_TWh, joiner='inner', left_input=['Years', 'Country'], right_input=['Years', 'Country'])
    backup_capacity_GW = out_6320_1.copy()
    mask = backup_capacity_GW['net-energy-production[TWh]'] * (0.2812 * backup_capacity_GW['intermitency-share[%]'] - 0.0228) < 0
    backup_capacity_GW.loc[mask, 'backup-capacity[GW]'] = 0
    backup_capacity_GW.loc[~mask, 'backup-capacity[GW]'] = backup_capacity_GW.loc[~mask, 'net-energy-production[TWh]'] * (0.2812 * backup_capacity_GW.loc[~mask, 'intermitency-share[%]'] - 0.0228)
    # backup-capacity [GW]
    backup_capacity_GW_2 = use_variable(input_table=backup_capacity_GW, selected_variable='backup-capacity[GW]')

    # Capacity-factor

    # capacity-factor [%]
    backup_capacity_GW = use_variable(input_table=backup_capacity_GW_2, selected_variable='backup-capacity[GW]')
    # Unit conversion GW to GWh/yr 24*365.25
    backup_capacity_GWh = backup_capacity_GW_2.drop(columns='backup-capacity[GW]').assign(**{'backup-capacity[GWh]': backup_capacity_GW_2['backup-capacity[GW]'] * 8766.0})
    # gas-consumption-for-backup [GWh]
    gas_consumption_for_backup_GWh = math_formula(df=backup_capacity_GWh, convert_to_int=False, replaced_column='gas-consumption-for-backup[GWh]', splitted=['', 'NEW_COLUMN', '=', 'backup-capacity[GWh]', '*', '', '@loadfactor_backup', '', ''], loadfactor_backup=loadfactor_backup)
    # gas-consumption-for-backup [GWh]
    gas_consumption_for_backup_GWh = use_variable(input_table=gas_consumption_for_backup_GWh, selected_variable='gas-consumption-for-backup[GWh]')
    # OTS/FTS primary-energy-costs [EUR / MWh] from TEC
    primary_energy_costs_EUR__per__MWh = import_data(trigram='tec', variable_name='primary-energy-costs')
    # Keep energy-carrier = gaseous-ff-natural
    primary_energy_costs_EUR__per__MWh = primary_energy_costs_EUR__per__MWh.loc[primary_energy_costs_EUR__per__MWh['energy-carrier'].isin(['gaseous-ff-natural'])].copy()
    # Unit conversion GWh to MWh
    gas_consumption_for_backup_MWh = gas_consumption_for_backup_GWh.drop(columns='gas-consumption-for-backup[GWh]').assign(**{'gas-consumption-for-backup[MWh]': gas_consumption_for_backup_GWh['gas-consumption-for-backup[GWh]'] * 1000.0})
    # OPEX VARIABLE for backup-capacity = gas-consumption[MWh] * energy-cost[EUR / MWh] for gas
    opex_EUR = mcd(input_table_1=gas_consumption_for_backup_MWh, input_table_2=primary_energy_costs_EUR__per__MWh, operation_selection='x * y', output_name='opex[EUR]')
    # Unit conversion EUR to MEUR
    opex_MEUR_2 = opex_EUR.drop(columns='opex[EUR]').assign(**{'opex[MEUR]': opex_EUR['opex[EUR]'] * 1e-06})
    # way-of-production = variable-backup-capacity
    opex_MEUR_2['way-of-production'] = "variable-backup-capacity"
    # Keep Years >= baseyear
    opex_MEUR_2, _ = filter_dimension(df=opex_MEUR_2, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # Lag  backup-capacity[kW]
    out_7559_1, out_7559_2 = lag_variable(df=backup_capacity_GW_2, in_var='backup-capacity[GW]')
    # backup-capacity -lagged [GW]
    backup_capacity_lagged_GW = use_variable(input_table=out_7559_1, selected_variable='backup-capacity_lagged[GW]')
    # backup-growth[GW] = backup-capacity[GW] - backup-capacity-lagged[GW]
    backup_growth_GW = mcd(input_table_1=backup_capacity_GW_2, input_table_2=backup_capacity_lagged_GW, operation_selection='x - y', output_name='backup-growth[GW]')
    # Timestep
    Timestep = use_variable(input_table=out_7559_2, selected_variable='Timestep')
    # annual-backup-growth[GW] = backup-growth[GW] / timestep
    annual_backup_growth_GW = mcd(input_table_1=backup_growth_GW, input_table_2=Timestep, operation_selection='x / y', output_name='annual-backup-growth[GW]')
    # Set to 0
    annual_backup_growth_GW = missing_value(df=annual_backup_growth_GW, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # If annual-backup-growth < 0 set to 0
    mask = annual_backup_growth_GW['annual-backup-growth[GW]'] < 0
    annual_backup_growth_GW.loc[mask, 'annual-backup-growth[GW]'] =  0
    annual_backup_growth_GW.loc[~mask, 'annual-backup-growth[GW]'] =  annual_backup_growth_GW.loc[~mask, 'annual-backup-growth[GW]']
    # annual-backup-growth[GW]
    annual_backup_growth_GW = use_variable(input_table=annual_backup_growth_GW, selected_variable='annual-backup-growth[GW]')
    # CAPEX for new backup-capacity
    capex_MEUR_2 = math_formula(df=annual_backup_growth_GW, convert_to_int=False, replaced_column='capex[MEUR]', splitted=['', 'NEW_COLUMN', '=', 'annual-backup-growth[GW]', '*', '', '@capex_backup_MEUR_per_GW', '', ''], capex_backup_MEUR_per_GW=capex_backup_MEUR_per_GW)
    # Assumption lifetime = 25 years
    out_9568_1 = spread_capital(output_table=capex_MEUR_2, df_wacc=wacc_percent)
    # capex [MEUR]
    capex_MEUR_2 = export_variable(input_table=out_9568_1, selected_variable='capex[MEUR]')
    # way-of-production = backup-capacity
    capex_MEUR_2['way-of-production'] = "backup-capacity"
    # Keep Years >= baseyear
    capex_MEUR_2, _ = filter_dimension(df=capex_MEUR_2, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # CAPEX
    capex_MEUR = pd.concat([capex_MEUR, capex_MEUR_2.set_index(capex_MEUR_2.index.astype(str) + '_dup')])
    # CAPEX
    out_9548_1 = pd.concat([out_9537_1, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])
    # CAPEX
    out_1 = pd.concat([out_9530_1, out_9548_1.set_index(out_9548_1.index.astype(str) + '_dup')])
    # RCP energy-production-cost-user [-]
    energy_production_cost_user_ = import_data(trigram='elc', variable_name='energy-production-cost-user', variable_type='RCP')
    # capex[MEUR] = capex[MEUR] * energy-production-cost-user[-]
    capex_MEUR = mcd(input_table_1=out_1, input_table_2=energy_production_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by  Country, Years, way-of-prooduction (sum)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'way-of-production', 'cost-user'], aggregation_method='Sum')
    # OPEX FIXED for backup-capacity
    opex_MEUR_3 = math_formula(df=backup_capacity_GW, convert_to_int=False, replaced_column='opex[MEUR]', splitted=['', 'NEW_COLUMN', '=', 'backup-capacity[GW]', '*', '', '@opex_backup_MEUR_per_GW', '', ''], opex_backup_MEUR_per_GW=opex_backup_MEUR_per_GW)
    # opex [MEUR]
    opex_MEUR_3 = export_variable(input_table=opex_MEUR_3, selected_variable='opex[MEUR]')
    # way-of-production = fixed-backup-capacity
    opex_MEUR_3['way-of-production'] = "fixed-backup-capacity"
    # Keep Years >= baseyear
    opex_MEUR_3, _ = filter_dimension(df=opex_MEUR_3, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # OPEX
    opex_MEUR_2 = pd.concat([opex_MEUR_2, opex_MEUR_3.set_index(opex_MEUR_3.index.astype(str) + '_dup')])
    # OPEX
    opex_MEUR = pd.concat([opex_MEUR, opex_MEUR_2.set_index(opex_MEUR_2.index.astype(str) + '_dup')])
    # OPEX
    out_9551_1 = pd.concat([out_9538_1, opex_MEUR.set_index(opex_MEUR.index.astype(str) + '_dup')])
    # OPEX
    out_1 = pd.concat([out_9527_1, out_9551_1.set_index(out_9551_1.index.astype(str) + '_dup')])
    # opex[MEUR] = opex[MEUR] * energy-production-cost-user[-]
    opex_MEUR = mcd(input_table_1=energy_production_cost_user_, input_table_2=out_1, operation_selection='x * y', output_name='opex[MEUR]')
    # Group by  Country, Years, way-of-prooduction (sum)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'cost-user', 'Years', 'way-of-production'], aggregation_method='Sum')
    MEUR = pd.concat([capex_MEUR, opex_MEUR.set_index(opex_MEUR.index.astype(str) + '_dup')])

    # Capex / Opex

    # Keep capex / opex + Country, Years
    MEUR = column_filter(df=MEUR, pattern='^.*$')
    # sector = electricity
    MEUR['sector'] = "electricity"

    # Quick Fix : Avoid jump
    # QUICK AND REALLY DIRTY FIX FOR EMISSION CALIBRATION TO GET TO A CALIBRATION FACTOR OF 1 AND AVOIDING JUMP

    # Only  belgium and its regions (not Belgium nor other countries) Others = set to 1 Only CO2 ?!
    out_1547_1 = pd.DataFrame(columns=['calibration_fix[%]', 'Country', 'Years', 'gaes'], data=[[1.0, 'belgium', 2000, 'CO2'], [1.0, 'belgium', 2001, 'CO2'], [1.0, 'belgium', 2002, 'CO2'], [1.0, 'belgium', 2003, 'CO2'], [1.0, 'belgium', 2004, 'CO2'], [1.0, 'belgium', 2005, 'CO2'], [1.0, 'belgium', 2006, 'CO2'], [1.0, 'belgium', 2007, 'CO2'], [1.0, 'belgium', 2008, 'CO2'], [1.0, 'belgium', 2009, 'CO2'], [1.0, 'belgium', 2010, 'CO2'], [1.0, 'belgium', 2011, 'CO2'], [1.0, 'belgium', 2012, 'CO2'], [1.0, 'belgium', 2013, 'CO2'], [1.0, 'belgium', 2014, 'CO2'], [1.0, 'belgium', 2015, 'CO2'], [1.0, 'belgium', 2016, 'CO2'], [1.0, 'belgium', 2017, 'CO2'], [1.0, 'belgium', 2018, 'CO2'], [1.0, 'belgium', 2019, 'CO2'], [0.88, 'belgium', 2020, 'CO2'], [0.88, 'belgium', 2021, 'CO2'], [0.88, 'belgium', 2022, 'CO2'], [0.88, 'belgium', 2023, 'CO2'], [0.88, 'belgium', 2024, 'CO2'], [0.705, 'belgium', 2025, 'CO2'], [0.705, 'belgium', 2026, 'CO2'], [0.705, 'belgium', 2027, 'CO2'], [0.705, 'belgium', 2028, 'CO2'], [0.705, 'belgium', 2029, 'CO2'], [0.588, 'belgium', 2030, 'CO2'], [0.588, 'belgium', 2031, 'CO2'], [0.588, 'belgium', 2032, 'CO2'], [0.588, 'belgium', 2033, 'CO2'], [0.588, 'belgium', 2034, 'CO2'], [0.588, 'belgium', 2035, 'CO2'], [0.588, 'belgium', 2036, 'CO2'], [0.588, 'belgium', 2037, 'CO2'], [0.588, 'belgium', 2038, 'CO2'], [0.588, 'belgium', 2039, 'CO2'], [0.588, 'belgium', 2040, 'CO2'], [0.588, 'belgium', 2041, 'CO2'], [0.588, 'belgium', 2042, 'CO2'], [0.588, 'belgium', 2043, 'CO2'], [0.588, 'belgium', 2044, 'CO2'], [0.588, 'belgium', 2045, 'CO2'], [0.588, 'belgium', 2046, 'CO2'], [0.588, 'belgium', 2047, 'CO2'], [0.588, 'belgium', 2048, 'CO2'], [0.588, 'belgium', 2049, 'CO2'], [0.588, 'belgium', 2050, 'CO2'], [1.0, 'brussels', 2000, 'CO2'], [1.0, 'brussels', 2001, 'CO2'], [1.0, 'brussels', 2002, 'CO2'], [1.0, 'brussels', 2003, 'CO2'], [1.0, 'brussels', 2004, 'CO2'], [1.0, 'brussels', 2005, 'CO2'], [1.0, 'brussels', 2006, 'CO2'], [1.0, 'brussels', 2007, 'CO2'], [1.0, 'brussels', 2008, 'CO2'], [1.0, 'brussels', 2009, 'CO2'], [1.0, 'brussels', 2010, 'CO2'], [1.0, 'brussels', 2011, 'CO2'], [1.0, 'brussels', 2012, 'CO2'], [1.0, 'brussels', 2013, 'CO2'], [1.0, 'brussels', 2014, 'CO2'], [1.0, 'brussels', 2015, 'CO2'], [1.0, 'brussels', 2016, 'CO2'], [1.0, 'brussels', 2017, 'CO2'], [1.0, 'brussels', 2018, 'CO2'], [1.0, 'brussels', 2019, 'CO2'], [0.88, 'brussels', 2020, 'CO2'], [0.705, 'brussels', 2025, 'CO2'], [0.588, 'brussels', 2030, 'CO2'], [0.588, 'brussels', 2035, 'CO2'], [0.588, 'brussels', 2040, 'CO2'], [0.588, 'brussels', 2045, 'CO2'], [0.588, 'brussels', 2050, 'CO2'], [1.0, 'flanders', 2000, 'CO2'], [1.0, 'flanders', 2001, 'CO2'], [1.0, 'flanders', 2002, 'CO2'], [1.0, 'flanders', 2003, 'CO2'], [1.0, 'flanders', 2004, 'CO2'], [1.0, 'flanders', 2005, 'CO2'], [1.0, 'flanders', 2006, 'CO2'], [1.0, 'flanders', 2007, 'CO2'], [1.0, 'flanders', 2008, 'CO2'], [1.0, 'flanders', 2009, 'CO2'], [1.0, 'flanders', 2010, 'CO2'], [1.0, 'flanders', 2011, 'CO2'], [1.0, 'flanders', 2012, 'CO2'], [1.0, 'flanders', 2013, 'CO2'], [1.0, 'flanders', 2014, 'CO2'], [1.0, 'flanders', 2015, 'CO2'], [1.0, 'flanders', 2016, 'CO2'], [1.0, 'flanders', 2017, 'CO2'], [1.0, 'flanders', 2018, 'CO2'], [1.0, 'flanders', 2019, 'CO2'], [0.88, 'flanders', 2020, 'CO2'], [0.705, 'flanders', 2025, 'CO2'], [0.588, 'flanders', 2030, 'CO2'], [0.588, 'flanders', 2035, 'CO2'], [0.588, 'flanders', 2040, 'CO2'], [0.588, 'flanders', 2045, 'CO2'], [0.588, 'flanders', 2050, 'CO2'], [1.0, 'wallonia', 2000, 'CO2'], [1.0, 'wallonia', 2001, 'CO2'], [1.0, 'wallonia', 2002, 'CO2'], [1.0, 'wallonia', 2003, 'CO2'], [1.0, 'wallonia', 2004, 'CO2'], [1.0, 'wallonia', 2005, 'CO2'], [1.0, 'wallonia', 2006, 'CO2'], [1.0, 'wallonia', 2007, 'CO2'], [1.0, 'wallonia', 2008, 'CO2'], [1.0, 'wallonia', 2009, 'CO2'], [1.0, 'wallonia', 2010, 'CO2'], [1.0, 'wallonia', 2011, 'CO2'], [1.0, 'wallonia', 2012, 'CO2'], [1.0, 'wallonia', 2013, 'CO2'], [1.0, 'wallonia', 2014, 'CO2'], [1.0, 'wallonia', 2015, 'CO2'], [1.0, 'wallonia', 2016, 'CO2'], [1.0, 'wallonia', 2017, 'CO2'], [1.0, 'wallonia', 2018, 'CO2'], [1.0, 'wallonia', 2019, 'CO2'], [0.88, 'wallonia', 2020, 'CO2'], [0.705, 'wallonia', 2025, 'CO2'], [0.588, 'wallonia', 2030, 'CO2'], [0.588, 'wallonia', 2035, 'CO2'], [0.588, 'wallonia', 2040, 'CO2'], [0.588, 'wallonia', 2045, 'CO2'], [0.588, 'wallonia', 2050, 'CO2']])
    # insitu-emissions[Mt] (replace) = insitu-emissions[Mt] * calibration_fix[%]  calibration_fix[%] set to 1 if missing
    insitu_emissions_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=out_1547_1, operation_selection='x * y', output_name='insitu-emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # Keep only CO2
    insitu_emissions_Mt = insitu_emissions_Mt.loc[insitu_emissions_Mt['gaes'].isin(['CO2'])].copy()

    # Apply CC levers 
    # => determine the quantity of CO2 emissions than can be capture
    # => decrease CO2 emissions

    # CC[Mt] = insitu-emissions[Mt] * CCS-ratio-elec[%]  if missing CCS-ratio-elec set to 0
    CC_Mt = mcd(input_table_1=insitu_emissions_Mt, input_table_2=CCS_ratio_elec_percent, operation_selection='x * y', output_name='CC[Mt]', fill_value_bool='Left [x] Outer Join')
    # Keep CO2
    CC_Mt_2 = CC_Mt.loc[CC_Mt['gaes'].isin(['CO2'])].copy()
    # primary-energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    primary_energy_demand_TWh_2 = mcd(input_table_1=CC_Mt_2, input_table_2=out_9504_1, operation_selection='x * y', output_name='primary-energy-demand[TWh]')
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier, co2-concentration
    primary_energy_demand_TWh_2 = group_by_dimensions(df=primary_energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes', 'co2-concentration'], aggregation_method='Sum')
    # primary-energy-demand [TWh]  only for Carbon capture  DO NOT ADD TO  primary-energy-demand (already in)
    primary_energy_demand_TWh_2 = export_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')

    # CC primary energy demand

    # primary-energy-demand [TWh]
    primary_energy_demand_TWh_2 = use_variable(input_table=primary_energy_demand_TWh_2, selected_variable='primary-energy-demand[TWh]')
    # sector = electricity
    primary_energy_demand_TWh_2['sector'] = "electricity"

    # Emissions Capture

    # CC [Mt]
    CC_Mt = use_variable(input_table=CC_Mt, selected_variable='CC[Mt]')
    # Group by  country, years, gaes, way-of-prod, primary-energy-carrier
    CC_Mt = group_by_dimensions(df=CC_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')
    # sector = electricity
    CC_Mt['sector'] = "electricity"
    # emissions[Mt] (replace) = emissions[Mt] * calibration_fix[%]  calibration_fix[%] set to 1 if missing
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_1547_1, operation_selection='x * y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    # Emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Group by  country, years, way-of-prod
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'way-of-production', 'primary-energy-carrier', 'gaes'], aggregation_method='Sum')
    # sector = electricity
    emissions_Mt['sector'] = "electricity"

    return cal_rate, net_energy_production_TWh_3, energy_imported_TWh_2, out_6436_1, MEUR, primary_energy_demand_TWh, emissions_Mt, CC_Mt, total_capacities_GW, cum_new_capacities_GW, capacity_factor_percent, backup_capacity_GW, primary_energy_demand_TWh_2, existing_capacities_GW_2


