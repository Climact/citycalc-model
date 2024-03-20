import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


def metanode_9702(port_01, port_02):

    module_name = 'electricity_supply'
    # energy-carrier = electricity
    port_02 = port_02.loc[port_02['energy-carrier'].isin(['electricity'])].copy()

    # Assign res-categories

    # Link between RES-categories and energy-carrier
    out_9675_1 = pd.DataFrame(columns=['primary-energy-carrier', 'res-category'], data=[['electricity', 'electricity'], ['gaseous-bio', 'biofuels'], ['gaseous-ff', 'non-res'], ['gaseous-ff-natural', 'non-res'], ['gaseous-syn', 'efuels'], ['heat', 'heat'], ['heat-geothermal', 'res'], ['heat-solar', 'res'], ['hydrogen', 'hydrogen'], ['liquid-bio', 'biofuels'], ['liquid-bio-diesel', 'biofuels'], ['liquid-bio-gasoline', 'biofuels'], ['liquid-bio-kerosene', 'biofuels'], ['liquid-bio-marinefueloil', 'biofuels'], ['liquid-ff', 'non-res'], ['liquid-ff-crudeoil', 'non-res'], ['liquid-ff-diesel', 'non-res'], ['liquid-ff-gasoline', 'non-res'], ['liquid-ff-kerosene', 'non-res'], ['liquid-ff-marinefueloil', 'non-res'], ['liquid-ff-oil', 'non-res'], ['liquid-syn', 'efuels'], ['liquid-syn-diesel', 'efuels'], ['liquid-syn-gasoline', 'efuels'], ['liquid-syn-kerosene', 'efuels'], ['liquid-syn-marinefueloil', 'efuels'], ['RES-electricity-hp', 'res'], ['solid-biomass', 'biofuels'], ['solid-ff-coal', 'non-res'], ['solid-waste', 'non-res'], ['solid-waste-nonres', 'non-res'], ['solid-waste-res', 'biofuels'], ['res', 'res'], ['non-res', 'non-res']])
    # default-pct-res[-] = 1
    default_pct_res = out_9675_1.assign(**{'default-pct-res[-]': 1.0})
    # energy-production[TWh] = energy-production[TWh] * default-pct-res[-]
    energy_production_TWh = mcd(input_table_1=port_02, input_table_2=default_pct_res, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, res-categories (sum)
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'res-category'], aggregation_method='Sum')
    # primary-energy-carrier as energy-carrier
    out_9685_1 = default_pct_res.rename(columns={'primary-energy-carrier': 'energy-carrier'})
    # energy-demand[TWh] = energy-demand[TWh] * default-pct-res[-]
    energy_demand_TWh = mcd(input_table_1=port_01, input_table_2=out_9685_1, operation_selection='x * y', output_name='energy-demand[TWh]')
    # Group by  Country, Years, res-category, sector (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'sector', 'res-category'], aggregation_method='Sum')
    # Bottom res-category = electricity
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['res-category'].isin(['electricity'])].copy()
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['res-category'].isin(['electricity'])].copy()

    # Get % of RES in electricity and apply it to all sector requiring electricity


    def helper_9692(input_table_1, input_table_2) -> pd.DataFrame:
        # ----------------------------------------------------------------------------------------------------------- #
        # Input tables
        ## electricity demand by sector
        demand_table = input_table_1.copy()
        ## electricity production
        production_table = input_table_2.copy()
        ## Non-res vs res electricity production
        mask = (production_table['res-category'] == 'res')
        res_production = production_table.loc[mask, :]
        nonres_production = production_table.loc[~mask, :]
        ##Column names
        resCatCol = 'res-category'
        energyDemandCol = 'energy-demand[TWh]'
        energyProdCol = 'energy-production[TWh]'
        sectorCol = 'sector'
        
        # ----------------------------------------------------------------------------------------------------------- #
        # LOOP : get res production for first : H2 and secondly : efuels
        for fuel in ["hydrogen", "efuels"]:
            # ----------------------------------------------------------------------------------------------------------- #
            # Get values
            mask = (demand_table[sectorCol] == fuel)
            fuel_demand = demand_table.loc[mask, :] ## Data for specific fuels (H2, ...)
            demand_table = demand_table.loc[~mask, :] ## Remove this specific fuel from initial table
            del fuel_demand[resCatCol]
            # Merge with res production
            common_cols = list(set(fuel_demand) & set(res_production))
            fuel_production = res_production.merge(fuel_demand, on=common_cols, how='inner')
            
            # ----------------------------------------------------------------------------------------------------------- #
            # DEMAND : Get differences => accordings to it, define TWh send to res and non-res
            ## RES
            res = fuel_production.copy()
            res["temp"] = res[energyDemandCol] # Defaut : all elec demand = from RES
            mask = (res[energyProdCol] < res[energyDemandCol]) # If not enough res available => define appropriate demand
            res.loc[mask, "temp"] = res.loc[mask, energyProdCol]
            ## NON-RES
            nonres = fuel_production.copy()
            nonres["temp"] = 0 # Defaut : all elec demand = from RES => Here 0
            mask = (nonres[energyProdCol] < nonres[energyDemandCol]) # If not enough res available => we fill demand with non-res / biofuels
            nonres.loc[mask, "temp"] = nonres.loc[mask, energyDemandCol] - nonres.loc[mask, energyProdCol]
            nonres[resCatCol] = "electricity" # Need to be allocated between non-res and biofuels
            ## Concat RES / NON-RES
            fuel_demand = pd.concat([res, nonres], ignore_index=True)
            for col in [energyDemandCol, energyProdCol]:
                del fuel_demand[col]
            fuel_demand = fuel_demand.rename(columns={"temp": energyDemandCol})
            demand_table = pd.concat([demand_table, fuel_demand], ignore_index=True)
            
            # ----------------------------------------------------------------------------------------------------------- #
            # PRODUCTION : Adapt RES values according to res demand
            for col in [sectorCol, resCatCol, energyDemandCol, energyProdCol]:
                del res[col]
            common_cols = list(set(res_production) & set(res))
            res_production = res_production.merge(res, on=common_cols, how='left')
            res_production[energyProdCol] = res_production[energyProdCol] - res_production["temp"]
            del res_production["temp"]
        
        # ----------------------------------------------------------------------------------------------------------- #
        # CONCATENATE : res and non-res production and distribute remaining res between remaining demand
        production = pd.concat([res_production, nonres_production], ignore_index=True)
        total_production = production.groupby(['Country', 'Years']).sum().reset_index()
        total_production = total_production.rename(columns={energyProdCol: "total"})
        pct_production = production.merge(total_production, on=['Country', 'Years'], how='inner')
        pct_production["pct[-]"] = pct_production[energyProdCol] / pct_production["total"]
        for col in [energyProdCol, "total"]:
            del pct_production[col]
            
        # Apply pct to demand table
        mask = (demand_table[resCatCol] == "electricity") # Keep only res category that are not yet defined (=electricity)
        nonres = demand_table.loc[mask, :]
        res = demand_table.loc[~mask, :]
        del nonres[resCatCol] # electricity category will be replaced by bio / res / nonres
        common_cols = list(set(nonres) & set(pct_production))
        nonres = pct_production.merge(nonres, on=common_cols, how='inner')
        nonres[energyDemandCol] = nonres[energyDemandCol] * nonres["pct[-]"]
        
        # Concat res / non-res
        final_demand = pd.concat([res, nonres], ignore_index=True)
        
        # ----------------------------------------------------------------------------------------------------------- #
        # Output
        output_table = final_demand
        return output_table
    # Set % of res / non-res / biofuels to Hydrogen, efuels and other sectors  First : res goes to H2, then to efuels, the to the remaining sectors
    out_9692_1 = helper_9692(input_table_1=energy_demand_TWh_excluded, input_table_2=energy_production_TWh)
    # pct [-]
    pct = use_variable(input_table=out_9692_1, selected_variable='pct[-]')
    # Keep sector = tra (default choice to be sure we have final electricity values)
    pct = pct.loc[pct['sector'].isin(['tra'])].copy()
    # sector = electricity
    pct['sector'] = "electricity"
    # energy-demand [TWh]
    energy_demand_TWh_2 = use_variable(input_table=out_9692_1, selected_variable='energy-demand[TWh]')
    # Group by  Country, Years, res-category, sector (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector', 'res-category'], aggregation_method='Sum')
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_2])
    # Bottom sector = electricity
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['electricity'])].copy()
    energy_demand_TWh_2 = energy_demand_TWh.loc[~energy_demand_TWh['sector'].isin(['electricity'])].copy()
    # Group by  Country, Years, sector (sum)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * pct[-]  We have to recompute values for electricity sector sector because % depends on which quantity of res has been attributed to hydrogen and efuels
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=pct, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh])
    # Keep sector = hydrogen
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['hydrogen'])].copy()

    # Get % of RES in hydrogen and apply it to all sector requiring H2

    # Group by  Country, Years, res-category, sector (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector', 'res-category'], aggregation_method='Sum')
    # Group by  Country, Years, sector (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # pct[-] = energy-demand[TWh] (by res-category) / energy-demand[TWh] (total)
    pct = mcd(input_table_1=energy_demand_TWh_3, input_table_2=energy_demand_TWh_2, operation_selection='x / y', output_name='pct[-]')
    # Set to 0
    pct = missing_value(df=pct, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by Country, Years, res-category (sum)
    pct = group_by_dimensions(df=pct, groupby_dimensions=['Country', 'Years', 'res-category'], aggregation_method='Sum')
    # Bottom res-category = hydrogen
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['res-category'].isin(['hydrogen'])].copy()
    energy_demand_TWh_2 = energy_demand_TWh.loc[~energy_demand_TWh['res-category'].isin(['hydrogen'])].copy()
    # Group by Country, Years, sector (sum)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * pct[-]
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=pct, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh])
    # Keep sector = efuels
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['sector'].isin(['efuels'])].copy()

    # Get % of RES in efuels and apply it to all sector requiring efuels

    # Group by  Country, Years, sector (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Group by  Country, Years, res-category, sector (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector', 'res-category'], aggregation_method='Sum')
    # pct[-] = energy-demand[TWh] (by res-category) / energy-demand[TWh] (total)
    pct = mcd(input_table_1=energy_demand_TWh_2, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='pct[-]')
    # Set to 0
    pct = missing_value(df=pct, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by Country, Years, res-category (sum)
    pct = group_by_dimensions(df=pct, groupby_dimensions=['Country', 'Years', 'res-category'], aggregation_method='Sum')
    # Bottom res-category = efuels
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['res-category'].isin(['efuels'])].copy()
    energy_demand_TWh_2 = energy_demand_TWh.loc[~energy_demand_TWh['res-category'].isin(['efuels'])].copy()
    # Group by Country, Years, sector (sum)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * pct[-]
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=pct, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh_2 = pd.concat([energy_demand_TWh_2, energy_demand_TWh])
    # Bottom res-category = heat
    energy_demand_TWh_excluded = energy_demand_TWh_2.loc[energy_demand_TWh_2['res-category'].isin(['heat'])].copy()
    energy_demand_TWh = energy_demand_TWh_2.loc[~energy_demand_TWh_2['res-category'].isin(['heat'])].copy()

    # Get % of RES in heat and apply it to all sector requiring heat

    # Group by Country, Years, sector (sum)
    energy_demand_TWh_excluded = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Keep sector = heat
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[energy_demand_TWh_2['sector'].isin(['heat'])].copy()
    # Group by  Country, Years, sector (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Group by  Country, Years, res-category, sector (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector', 'res-category'], aggregation_method='Sum')
    # pct[-] = energy-demand[TWh] (by res-category) / energy-demand[TWh] (total)
    pct = mcd(input_table_1=energy_demand_TWh_2, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='pct[-]')
    # Set to 0
    pct = missing_value(df=pct, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Group by Country, Years, res-category (sum)
    pct = group_by_dimensions(df=pct, groupby_dimensions=['Country', 'Years', 'res-category'], aggregation_method='Sum')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * pct[-]
    energy_demand_TWh_2 = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=pct, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh_3 = pd.concat([energy_demand_TWh, energy_demand_TWh_2])
    # Bottom res-category = res (RES without biomass)
    energy_demand_TWh_2 = energy_demand_TWh_3.loc[energy_demand_TWh_3['res-category'].isin(['res'])].copy()

    # Compute pct of RES total (with or without biomass)

    # Group by Country, Years (sum)
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Compute pct of RES per sector (with or without biomass)

    # Group by Country, Years, sector (sum)
    energy_demand_TWh_4 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Bottom res-category = res, biofuels (RES including biomass)
    energy_demand_TWh_5 = energy_demand_TWh_3.loc[energy_demand_TWh_3['res-category'].isin(['biofuels', 'res'])].copy()
    # Group by Country, Years (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Group by Country, Years, sector (sum)
    energy_demand_TWh_6 = group_by_dimensions(df=energy_demand_TWh_5, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # Bottom res-category = nonres, res, biofuels (Total)
    energy_demand_TWh_3 = energy_demand_TWh_3.loc[energy_demand_TWh_3['res-category'].isin(['biofuels', 'non-res', 'res'])].copy()
    # Group by Country, Years, sector (sum)
    energy_demand_TWh_5 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'sector'], aggregation_method='Sum')
    # pct-res-with-biomass-by-sector[-] = energy-demand[TWh] (RES) / energy-demand[TWh] (Total)
    pct_res_with_biomass_by_sector = mcd(input_table_1=energy_demand_TWh_6, input_table_2=energy_demand_TWh_5, operation_selection='x / y', output_name='pct-res-with-biomass-by-sector[-]')
    # Set to 0
    pct_res_with_biomass_by_sector = missing_value(df=pct_res_with_biomass_by_sector, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # pct-res-without-biomass-by-sector[-] = energy-demand[TWh] (RES) / energy-demand[TWh] (Total)
    pct_res_without_biomass_by_sector = mcd(input_table_1=energy_demand_TWh_4, input_table_2=energy_demand_TWh_5, operation_selection='x / y', output_name='pct-res-without-biomass-by-sector[-]')
    # Set to 0
    pct_res_without_biomass_by_sector = missing_value(df=pct_res_without_biomass_by_sector, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    pct_res_biomass_by_sector = pd.concat([pct_res_without_biomass_by_sector, pct_res_with_biomass_by_sector])
    # Group by Country, Years (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # pct-res-with-biomass-total[-] = energy-demand[TWh] (RES) / energy-demand[TWh] (Total)
    pct_res_with_biomass_total = mcd(input_table_1=energy_demand_TWh_2, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='pct-res-with-biomass-total[-]')
    # Set to 0
    pct_res_with_biomass_total = missing_value(df=pct_res_with_biomass_total, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # pct-res-without-biomass-total[-] = energy-demand[TWh] (RES) / energy-demand[TWh] (Total)
    pct_res_without_biomass_total = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_demand_TWh_3, operation_selection='x / y', output_name='pct-res-without-biomass-total[-]')
    # Set to 0
    pct_res_without_biomass_total = missing_value(df=pct_res_without_biomass_total, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    pct_res_biomass_total = pd.concat([pct_res_without_biomass_total, pct_res_with_biomass_total])
    pct_res_biomass = pd.concat([pct_res_biomass_by_sector, pct_res_biomass_total])

    return pct_res_biomass


