import pandas as pd

from patex.nodes.globals import Globals
from patex.nodes import *


# Industry module
def metanode_9099(port_01, port_02, port_03, port_04):
    # Input Data + inject variables names (module)


    # Data from lifestyle


    # Apply demand-inland levers (avoid)
    # => determine the km demand for freight for inland


    # Distinction between Bio-/Syn-fuels and others fuels (energy-carrier-category)


    # Apply here MCE 
    # => based on  ?? eco_added-value-industry ??
    # 
    # A appliquer sur FTS / OTS importé ou sur tout (après apply link-material-to-activity) ?


    # LIMITATION
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
    # no import/export for exogenous product


    # LIMITATION
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
    # no import/export for exogenous product


    # note : Several power plants are not considered for now (partially because need of feedback loop because capacity of gas depend on outputs of module ELC)


    # QUICK FIX


    # TEMP > delete when previous has been migrated



    module_name = 'industry'

    # Formating data for other modules + Pathway Explorer

    # For : Minerals
    # 
    # - Material production

    out_7741_1 = pd.DataFrame(columns=['new_name', 'old_name'], data=[['material-production_aluminium[kt]', 'ind_material-production-per-material_aluminium[kt]'], ['material-production_cement[kt]', 'ind_material-production-per-material_cement[kt]'], ['material-production_ceramic[kt]', 'ind_material-production-per-material_ceramic[kt]'], ['material-production_chemical-chlorine[kt]', 'ind_material-production-per-material_chemical-chlorine[kt]'], ['material-production_chemical-olefin[kt]', 'ind_material-production-per-material_chemical-olefin[kt]'], ['material-production_chemical-other[kt]', 'ind_material-production-per-material_chemical-other[kt]'], ['material-production_glass[kt]', 'ind_material-production-per-material_glass[kt]'], ['material-production_lime[kt]', 'ind_material-production-per-material_lime[kt]'], ['material-production_non-ferrous[kt]', 'ind_material-production-per-material_non-ferrous[kt]'], ['material-production_other-industries[kt]', 'ind_material-production-per-material_other-industries[kt]'], ['material-production_paper[kt]', 'ind_material-production-per-material_paper[kt]'], ['material-production_steel[kt]', 'ind_material-production-per-material_steel[kt]'], ['material-production_wood[kt]', 'ind_material-production-per-material_wood[kt]']])
    out_7739_1 = pd.DataFrame(columns=['new_name', 'old_name'], data=[['material-production_aluminium_tech[kt]', 'ind_material-production_aluminium_tech[kt]'], ['material-production_cement_dry-kiln[kt]', 'ind_material-production_cement_dry-kiln[kt]'], ['material-production_cement_geopolym[kt]', 'ind_material-production_cement_geopolym[kt]'], ['material-production_cement_wet-kiln[kt]', 'ind_material-production_cement_wet-kiln[kt]'], ['material-production_ceramic_tech[kt]', 'ind_material-production_ceramic_tech[kt]'], ['material-production_chemical-chlorine_tech[kt]', 'ind_material-production_chemical-chlorine_tech[kt]'], ['material-production_chemical-olefin_tech[kt]', 'ind_material-production_chemical-olefin_tech[kt]'], ['material-production_chemical-other_tech[kt]', 'ind_material-production_chemical-other_tech[kt]'], ['material-production_glass_tech[kt]', 'ind_material-production_glass_tech[kt]'], ['material-production_lime_tech[kt]', 'ind_material-production_lime_tech[kt]'], ['material-production_non-ferrous_tech[kt]', 'ind_material-production_non-ferrous_tech[kt]'], ['material-production_other-industries_tech[kt]', 'ind_material-production_other-industries_tech[kt]'], ['material-production_paper_recycling[kt]', 'ind_material-production_paper_recycling[kt]'], ['material-production_paper_woodpulp[kt]', 'ind_material-production_paper_woodpulp[kt]'], ['material-production_steel_BF-BOF[kt]', 'ind_material-production_steel_BF-BOF[kt]'], ['material-production_steel_HIsarna[kt]', 'ind_material-production_steel_HIsarna[kt]'], ['material-production_steel_hydrogen-DRI[kt]', 'ind_material-production_steel_hydrogen-DRI[kt]'], ['material-production_steel_scrap-EAF[kt]', 'ind_material-production_steel_scrap-EAF[kt]'], ['material-production_wood_tech[kt]', 'ind_material-production_wood_tech[kt]']])

    # For : Pathway Explorer
    # 
    # - Energy-demand by material and energy-carrier

    # SHOULD BE IN GOOGLE SHEET ?

    # material group
    out_7718_1 = pd.DataFrame(columns=['material', 'material-group'], data=[['aluminium', 'metals-group'], ['steel', 'metals-group'], ['non-ferrous', 'metals-group'], ['cement', 'non-metallic-minerals-group'], ['ceramic-and-others', 'non-metallic-minerals-group'], ['glass', 'non-metallic-minerals-group'], ['lime', 'non-metallic-minerals-group'], ['paper', 'other-industries-group'], ['food', 'other-industries-group'], ['other-industries', 'other-industries-group'], ['wood', 'other-industries-group'], ['chemical-olefin', 'chemicals-group'], ['chemical-other', 'chemicals-group'], ['chemical-chlorine', 'chemicals-group'], ['chemical-ammonia', 'chemicals-group']])
    # Agriculture
    out_9267_1 = port_04

    # TEMP > delete when previous has been migrated

    # Data from agriculture

    # food-demand [t]
    food_demand_t = use_variable(input_table=out_9267_1, selected_variable='food-demand[t]')
    # unit = t
    food_demand_t['unit'] = "t"
    # product = food
    food_demand_t['product'] = "food"
    # food-demand[t] as product-demand[unit]
    out_9275_1 = food_demand_t.rename(columns={'food-demand[t]': 'product-demand[unit]'})
    # fertilizer-application [t]
    fertilizer_application_t = use_variable(input_table=out_9267_1, selected_variable='fertilizer-application[t]')

    # Data from power 
    # (coming directly from levers, no data received from ELC module as it is downstream of the IND module)
    # note : infrastructure-electric-cable is estimated as an exogenous product further in this module.

    # capacity [GW]
    capacity_GW = import_data(trigram='elc', variable_name='capacity')
    # Same as last available year
    capacity_GW = add_missing_years(df_data=capacity_GW)
    # Convert Unit GW to MW
    capacity_MW = capacity_GW.drop(columns='capacity[GW]').assign(**{'capacity[MW]': capacity_GW['capacity[GW]'] * 1000.0})
    # unit = MW
    capacity_MW['unit'] = "MW"
    # way-of-product to product
    out_9768_1 = capacity_MW.rename(columns={'way-of-production': 'product'})
    # capacity[MW] as product-demand[unit]
    out_9738_1 = out_9768_1.rename(columns={'capacity[MW]': 'product-demand[unit]'})
    # included : geo, solar,  wind and nuclear excluded : fossil plants,  CHP, bio-waste,  marine, hydraulic
    out_9738_1 = out_9738_1.loc[out_9738_1['product'].isin(['RES-solar-pv', 'RES-geothermal', 'RES-solar-csp', 'elec-plant-with-nuclear', 'RES-wind-offshore', 'RES-wind-onshore'])].copy()

    def helper_9771(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('RES-geothermal', 'geothermal', regex=False)
        output_table["product"] = output_table["product"].str.replace('RES-solar-csp', 'solar-CSP', regex=False)
        output_table["product"] = output_table["product"].str.replace('RES-solar-pv', 'solar-PV', regex=False)
        output_table["product"] = output_table["product"].str.replace('RES-wind-offshore', 'wind-offshore', regex=False)
        output_table["product"] = output_table["product"].str.replace('RES-wind-onshore', 'wind-onshore', regex=False)
        output_table["product"] = output_table["product"].str.replace('elec-plant-with-nuclear', 'nuclear', regex=False)
        return output_table
    # RES-solar-pv ==>solar-PV RES-geothermal ==>geothermal RES-solar-csp ==>solar-CSP elec-plant-with-nuclear ==>nuclear RES-wind-offshore ==>wind-offshore RES-wind-onshore ==>wind-onshore  => REMOVE THIS AND APPLY CHANGES IN INDUSTRY GOOGLE SHEET
    out_9771_1 = helper_9771(input_table=out_9738_1)
    # unit = t
    fertilizer_application_t['unit'] = "t"
    # product = fertilizer
    fertilizer_application_t['product'] = "fertilizer"
    # fertilizer-application[t] as product-demand[unit]
    out_9274_1 = fertilizer_application_t.rename(columns={'fertilizer-application[t]': 'product-demand[unit]'})
    out_1 = pd.concat([out_9275_1, out_9274_1.set_index(out_9274_1.index.astype(str) + '_dup')])

    def helper_9733(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('food', 'processed-food', regex=False)
        return output_table
    # food (AGR) ==> processed-food (IND)  => REMOVE THIS AND APPLY CHANGES IN INDUSTRY GOOGLE SHEET
    out_9733_1 = helper_9733(input_table=out_1)
    # Transport
    out_7509_1 = port_03

    # Data from transport

    # material-length-demand [km]
    material_length_demand_km = use_variable(input_table=out_7509_1, selected_variable='material-length-demand[km]')
    # unit = km
    material_length_demand_km['unit'] = "km"
    # material to product
    out_8127_1 = material_length_demand_km.rename(columns={'material': 'product'})
    # material-length-demand[km] to product-demand[unit]
    out_8129_1 = out_8127_1.rename(columns={'material-length-demand[km]': 'product-demand[unit]'})
    # Group by  Country, Years, product, unit (sum)
    out_8129_1 = group_by_dimensions(df=out_8129_1, groupby_dimensions=['Country', 'Years', 'unit', 'product'], aggregation_method='Sum')

    def helper_8158(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('roads', 'infrastructure-road', regex=False)
        output_table["product"] = output_table["product"].str.replace('rails', 'infrastructure-rail', regex=False)
        output_table["product"] = output_table["product"].str.replace('trolley-cables', 'infrastructure-catenary', regex=False)
        return output_table
    # roads (TRA) ==> infrastructure-road (IND) rails (TRA) ==> infrastructure-rail (IND) trolley-cables (TRA) ==> infrastructure-catenary (IND)  => REMOVE THIS AND APPLY CHANGES IN INDUSTRY GOOGLE SHEET
    out_8158_1 = helper_8158(input_table=out_8129_1)
    # final-veh-fleet-ind [number]
    final_veh_fleet_ind_number = use_variable(input_table=out_7509_1, selected_variable='final-veh-fleet-ind[number]')
    # unit = num
    final_veh_fleet_ind_number['unit'] = "num"
    # join vehicule-type and motot-type as product
    final_veh_fleet_ind_number = string_manipulation(df=final_veh_fleet_ind_number, expression='join($vehicule-type$, "-", $motor-type$)', var_name='product')
    # final-veh-fleet-ind[number] to product-demand[unit]
    out_8128_1 = final_veh_fleet_ind_number.rename(columns={'final-veh-fleet-ind[number]': 'product-demand[unit]'})
    out_8128_1 = out_8128_1.loc[~out_8128_1['vehicule-type'].isin(['bus'])].copy()
    # Group by  Country, Years, product, unit (sum)
    out_8128_1 = group_by_dimensions(df=out_8128_1, groupby_dimensions=['Country', 'Years', 'unit', 'product'], aggregation_method='Sum')

    def helper_8157(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('-same', '', regex=False)
        output_table["product"] = output_table["product"].str.replace('rail', 'trains', regex=False)
        return output_table
    # remove "-same" rail to trains  => REMOVE THIS AND APPLY CHANGES IN INDUSTRY GOOGLE SHEET
    out_8157_1 = helper_8157(input_table=out_8128_1)
    out_1 = pd.concat([out_8157_1, out_8158_1.set_index(out_8158_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_9733_1.set_index(out_9733_1.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1, out_9771_1.set_index(out_9771_1.index.astype(str) + '_dup')])
    # Buildings
    out_7508_1 = port_02

    # Data from buildings

    # floor-area-yearly [m2]
    floor_area_yearly_m2 = use_variable(input_table=out_7508_1, selected_variable='floor-area-yearly[m2]')
    # unit = m2
    floor_area_yearly_m2['unit'] = "m2"
    # join area-type,  building-type and  renovation-category as product
    floor_area_yearly_m2 = string_manipulation(df=floor_area_yearly_m2, expression='join($area-type$, "-", $building-type$, "-",$renovation-category$)', var_name='product')
    # floor-area-yearly[m2] to product-demand[unit]
    out_8103_1 = floor_area_yearly_m2.rename(columns={'floor-area-yearly[m2]': 'product-demand[unit]'})
    # Group by  Country, Years, product, unit (sum)
    out_8103_1 = group_by_dimensions(df=out_8103_1, groupby_dimensions=['Country', 'Years', 'unit', 'product'], aggregation_method='Sum')

    def helper_6732(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('constructed-', 'construction-', regex=False)
        output_table["product"] = output_table["product"].str.replace('renovated-', 'renovation-', regex=False)
        output_table["product"] = output_table["product"].str.replace('dep', 'deep', regex=False)
        output_table["product"] = output_table["product"].str.replace('shl', 'shallow', regex=False)
        output_table["product"] = output_table["product"].str.replace('med-high', 'mid-high', regex=False)
        output_table["product"] = output_table["product"].str.replace('med-low', 'mid-low', regex=False)
        return output_table
    # constructed- ==> construction-  renovated- ==> renovation- dep ==> deep med-low ==> mid-low med-high ==> mid-high shl ==> shallow
    out_6732_1 = helper_6732(input_table=out_8103_1)
    # new-appliances [num]
    new_appliances_num = use_variable(input_table=out_7508_1, selected_variable='new-appliances[num]')
    # unit = num
    new_appliances_num['unit'] = "num"
    # appliances to product
    out_8107_1 = new_appliances_num.rename(columns={'appliances': 'product'})
    # new-appliances[num] to product-demand[unit]
    out_8106_1 = out_8107_1.rename(columns={'new-appliances[num]': 'product-demand[unit]'})
    # Group by  Country, Years, product, unit (sum)
    out_8106_1 = group_by_dimensions(df=out_8106_1, groupby_dimensions=['Country', 'Years', 'unit', 'product'], aggregation_method='Sum')

    def helper_8156(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('dishwasher', 'appliance-dishwashers', regex=False)
        output_table["product"] = output_table["product"].str.replace('fridge', 'appliance-fridges', regex=False)
        output_table["product"] = output_table["product"].str.replace('others', 'electronic-smartphone', regex=False)
        output_table["product"] = output_table["product"].str.replace('wmachine', 'appliance-wash-machines', regex=False)
        output_table["product"] = output_table["product"].str.replace('freezer', 'appliance-freezer', regex=False)
        output_table["product"] = output_table["product"].str.replace('dryer', 'appliance-dryer', regex=False)
        output_table["product"] = output_table["product"].str.replace('tv', 'electronic-tv', regex=False)
        output_table["product"] = output_table["product"].str.replace('computer', 'electronic-computer', regex=False)
        return output_table
    # dishwasher (BLD) => appliance-dishwashers (IND) fridge (BLD) => appliance-fridges (IND) others (BLD) => electronic-smartphone (IND) computer (BLD) => electronic-computer (IND) tv (BLD) => electronic-tv (IND) wmachine (BLD) => appliance-wash-machines (IND) freezer (BLD) => appliance-freezer (IND) dryer (BLD) => dryer-machines (IND)  => REMOVE THIS AND APPLY CHANGES IN INDUSTRY GOOGLE SHEET
    out_8156_1 = helper_8156(input_table=out_8106_1)
    # new-pipeline-length [km]
    new_pipes_length_km = use_variable(input_table=out_7508_1, selected_variable='new-pipes-length[km]')
    # unit = km
    new_pipes_length_km['unit'] = "km"
    # product = pipes
    new_pipes_length_km['product'] = "pipes"
    # new-pipeline-length[km] to product-demand[unit]
    out_8111_1 = new_pipes_length_km.rename(columns={'new-pipes-length[km]': 'product-demand[unit]'})
    # Group by  Country, Years, product, unit (sum)
    out_8111_1 = group_by_dimensions(df=out_8111_1, groupby_dimensions=['Country', 'Years', 'unit', 'product'], aggregation_method='Sum')

    def helper_9801(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["product"] = output_table["product"].str.replace('pipes', 'infrastructure-pipes', regex=False)
        return output_table
    # pipes ==> infrastructure-pipes
    out_9801_1 = helper_9801(input_table=out_8111_1)
    out_1_2 = pd.concat([out_9801_1, out_6732_1.set_index(out_6732_1.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_1_2, out_8156_1.set_index(out_8156_1.index.astype(str) + '_dup')])
    # Lifestyle
    out_5965_1 = port_01
    out_1_2 = pd.concat([out_5965_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    out_1 = pd.concat([out_1_2, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # product-demand [unit]
    product_demand_unit = export_variable(input_table=out_1, selected_variable='product-demand[unit]')

    # Product / material resources demand

    # Endogenous product demand & production

    # Calibration

    # product-demand [unit]
    product_demand_unit_2 = use_variable(input_table=product_demand_unit, selected_variable='product-demand[unit]')
    # CP ind_params
    ind_params = import_data(trigram='ind', variable_name='ind_params', variable_type='CP')
    # apply-calibration 1 if disable = 0 0 if disable = 1
    disable_product_calibration = ind_params[ind_params['metric-name'] == 'disable_product_calibration'].iloc[0]['value'] == 1
    # Calibration product-demand [unit]
    product_demand_unit = import_data(trigram='ind', variable_name='product-demand', variable_type='Calibration')
    # Apply Calibration on product-demand [unit]
    product_demand_unit_2, _, out_7958_3 = calibration(input_table=product_demand_unit_2, cal_table=product_demand_unit, apply_calib=not disable_product_calibration, data_to_be_cal='product-demand[unit]', data_cal='product-demand[unit]')

    # Apply product-import-share & product-export-share levers
    # => determine the xxx

    # product-demand [unit]
    product_demand_unit_2 = use_variable(input_table=product_demand_unit_2, selected_variable='product-demand[unit]')

    # Parameters

    # OTS/FTS product-export-share [%]
    product_export_share_percent = import_data(trigram='ind', variable_name='product-export-share')
    # product-export[unit] = product-demand[unit] * product-export-share[%]
    product_export_unit = mcd(input_table_1=product_demand_unit_2, input_table_2=product_export_share_percent, operation_selection='x * y', output_name='product-export[unit]')

    # For : Pathway Explorer
    # 
    # - Product domestic share of production

    # product-export-share [%]
    product_export_share_percent_demand = use_variable(input_table=product_export_share_percent, selected_variable='product-export-share[%demand]')
    # OTS/FTS product-import-share [%]
    product_import_share_percent = import_data(trigram='ind', variable_name='product-import-share')
    # product-import-share [%demand] = min(product-import-share,100%)
    product_import_share_percent_demand = math_formula(df=product_import_share_percent, convert_to_int=False, replaced_column='product-import-share[%demand]', splitted='min_in_args($product-import-share[%demand]$,1)')
    # product-import[unit] = product-demand[unit] * product-import-share[%]
    product_import_unit = mcd(input_table_1=product_demand_unit_2, input_table_2=product_import_share_percent_demand, operation_selection='x * y', output_name='product-import[unit]')

    # For : Scope 2 & 3
    # 
    # - imported products and imported materials (after calibration)
    # - note : imported subproducts only comprises batteries, as other subproducts are the same as products (and thus no imports to avoid double counting). The filter dimension on battery in done to avoid confusion (as subproduct and products have the same name)
    # including imported batteries in indirect emissions does not *induce a double counting as it only include batteries emboddied in products that are manufactured domestically.

    # product-import [unit]
    product_import_unit_2 = use_variable(input_table=product_import_unit, selected_variable='product-import[unit]')
    # product-net-export[unit] = product-export[unit] - product-import[unit]
    product_net_export_unit = mcd(input_table_1=product_import_unit, input_table_2=product_export_unit, operation_selection='y - x', output_name='product-net-export[unit]')
    # product-production[unit] = product-demand[unit] + product-net-export[unit]
    product_production_unit = mcd(input_table_1=product_demand_unit_2, input_table_2=product_net_export_unit, operation_selection='x + y', output_name='product-production[unit]')
    # product-production [unit]
    product_production_unit = export_variable(input_table=product_production_unit, selected_variable='product-production[unit]')

    # Convert product to subproduct demand

    # product-production [unit]
    product_production_unit_2 = use_variable(input_table=product_production_unit, selected_variable='product-production[unit]')

    # Parameters

    # RCP product-to-subproduct-demand [unit/unit]
    product_to_subproduct_demand_unit_per_unit = import_data(trigram='ind', variable_name='product-to-subproduct-demand', variable_type='RCP')
    # Group by  Country, product, subproduct (sum) (remove unit)
    product_to_subproduct_demand_unit_per_unit = group_by_dimensions(df=product_to_subproduct_demand_unit_per_unit, groupby_dimensions=['Country', 'product', 'subproduct'], aggregation_method='Sum')
    # subproduct-demand[unit] = product-production[unit] * product-to-subproduct-demand [unit/unit]
    subproduct_demand_unit = mcd(input_table_1=product_production_unit_2, input_table_2=product_to_subproduct_demand_unit_per_unit, operation_selection='x * y', output_name='subproduct-demand[unit]')

    # Apply subproduct-import-share & subproduct-export-share levers
    # note : for now, most subproduct are = to product (e.g. 1 fertilizer = 1 fertilizer) ; we only have "battery" as specific subproduct (asked in BEV etc) ; the import/export of subproduct thus only apply for batteries

    # subproduct-demand [unit]
    subproduct_demand_unit = use_variable(input_table=subproduct_demand_unit, selected_variable='subproduct-demand[unit]')

    # Parameters

    # OTS/FTS subproduct-import-share [%]
    subproduct_import_share_percent = import_data(trigram='ind', variable_name='subproduct-import-share')
    # Same as last available year
    subproduct_import_share_percent = add_missing_years(df_data=subproduct_import_share_percent)
    # subproduct-import-share [%] = min(import-share,100%)
    subproduct_import_share_percent_demand = math_formula(df=subproduct_import_share_percent, convert_to_int=False, replaced_column='subproduct-import-share[%demand]', splitted='min_in_args($subproduct-import-share[%demand]$,1)')
    # subproduct-import[unit] = subproduct-demand[unit] * subproduct-import-share[%]
    subproduct_import_unit = mcd(input_table_1=subproduct_demand_unit, input_table_2=subproduct_import_share_percent_demand, operation_selection='x * y', output_name='subproduct-import[unit]')
    # subproduct-import [unit]
    subproduct_import_unit_2 = use_variable(input_table=subproduct_import_unit, selected_variable='subproduct-import[unit]')
    # keep battery
    subproduct_import_unit_2 = subproduct_import_unit_2.loc[subproduct_import_unit_2['subproduct'].isin(['battery'])].copy()
    # Concatenate
    import_unit = pd.concat([subproduct_import_unit_2, product_import_unit_2.set_index(product_import_unit_2.index.astype(str) + '_dup')])
    # OTS/FTS subproduct-export-share [%]
    subproduct_export_share_percent = import_data(trigram='ind', variable_name='subproduct-export-share')
    # Same as last available year
    subproduct_export_share_percent = add_missing_years(df_data=subproduct_export_share_percent)
    # subproduct-export[unit] = subproduct-demand[unit] * subproduct-export-share[%]
    subproduct_export_unit = mcd(input_table_1=subproduct_demand_unit, input_table_2=subproduct_export_share_percent, operation_selection='x * y', output_name='subproduct-export[unit]')
    # subproduct-net-export[unit] = subproduct-export[unit] - subproduct-import[unit]
    product_net_export_unit = mcd(input_table_1=subproduct_import_unit, input_table_2=subproduct_export_unit, operation_selection='y - x', output_name='product-net-export[unit]')
    # subproduct-production[unit] = subproduct-demand[unit] + subproduct-net-export[unit]
    subproduct_production_unit = mcd(input_table_1=subproduct_demand_unit, input_table_2=product_net_export_unit, operation_selection='x + y', output_name='subproduct-production[unit]')
    # subproduct-production [unit]
    subproduct_production_unit = export_variable(input_table=subproduct_production_unit, selected_variable='subproduct-production[unit]')

    # Material demand & production

    # Convert subproduct to material demand

    # subproduct-production [unit]
    subproduct_production_unit = use_variable(input_table=subproduct_production_unit, selected_variable='subproduct-production[unit]')

    # Parameters

    # RCP subproduct-to-material-demand [t/unit]
    subproduct_to_material_demand_t_per_unit = import_data(trigram='ind', variable_name='subproduct-to-material-demand', variable_type='RCP')
    # Group by  Country, subproduct, material (sum) (remove unit)
    subproduct_to_material_demand_t_per_unit = group_by_dimensions(df=subproduct_to_material_demand_t_per_unit, groupby_dimensions=['Country', 'material', 'subproduct'], aggregation_method='Sum')
    # material-demand[t] (only from  endogenous product) = subproduct-production[unit] * subproduct-to-material-demand [t/unit]
    material_demand_t = mcd(input_table_1=subproduct_production_unit, input_table_2=subproduct_to_material_demand_t_per_unit, operation_selection='x * y', output_name='material-demand[t]')

    # Apply material switch levers (switch)
    # => switch one material to another for each production of product

    # material-demand [t] (only from  endogenous product)
    material_demand_t = use_variable(input_table=material_demand_t, selected_variable='material-demand[t]')
    # OTS/FTS material-switch [%]
    material_switch_percent = import_data(trigram='ind', variable_name='material-switch')
    # retrieve-material[t] = material-demand[t] * material-switch[%]
    retrieve_material_t = mcd(input_table_1=material_demand_t, input_table_2=material_switch_percent, operation_selection='x * y', output_name='retrieve-material[t]')
    # Group by  Country, Years, product, material
    retrieve_material_t_2 = group_by_dimensions(df=retrieve_material_t, groupby_dimensions=['Country', 'Years', 'product', 'material'], aggregation_method='Sum')
    # material-demand[t] (replace) = material-demand[t] - retrieve-material[t]
    material_demand_t = mcd(input_table_1=material_demand_t, input_table_2=retrieve_material_t_2, operation_selection='x - y', output_name='material-demand[t]', fill_value_bool='Left [x] Outer Join')

    # Parameters

    # RCP material-switch-ratio [kg/kg]
    material_switch_ratio_kg_per_kg = import_data(trigram='ind', variable_name='material-switch-ratio', variable_type='RCP')
    # add-material[t] = retrieve-material[t] * material-switch-ratio[kg/kg]
    add_material_t = mcd(input_table_1=retrieve_material_t, input_table_2=material_switch_ratio_kg_per_kg, operation_selection='x * y', output_name='add-material[t]')
    # Group by  Country, Years, product, material-to
    add_material_t = group_by_dimensions(df=add_material_t, groupby_dimensions=['Country', 'Years', 'product', 'material-to'], aggregation_method='Sum')
    # material-to to material
    out_7950_1 = add_material_t.rename(columns={'material-to': 'material'})
    # material-demand[t] (replace) = material-demand[t] + add-material[t]
    material_demand_t = mcd(input_table_1=material_demand_t, input_table_2=out_7950_1, operation_selection='x + y', output_name='material-demand[t]', fill_value_bool='Left [x] Outer Join')

    # Apply material efficiency levers (improve)
    # => change the amount of material to be produced by improving material efficiency

    # material-demand [t] (only from  endogenous product)
    material_demand_t = use_variable(input_table=material_demand_t, selected_variable='material-demand[t]')
    # OTS/FTS material-efficiency [%]
    material_efficiency_percent = import_data(trigram='ind', variable_name='material-efficiency')

    # For : Minerals
    # 
    # - Material efficiency

    # material-efficiency [%]
    material_efficiency_percent_2 = use_variable(input_table=material_efficiency_percent, selected_variable='material-efficiency[%]')

    # Pivot

    # by material
    out_7846_1, _, _ = pivoting(df=material_efficiency_percent_2, agg_dict={'material-efficiency[%]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['material'])
    out_7847_1 = missing_value_column_filter(df=out_7846_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7847_1 = column_rename_regex(df=out_7847_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[%]')
    # Keep steel aluminium
    out_7847_1 = column_filter(df=out_7847_1, pattern='^Country$|^Years$|^.*steel.*$|^.*aluminium.*$')
    # Add ind_suffix
    out_7847_1 = column_rename_regex(df=out_7847_1, search_string='(.*\\[.*)', replace_string='ind_$1')
    # material-demand[t] (replace) = material-demand[t] * material-efficiency[%]
    material_demand_t = mcd(input_table_1=material_demand_t, input_table_2=material_efficiency_percent, operation_selection='x * y', output_name='material-demand[t]')
    # material-demand [t] (only from  endogenous product)
    material_demand_t = export_variable(input_table=material_demand_t, selected_variable='material-demand[t]')

    # For : Minerals
    # 
    # - Material switch

    # material-switch [%]
    material_switch_percent = use_variable(input_table=material_switch_percent, selected_variable='material-switch[%]')
    # Keep material = steel
    material_switch_percent = material_switch_percent.loc[material_switch_percent['material'].isin(['steel'])].copy()

    # Pivot

    # by product, material, material-to
    out_7860_1, _, _ = pivoting(df=material_switch_percent, agg_dict={'material-switch[%]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['product', 'material', 'material-to'])
    out_7861_1 = missing_value_column_filter(df=out_7860_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7861_1 = column_rename_regex(df=out_7861_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[%]')
    # product-import-share [%]
    product_import_share_percent_demand = use_variable(input_table=product_import_share_percent_demand, selected_variable='product-import-share[%demand]')

    # For : Minerals
    # 
    # - Product domestic share of production

    # Pivot

    # by product
    out_7964_1, _, _ = pivoting(df=product_import_share_percent_demand, agg_dict={'product-import-share[%demand]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['product'])
    out_7965_1 = missing_value_column_filter(df=out_7964_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7965_1 = column_rename_regex(df=out_7965_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[%]')
    out_7866_1 = joiner(df_left=out_7965_1, df_right=out_7861_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    out_7872_1 = joiner(df_left=out_7866_1, df_right=out_7847_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Concatenate to Pathway Explorer
    product_share_percent_demand = pd.concat([product_import_share_percent_demand, product_export_share_percent_demand.set_index(product_export_share_percent_demand.index.astype(str) + '_dup')])

    # Calibration RATES

    # Cal_rate for product demand

    # Cal rate for product-demand [unit]
    cal_rate_product_demand_unit = use_variable(input_table=out_7958_3, selected_variable='cal_rate_product-demand[unit]')

    # Exogenous product demand & production

    # Add exogenous products to product and material demand

    # product-demand [unit]
    product_demand_unit = use_variable(input_table=product_demand_unit, selected_variable='product-demand[unit]')
    # Same as last available year
    product_demand_unit = add_missing_years(df_data=product_demand_unit)
    # keep exogenous  products (other-other ;  machinery-and-equipments ; infrastructure-electric-cables) 
    product_demand_unit = product_demand_unit.loc[product_demand_unit['product'].isin(['infrastructure-electric-cables', 'machinery-and-equipments', 'other-other'])].copy()
    # exclude infrastructure -electric-cables (we apply projection on it based on assumed eolution)
    product_demand_unit_3 = product_demand_unit.loc[product_demand_unit['product'].isin(['machinery-and-equipments', 'other-other'])].copy()
    product_demand_unit_excluded = product_demand_unit.loc[~product_demand_unit['product'].isin(['machinery-and-equipments', 'other-other'])].copy()

    # HIGH-LEVEL APPROXIMATION
    # The demand for electric cables are related to the total electrification. Hence it is only available after ELC module, to avoid loop, we approximate its trend (profile) by the maximum value (among material) of the lever fuel_switch (to elec). A second profile is added on the material demand afterwards (below)

    # OTS/FTS fuel-switch[%]
    fuel_switch_percent = import_data(trigram='ind', variable_name='fuel-switch')
    # Keep only energy-carrier-to = electricity 
    fuel_switch_percent_2 = fuel_switch_percent.loc[fuel_switch_percent['energy-carrier-to'].isin(['electricity'])].copy()
    # Group by Country, year (max)
    fuel_switch_percent_2 = group_by_dimensions(df=fuel_switch_percent_2, groupby_dimensions=['Years', 'Country'], aggregation_method='Maximum')
    # fuel-switch[%] (replace) = 1 + fuel-switch[%] 
    fuel_switch_percent_2['fuel-switch[%]'] = 1.0+fuel_switch_percent_2['fuel-switch[%]']
    # product-demand[unit] = fuel-switch[%] (only elec) * product-demand[unit]
    product_demand_unit = mcd(input_table_1=product_demand_unit_excluded, input_table_2=fuel_switch_percent_2, operation_selection='x * y', output_name='product-demand[unit]')
    product_demand_unit = pd.concat([product_demand_unit_3, product_demand_unit.set_index(product_demand_unit.index.astype(str) + '_dup')])

    # Material demand evolution : this profile is applied on exogenous product to give the same trends to their projected values

    # material-demand [t]
    material_demand_t_2 = use_variable(input_table=material_demand_t, selected_variable='material-demand[t]')
    # Group by  Country, Years
    material_demand_t_3 = group_by_dimensions(df=material_demand_t_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Keep year  >= base year
    material_demand_t_2, material_demand_t_excluded = filter_dimension(df=material_demand_t_3, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # profile-evolution[%] = 1
    profile_evolution_percent = material_demand_t_excluded.assign(**{'profile-evolution[%]': 1.0})
    # profile-evolution [%]
    profile_evolution_percent = use_variable(input_table=profile_evolution_percent, selected_variable='profile-evolution[%]')
    # Keep year = base year
    material_demand_t_3, _ = filter_dimension(df=material_demand_t_3, dimension='Years', operation_selection='=', value_years=Globals.get().base_year)
    # Group by Country (sum)
    material_demand_t_3 = group_by_dimensions(df=material_demand_t_3, groupby_dimensions=['Country'], aggregation_method='Sum')
    # profile-evolution[%] = material-demand[t] / material-demand[t] base year
    profile_evolution_percent_2 = mcd(input_table_1=material_demand_t_3, input_table_2=material_demand_t_2, operation_selection='y / x', output_name='profile-evolution[%]')
    profile_evolution_percent = pd.concat([profile_evolution_percent_2, profile_evolution_percent.set_index(profile_evolution_percent.index.astype(str) + '_dup')])
    # product-demand[unit] = product-demand[unit] * profile-evolution[%]
    product_demand_unit = mcd(input_table_1=product_demand_unit, input_table_2=profile_evolution_percent, operation_selection='x * y', output_name='product-demand[unit]')
    product_demand_unit_2 = pd.concat([product_demand_unit_2, product_demand_unit.set_index(product_demand_unit.index.astype(str) + '_dup')])

    # Concatenate all product demand and production (endogenous and exogenous products)

    # product-demand [unit]
    product_demand_unit_2 = export_variable(input_table=product_demand_unit_2, selected_variable='product-demand[unit]')

    # For : Pathway Explorer
    # 
    # - Product demand after calibration

    # product-demand [unit]
    product_demand_unit_2 = use_variable(input_table=product_demand_unit_2, selected_variable='product-demand[unit]')
    # Split food demand
    product_demand_unit_excluded = product_demand_unit_2.loc[product_demand_unit_2['product'].isin(['processed-food'])].copy()
    product_demand_unit_2 = product_demand_unit_2.loc[~product_demand_unit_2['product'].isin(['processed-food'])].copy()
    # Convert Unit t to kt (for food)
    product_demand_unit_3 = product_demand_unit_excluded.drop(columns='product-demand[unit]').assign(**{'product-demand[unit]': product_demand_unit_excluded['product-demand[unit]'] * 0.001})
    # Set unit to kt
    product_demand_unit_3['unit'] = "kt"
    product_demand_unit_2 = pd.concat([product_demand_unit_2, product_demand_unit_3.set_index(product_demand_unit_3.index.astype(str) + '_dup')])
    # product-demand[unit] to product-production[unit]
    out_9799_1 = product_demand_unit.rename(columns={'product-demand[unit]': 'product-production[unit]'})
    out_9795_1 = pd.concat([product_production_unit, out_9799_1.set_index(out_9799_1.index.astype(str) + '_dup')])
    # product-production [unit]
    product_production_unit = export_variable(input_table=out_9795_1, selected_variable='product-production[unit]')

    # For : Pathway Explorer
    # 
    # - Product production

    # product-production [unit]
    product_production_unit = use_variable(input_table=product_production_unit, selected_variable='product-production[unit]')
    # Split processed-food demand
    product_production_unit_excluded = product_production_unit.loc[product_production_unit['product'].isin(['processed-food'])].copy()
    product_production_unit = product_production_unit.loc[~product_production_unit['product'].isin(['processed-food'])].copy()
    # Convert Unit t to kt (for food)
    product_production_unit_2 = product_production_unit_excluded.drop(columns='product-production[unit]').assign(**{'product-production[unit]': product_production_unit_excluded['product-production[unit]'] * 0.001})
    # Set unit to kt
    product_production_unit_2['unit'] = "kt"
    product_production_unit = pd.concat([product_production_unit, product_production_unit_2.set_index(product_production_unit_2.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    product_unit = pd.concat([product_production_unit, product_demand_unit_2.set_index(product_demand_unit_2.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    product = pd.concat([product_unit, product_share_percent_demand.set_index(product_share_percent_demand.index.astype(str) + '_dup')])
    # subproduct-demand[unit] = product-production[unit] * product-to-subproduct-demand [unit/unit]
    subproduct_demand_unit = mcd(input_table_1=out_9799_1, input_table_2=product_to_subproduct_demand_unit_per_unit, operation_selection='x * y', output_name='subproduct-demand[unit]')
    # subproduct-demand[unit] to subproduct-production[unit]
    out_9800_1 = subproduct_demand_unit.rename(columns={'subproduct-demand[unit]': 'subproduct-production[unit]'})
    # material-demand[t] = subproduct-production[unit] * subproduct-to-material-demand [t/unit]
    material_demand_t_2 = mcd(input_table_1=out_9800_1, input_table_2=subproduct_to_material_demand_t_per_unit, operation_selection='x * y', output_name='material-demand[t]')
    # material-demand [t] (only from  exogenous product)
    material_demand_t_2 = export_variable(input_table=material_demand_t_2, selected_variable='material-demand[t]')
    # Add exogenous demand
    material_demand_t = pd.concat([material_demand_t, material_demand_t_2.set_index(material_demand_t_2.index.astype(str) + '_dup')])

    # Sum material demand from endogenous and exogenous products

    # Group by  all dimensions (sum)
    material_demand_t = group_by_dimensions(df=material_demand_t, groupby_dimensions=['Country', 'Years', 'product', 'unit', 'material'], aggregation_method='Sum')
    # material-demand [t]
    material_demand_t_2 = export_variable(input_table=material_demand_t, selected_variable='material-demand[t]')

    # For : Pathways Explorer - Minerals
    # 
    # - Minerals and primary materials

    # material-demand [t] (with detailed minerals)
    material_demand_t = use_variable(input_table=material_demand_t_2, selected_variable='material-demand[t]')

    # Parameters

    # RCP material-to-primary-material[t/t]
    material_to_primary_material_t_per_t = import_data(trigram='ind', variable_name='material-to-primary-material', variable_type='RCP')
    # primary-material-demand[t] = material-to-primary-material[t/t] x material-demand[t]
    primary_material_demand_t = mcd(input_table_1=material_demand_t, input_table_2=material_to_primary_material_t_per_t, operation_selection='x * y', output_name='primary-material-demand[t]')
    # group by country, years, primary-material
    primary_material_demand_t = group_by_dimensions(df=primary_material_demand_t, groupby_dimensions=['Country', 'Years', 'primary-material'], aggregation_method='Sum')
    # group by country, years, material
    material_demand_t = group_by_dimensions(df=material_demand_t, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    material_demand_t = pd.concat([material_demand_t, primary_material_demand_t.set_index(primary_material_demand_t.index.astype(str) + '_dup')])
    # bottom: every non-ferrous
    material_demand_t_excluded = material_demand_t_2.loc[material_demand_t_2['material'].isin(['cobalt', 'copper', 'graphite', 'lithium', 'manganese', 'nickel', 'platinium', 'ree-dysprosium', 'ree-neodymium', 'ree-praseodymium', 'silicon'])].copy()
    material_demand_t_2 = material_demand_t_2.loc[~material_demand_t_2['material'].isin(['cobalt', 'copper', 'graphite', 'lithium', 'manganese', 'nickel', 'platinium', 'ree-dysprosium', 'ree-neodymium', 'ree-praseodymium', 'silicon'])].copy()

    # Groub 'Minerals' into 'non-ferrous'

    # Group by countru, years, produit, unistt, sub-product (sum)
    material_demand_t_excluded = group_by_dimensions(df=material_demand_t_excluded, groupby_dimensions=['Country', 'Years', 'product', 'unit', 'subproduct'], aggregation_method='Sum')
    # Add material = non-ferrous
    material_demand_t_excluded['material'] = "non-ferrous"
    material_demand_t_2 = pd.concat([material_demand_t_2, material_demand_t_excluded.set_index(material_demand_t_excluded.index.astype(str) + '_dup')])
    # material-demand [t]  (grouping non-ferrous)
    material_demand_t_2 = export_variable(input_table=material_demand_t_2, selected_variable='material-demand[t]')

    # Apply cal rate to material production
    # And material-demand

    # material-demand [t]
    material_demand_t_2 = use_variable(input_table=material_demand_t_2, selected_variable='material-demand[t]')

    # Apply material-import-share & material-export-share levers
    # => determine the xxx

    # OTS/FTS material-export-share [%]
    material_export_share_percent = import_data(trigram='ind', variable_name='material-export-share')

    # For : Pathway Explorer
    # 
    # - Material domestic share of production

    # material-export-share [%]
    material_export_share_percent_demand = use_variable(input_table=material_export_share_percent, selected_variable='material-export-share[%demand]')
    # material-export[t] = material-demand[t] * material-export-share[%]
    material_export_t = mcd(input_table_1=material_demand_t_2, input_table_2=material_export_share_percent, operation_selection='x * y', output_name='material-export[t]')
    # OTS/FTS material-import-share [%]
    material_import_share_percent = import_data(trigram='ind', variable_name='material-import-share')
    # material-import-share [%] = min(import-share,100%)
    material_import_share_percent_demand = math_formula(df=material_import_share_percent, convert_to_int=False, replaced_column='material-import-share[%demand]', splitted='min_in_args($material-import-share[%demand]$,1)')
    # material-import[t] = material-demand[t] * material-import-share[%]
    material_import_t = mcd(input_table_1=material_demand_t_2, input_table_2=material_import_share_percent_demand, operation_selection='x * y', output_name='material-import[t]')
    # material-net-export[t] = material-export[t] - material-import[t]
    material_net_export_t = mcd(input_table_1=material_import_t, input_table_2=material_export_t, operation_selection='y - x', output_name='material-net-export[t]')
    # material-production[t] = material-demand[t] + material-net-export[t]
    material_production_t = mcd(input_table_1=material_demand_t_2, input_table_2=material_net_export_t, operation_selection='x + y', output_name='material-production[t]')
    # Convert Unit t to kt
    material_production_kt = material_production_t.drop(columns='material-production[t]').assign(**{'material-production[kt]': material_production_t['material-production[t]'] * 0.001})

    # Calibration

    # Group by  Country, Years material (SUM)
    material_production_kt_2 = group_by_dimensions(df=material_production_kt, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Calibration material-production [kt]
    material_production_kt_3 = import_data(trigram='ind', variable_name='material-production', variable_type='Calibration')
    # Apply Calibration on material-production [kt]
    _, out_7957_2, out_7957_3 = calibration(input_table=material_production_kt_2, cal_table=material_production_kt_3, data_to_be_cal='material-production[kt]', data_cal='material-production[kt]')

    # Cal_rate for material production

    # Cal rate for material-production [kt]
    cal_rate_material_production_kt = use_variable(input_table=out_7957_3, selected_variable='cal_rate_material-production[kt]')
    cal_rate = pd.concat([cal_rate_product_demand_unit, cal_rate_material_production_kt.set_index(cal_rate_material_production_kt.index.astype(str) + '_dup')])
    # cal-rate
    cal_rate_material_production_kt = use_variable(input_table=out_7957_2, selected_variable='cal_rate_material-production[kt]')
    # material-import[t] (replace) = material-import[t] * cal_rate[%]
    material_import_t = mcd(input_table_1=material_import_t, input_table_2=cal_rate_material_production_kt, operation_selection='x * y', output_name='material-import[t]')
    # material-import [t]
    material_import_t = export_variable(input_table=material_import_t, selected_variable='material-import[t]')
    # material-import [t]
    material_import_t = use_variable(input_table=material_import_t, selected_variable='material-import[t]')
    # Concatenate
    import_ = pd.concat([material_import_t, import_unit.set_index(import_unit.index.astype(str) + '_dup')])
    # Module = Scope 2 and 3
    import_ = column_filter(df=import_, pattern='^.*$')
    # material-demand[t] (replace) = material-demand[t] * cal_rate[%]
    material_demand_t_2 = mcd(input_table_1=cal_rate_material_production_kt, input_table_2=material_demand_t_2, operation_selection='x * y', output_name='material-demand[t]')
    # Convert Unit t to kt
    material_demand_kt = material_demand_t_2.drop(columns='material-demand[t]').assign(**{'material-demand[kt]': material_demand_t_2['material-demand[t]'] * 0.001})
    # material-demand [kt]
    material_demand_kt = export_variable(input_table=material_demand_kt, selected_variable='material-demand[kt]')

    # For : Pathway Explorer
    # 
    # - Material demand

    # material-demand [kt]
    material_demand_kt = use_variable(input_table=material_demand_kt, selected_variable='material-demand[kt]')
    # Group by  product (sum)
    material_demand_kt_2 = group_by_dimensions(df=material_demand_kt, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # Rename variable to material-demand-by-product[kt]
    out_9234_1 = material_demand_kt_2.rename(columns={'material-demand[kt]': 'material-demand-by-product[kt]'})
    # Group by  material (sum)
    material_demand_kt_2 = group_by_dimensions(df=material_demand_kt, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Rename variable to material-demand-by-material[kt]
    out_9262_1 = material_demand_kt_2.rename(columns={'material-demand[kt]': 'material-demand-by-material[kt]'})
    # Concatenate to Pathway Explorer
    out_1 = pd.concat([out_9262_1, out_9234_1.set_index(out_9234_1.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_9224_1 = pd.concat([out_1, product.set_index(product.index.astype(str) + '_dup')])

    # KPI's requiring computing

    # Material Efficiency [%]
    # - include: all exept food

    # Group by country, years, (sum)
    material_demand_kt_3 = group_by_dimensions(df=material_demand_kt_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    def helper_10031(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        output_table["factor[-]"] = (1-output_table["material-efficiency[%]"])/output_table["material-efficiency[%]"]
        del output_table["material-efficiency[%]"]
        return output_table
    # factor[-] = (1-material-efficiency[%]) / material-efficiency[%]
    out_10031_1 = helper_10031(input_table=material_efficiency_percent)
    # material-saved [kt] = material-demand[kt] * factor
    material_saved_kt = mcd(input_table_1=material_demand_kt_2, input_table_2=out_10031_1, operation_selection='x * y', output_name='material-saved[kt]')
    # Group by country, years, (sum)
    material_saved_kt = group_by_dimensions(df=material_saved_kt, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # material-saved [t/t] = material-saved[kt] / material-demand[kt] 
    material_saved_t_per_t = mcd(input_table_1=material_demand_kt_3, input_table_2=material_saved_kt, operation_selection='y / x', output_name='material-saved[t/t]')
    # material-saved [t/t]
    material_saved_t_per_t = export_variable(input_table=material_saved_t_per_t, selected_variable='material-saved[t/t]')
    # Top: all exept food and other-indusitries
    material_demand_kt = material_demand_kt.loc[material_demand_kt['material'].isin(['aluminium', 'cement', 'ceramic-and-others', 'chemical-ammonia', 'chemical-olefin', 'chemical-other', 'glass', 'lime', 'non-ferrous', 'paper', 'steel', 'wood'])].copy()

    # Material footprint per capita [t/cap]
    # - include: all except food and other-industries

    # Group by country, years, (sum)
    material_demand_kt = group_by_dimensions(df=material_demand_kt, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # OTS/FTS population [cap]
    population_cap = import_data(trigram='lfs', variable_name='population')
    # material-foot-print-demand [kt/cap] = material-demand[kt] / population[cap]
    material_foot_print_demand_kt_per_cap = mcd(input_table_1=material_demand_kt, input_table_2=population_cap, operation_selection='x / y', output_name='material-foot-print-demand[kt/cap]')
    # Convert Unit kt/cap to t/cap
    material_foot_print_demand_t_per_cap = material_foot_print_demand_kt_per_cap.drop(columns='material-foot-print-demand[kt/cap]').assign(**{'material-foot-print-demand[t/cap]': material_foot_print_demand_kt_per_cap['material-foot-print-demand[kt/cap]'] * 1000.0})
    # material-foot-print-demand [t/cap]
    material_foot_print_demand_t_per_cap = export_variable(input_table=material_foot_print_demand_t_per_cap, selected_variable='material-foot-print-demand[t/cap]')
    # material-production [kt]
    material_production_kt = use_variable(input_table=material_production_kt, selected_variable='material-production[kt]')
    # material-production[kt] (replace) = material-production[kt] * cal_rate[%]
    material_production_kt = mcd(input_table_1=material_production_kt, input_table_2=cal_rate_material_production_kt, operation_selection='x * y', output_name='material-production[kt]')

    # Apply link-material-to-activty
    # => Switch between endogenous and exogenous data for material demand

    # material-production[kt]
    material_production_kt = use_variable(input_table=material_production_kt, selected_variable='material-production[kt]')
    # Group by  Country, Years, material (SUM)
    material_production_kt = group_by_dimensions(df=material_production_kt, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # OTS/FTS link-material-to-activity [-]
    link_material_to_activity_ = import_data(trigram='ind', variable_name='link-material-to-activity')
    # Group by  Country, material (MIN)
    link_material_to_activity_ = group_by_dimensions(df=link_material_to_activity_, groupby_dimensions=['Country', 'material'], aggregation_method='Minimum')
    # material-production[kt] (replace) = material-production[kt] * link-material-to-activity[-]
    material_production_kt = mcd(input_table_1=material_production_kt, input_table_2=link_material_to_activity_, operation_selection='x * y', output_name='material-production[kt]')
    # OTS/FTS material-production [kt]
    material_production_kt_2 = import_data(trigram='ind', variable_name='material-production')
    # material-production[kt] (replace) = material-production[kt] * (1 - link-material-to-activity[-])
    material_production_kt_2 = mcd(input_table_1=link_material_to_activity_, input_table_2=material_production_kt_2, operation_selection='(1-x) * y', output_name='material-production[kt]')
    # material-production[kt] (replace) = material-production[kt] + material-production[kt]
    material_production_kt = mcd(input_table_1=material_production_kt, input_table_2=material_production_kt_2, operation_selection='x + y', output_name='material-production[kt]')

    # Apply recycling techs share (shift)
    # => determine the share of virgin (primary route) and recyclied (secondary route) material used for each production of material

    # OTS/FTS recycling-techs-share [%]
    recycling_techs_share_percent = import_data(trigram='ind', variable_name='recycling-techs-share')
    # material-production[kt] (replace) = material-production[kt] * recycling-techs-share[%]
    material_production_kt = mcd(input_table_1=material_production_kt, input_table_2=recycling_techs_share_percent, operation_selection='x * y', output_name='material-production[kt]')

    # Apply technology-switch (shift)
    # => determine the technology used for each production of material and route

    # material-production[kt]
    material_production_kt = use_variable(input_table=material_production_kt, selected_variable='material-production[kt]')
    # Convert Unit kt to Mt
    material_production_Mt = material_production_kt.drop(columns='material-production[kt]').assign(**{'material-production[Mt]': material_production_kt['material-production[kt]'] * 0.001})
    # OTS/FTS technology-switch [%]
    technology_switch_percent = import_data(trigram='ind', variable_name='technology-switch')
    # material-production[Mt] (replace) = material-production[Mt] * technology-share[%]  LEFT Join If missing : set to 1
    material_production_Mt = mcd(input_table_1=material_production_Mt, input_table_2=technology_switch_percent, operation_selection='x * y', output_name='material-production[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    # Set % of ETS and non-ETS
    # => determine the part of production linked to ETS and non-ETS

    # RCP industry-ets-share [%] (from technology)
    industry_ets_share_percent = import_data(trigram='tec', variable_name='industry-ets-share', variable_type='RCP')
    # Convert Unit % to - (* 0.01)
    industry_ets_share_ = industry_ets_share_percent.drop(columns='industry-ets-share[%]').assign(**{'industry-ets-share[-]': industry_ets_share_percent['industry-ets-share[%]'] * 0.01})
    # material-production[Mt] (replace) = material-production[Mt] * industry-ets-share[-]  LEFT Join If missing : set to 1
    material_production_Mt = mcd(input_table_1=material_production_Mt, input_table_2=industry_ets_share_, operation_selection='x * y', output_name='material-production[Mt]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)

    def helper_9203(input_table) -> pd.DataFrame:
        # Copy input to output
        output_table = input_table.copy()
        
        # Missing value => set non-ETS
        mask = (output_table['ets-or-not'].isna())
        output_table.loc[mask, 'ets-or-not'] = "non-ETS"
        return output_table
    # If missing value for ets-or-not set non-ETS (default value)
    out_9203_1 = helper_9203(input_table=material_production_Mt)
    # material-production [Mt]
    material_production_Mt = export_variable(input_table=out_9203_1, selected_variable='material-production[Mt]')

    # Share of alternative plastics [%]
    # - Sum chemical plastics produced via: secondary route, primary route e-MTO and e-dehydratation, and primary route Tech taking into consideration fuel switch

    # material-production [Mt]
    material_production_Mt = use_variable(input_table=material_production_Mt, selected_variable='material-production[Mt]')
    # Top: only chemichal olefin
    material_production_Mt_3 = material_production_Mt.loc[material_production_Mt['material'].isin(['chemical-olefin'])].copy()
    # Group by country, years, (sum)
    material_production_Mt_2 = group_by_dimensions(df=material_production_Mt_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Top: only secondary
    material_production_Mt_4 = material_production_Mt_3.loc[material_production_Mt_3['route'].isin(['secondary'])].copy()
    material_production_Mt_excluded = material_production_Mt_3.loc[~material_production_Mt_3['route'].isin(['secondary'])].copy()
    # Top: only e-MTO and e-dehydratation
    material_production_Mt_excluded_2 = material_production_Mt_excluded.loc[material_production_Mt_excluded['technology'].isin(['e-MTO', 'e-dehydration'])].copy()
    material_production_Mt_excluded_excluded = material_production_Mt_excluded.loc[~material_production_Mt_excluded['technology'].isin(['e-MTO', 'e-dehydration'])].copy()
    material_production_Mt_5 = pd.concat([material_production_Mt_4, material_production_Mt_excluded_2.set_index(material_production_Mt_excluded_2.index.astype(str) + '_dup')])
    # Top: all exept food and other-indusitries
    material_production_Mt_3 = material_production_Mt.loc[material_production_Mt['material'].isin(['aluminium', 'cement', 'ceramic-and-others', 'chemical-ammonia', 'chemical-olefin', 'chemical-other', 'glass', 'lime', 'non-ferrous', 'paper', 'steel', 'wood'])].copy()
    # Group by country, years, (sum)
    material_production_Mt_3 = group_by_dimensions(df=material_production_Mt_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # material-foot-print-productio [Mt/cap] = material-production[Mt] / population[cap]
    material_foot_print_production_Mt_per_cap = mcd(input_table_1=material_production_Mt_3, input_table_2=population_cap, operation_selection='x / y', output_name='material-foot-print-production[Mt/cap]')
    # Convert Unit kt/cap to t/cap
    material_foot_print_production_t_per_cap = material_foot_print_production_Mt_per_cap.drop(columns='material-foot-print-production[Mt/cap]').assign(**{'material-foot-print-production[t/cap]': material_foot_print_production_Mt_per_cap['material-foot-print-production[Mt/cap]'] * 1000000.0})
    # material-foot-print-production [t/cap]
    material_foot_print_production_t_per_cap = export_variable(input_table=material_foot_print_production_t_per_cap, selected_variable='material-foot-print-production[t/cap]')
    # KPI's
    material_foot_print_t_per_cap = pd.concat([material_foot_print_demand_t_per_cap, material_foot_print_production_t_per_cap.set_index(material_foot_print_production_t_per_cap.index.astype(str) + '_dup')])

    # For : Air quality
    # 
    # - Energy-demand by energy-carrier and material

    # Group by  gaes, energy-carrier-category (sum)
    material_production_Mt_3 = group_by_dimensions(df=material_production_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'energy-carrier', 'feedstock-type'], aggregation_method='Sum')

    # For : Bio-Energy
    # 
    # - Material production

    # Group by  Country, Years, material (sum)
    material_production_Mt_4 = group_by_dimensions(df=material_production_Mt, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Filter material wood and paper
    material_production_Mt_4 = material_production_Mt_4.loc[material_production_Mt_4['material'].isin(['paper', 'wood'])].copy()
    # Module = Bioenergy
    material_production_Mt_7 = column_filter(df=material_production_Mt_4, pattern='^.*$')

    # For : Water
    # 
    # - Material production

    # Group by  Country, Years, material (sum)
    material_production_Mt_9 = group_by_dimensions(df=material_production_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route'], aggregation_method='Sum')
    # Module = Water
    material_production_Mt_6 = column_filter(df=material_production_Mt_9, pattern='^.*$')
    # Convert Unit Mt to kt
    material_production_kt = material_production_Mt.drop(columns='material-production[Mt]').assign(**{'material-production[kt]': material_production_Mt['material-production[Mt]'] * 1000.0})

    # For : Pathway Explorer
    # 
    # - Material production

    # Group by  material (sum)
    material_production_kt_2 = group_by_dimensions(df=material_production_kt, groupby_dimensions=['Country', 'Years', 'material', 'unit'], aggregation_method='Sum')
    # Rename variable to material-production-by-material[kt]
    out_9263_1 = material_production_kt_2.rename(columns={'material-production[kt]': 'material-production-by-material[kt]'})
    # Group by  material, route, technology (sum)
    material_production_kt_2 = group_by_dimensions(df=material_production_kt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # Rename variable to material-production-by-route-tech[kt]
    out_9232_1 = material_production_kt_2.rename(columns={'material-production[kt]': 'material-production-by-route-tech[kt]'})
    # Concatenate to Pathway Explorer
    out_1 = pd.concat([out_9263_1, out_9232_1.set_index(out_9232_1.index.astype(str) + '_dup')])
    # Group by  material, ets (sum)
    material_production_kt_2 = group_by_dimensions(df=material_production_kt, groupby_dimensions=['Country', 'Years', 'material', 'ets-or-not'], aggregation_method='Sum')
    # Rename variable to material-production-by-ets[kt]
    out_9233_1 = material_production_kt_2.rename(columns={'material-production[kt]': 'material-production-by-tech-ets[kt]'})
    # Concatenate to Pathway Explorer
    out_1 = pd.concat([out_1, out_9233_1.set_index(out_9233_1.index.astype(str) + '_dup')])

    # Pivot

    # by material, technology
    out_7737_1, _, _ = pivoting(df=material_production_kt, agg_dict={'material-production[kt]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['material', 'technology'])
    out_7732_1 = missing_value_column_filter(df=out_7737_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7732_1 = column_rename_regex(df=out_7732_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[kt]')

    def helper_7735(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_7735_1 = helper_7735(input_table_1=out_7739_1, input_table_2=out_7732_1)
    # per material and tech : keep : - wood
    out_7735_1 = column_filter(df=out_7735_1, pattern='^Country$|^Years$|^.*wood.*$')

    # Pivot

    # by material
    out_7740_1, _, _ = pivoting(df=material_production_kt, agg_dict={'material-production[kt]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['material'])
    out_7738_1 = missing_value_column_filter(df=out_7740_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7738_1 = column_rename_regex(df=out_7738_1, search_string='(.*)\\+(.*)\\[.*', replace_string='$2_$1[kt]')

    def helper_7736(input_table_1, input_table_2) -> pd.DataFrame:
        # Input tables
        corr_table = input_table_1.copy()
        output_table = input_table_2.copy()
        
        # Create dict with old_new_name
        dict_names = {}
        for row in corr_table.iterrows():
            old_name = row[1]["old_name"]
            new_name = row[1]["new_name"]
            dict_names[new_name] = old_name
        
        # For col in columns > if col correspond to new name => get in dict for rename
        dict_rename = {}
        for col in output_table.head():
            for i in dict_names.keys():
                if i == col:
                    dict_rename[i] = dict_names[i]
        
        output_table = output_table.rename(columns = dict_rename)
        return output_table
    # rename based on table creator
    out_7736_1 = helper_7736(input_table_1=out_7741_1, input_table_2=out_7738_1)
    # per material : keep : - cement - glass
    out_7736_1 = column_filter(df=out_7736_1, pattern='^Country$|^Years$|^.*cement.*$|^.*glass.*$')
    out_7806_1 = joiner(df_left=out_7736_1, df_right=out_7735_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    out_7807_1 = joiner(df_left=out_7806_1, df_right=out_7872_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])

    # Emission per tonne of material for each route [tCO2e/t]

    # Group by  Country, Years material, technology, route (SUM)
    material_production_Mt_4 = group_by_dimensions(df=material_production_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # Convert Unit Mt to kt
    material_production_kt = material_production_Mt_4.drop(columns='material-production[Mt]').assign(**{'material-production[kt]': material_production_Mt_4['material-production[Mt]'] * 1000.0})

    # Costs

    # RCP costs-for-industry-material [MEUR/kt] from TECH
    costs_for_industry_material_MEUR_per_kt = import_data(trigram='tec', variable_name='costs-for-industry-material', variable_type='RCP')
    # RCP price-indices [-] from TECH
    price_indices_ = import_data(trigram='tec', variable_name='price-indices', variable_type='RCP')

    # Secondary Production Share [%]

    # Group by material, material-production (sum)
    material_production_Mt_8 = group_by_dimensions(df=material_production_Mt_9, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Group by route (sum)
    material_production_Mt_10 = group_by_dimensions(df=material_production_Mt_8, groupby_dimensions=['Country', 'Years', 'route'], aggregation_method='Sum')
    # Top: secoundary route
    material_production_Mt_9 = material_production_Mt_9.loc[material_production_Mt_9['route'].isin(['secondary'])].copy()
    # Group by route (sum)
    material_production_Mt_11 = group_by_dimensions(df=material_production_Mt_9, groupby_dimensions=['Country', 'Years', 'route'], aggregation_method='Sum')
    # share-route-material-production[%] = material-production[Mt] (only 2nd) / material-production[Mt] (total)
    share_route_material_production_percent = mcd(input_table_1=material_production_Mt_11, input_table_2=material_production_Mt_10, operation_selection='x / y', output_name='share-route-material-production[%]')
    # share-route-material-production[%] = material-production[Mt] (only 2nd) / material-production[Mt] (total)
    share_route_material_production_percent_2 = mcd(input_table_1=material_production_Mt_9, input_table_2=material_production_Mt_8, operation_selection='x / y', output_name='share-route-material-production[%]')
    # KPI's
    share_route_material_production_percent = pd.concat([share_route_material_production_percent_2, share_route_material_production_percent.set_index(share_route_material_production_percent.index.astype(str) + '_dup')])
    # share-route-material-production[%]
    share_route_material_production_percent = export_variable(input_table=share_route_material_production_percent, selected_variable='share-route-material-production[%]')
    # Split logic for cement
    material_production_Mt_excluded = material_production_Mt.loc[material_production_Mt['material'].isin(['cement'])].copy()
    material_production_Mt_8 = material_production_Mt.loc[~material_production_Mt['material'].isin(['cement'])].copy()

    # Energy demand

    # Convert material to energy demand and CCU
    # 
    # Energy demand is the energy needed to produce the material production.
    # 
    # CO2 demand is modeled by the two variables. Energy demand will include efuels (requiring co2 to be produced).
    # CCU is a direct need of CO2.
    # Embedded CO2 in feedstock (see later) is efuels used as feedstock (after fuel switch).

    # CCU
    # Compute the CO2 embedded in feedstock directly in industry. Second part of CO2 embedded in feedstock will be in power through the fuel switch in industry (feedstock switch to efuels).
    # 
    # Cement has here a particular modeling logic because a technology-switch is applied only for cement. This is co2-curing allowing to add in cement CO2 as feedstock.

    # Parameters
    # CO2-specific : Expressed in Mt/Mt for CO2
    # CO2-curing-for-cement : Tailor made lever for cement to take co2-curing into account (independent of the technology)

    # RCP co2-specific [Mt/Mt]
    co2_specific_Mt_per_Mt = import_data(trigram='ind', variable_name='co2-specific', variable_type='RCP')
    # ccu[Mt] = material-production[Mt] * co2-specific[Mt/Mt]
    CCU_Mt_2 = mcd(input_table_1=material_production_Mt_8, input_table_2=co2_specific_Mt_per_Mt, operation_selection='x * y', output_name='CCU[Mt]')
    # OTS/FTS co2-curing-for-cement [%]
    co2_curing_for_cement_percent = import_data(trigram='ind', variable_name='co2-curing-for-cement')

    # Apply technology-switch (shift)
    # (only for cement)
    # => determine the share of co2 curing done for cement

    # co2-specific[Mt/Mt] (replace) = co2-specific[Mt/Mt] * co2-curing-for-cement[%]  (only cement)
    co2_specific_Mt_per_Mt = mcd(input_table_1=co2_specific_Mt_per_Mt, input_table_2=co2_curing_for_cement_percent, operation_selection='x * y', output_name='co2-specific[Mt/Mt]')
    # ccu[Mt] = material-production[Mt] * co2-specific[Mt/Mt]  (only cement)
    CCU_Mt = mcd(input_table_1=material_production_Mt_excluded, input_table_2=co2_specific_Mt_per_Mt, operation_selection='x * y', output_name='CCU[Mt]')
    CCU_Mt = pd.concat([CCU_Mt_2, CCU_Mt.set_index(CCU_Mt.index.astype(str) + '_dup')])
    # CCU[Mt]
    CCU_Mt = export_variable(input_table=CCU_Mt, selected_variable='CCU[Mt]')

    # For : Power supply
    # 
    # - Energy-demand by energy-carrier (contains co2 demand through efuels due to fuel switch)
    # - CC (co2 production by industry)
    # - CCU (co2 demand for alternative feedstock)

    # CCU[Mt] from material
    CCU_Mt_2 = use_variable(input_table=CCU_Mt, selected_variable='CCU[Mt]')
    # Group by  material, technology (sum)
    CCU_Mt = group_by_dimensions(df=CCU_Mt_2, groupby_dimensions=['Country', 'Years', 'material', 'technology'], aggregation_method='Sum')
    # Add dimension  gaes =  CO2
    CCU_Mt_3 = CCU_Mt_2.assign(**{'gaes': "CO2"})

    # Apply energy-efficiency (improve)
    # => energy-efficiency : determine the efficiency of each technology and then the reduction in energy consumption we can expect

    # material-production [Mt]
    material_production_Mt_9 = use_variable(input_table=material_production_Mt, selected_variable='material-production[Mt]')
    # Keep Years >= baseyear
    material_production_Mt_8, _ = filter_dimension(df=material_production_Mt_9, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # Keep Years = baseyear
    material_production_Mt_9, _ = filter_dimension(df=material_production_Mt_9, dimension='Years', operation_selection='=', value_years=Globals.get().base_year)
    # Remove Years
    material_production_Mt_9 = column_filter(df=material_production_Mt_9, columns_to_drop=['Years'])
    # normalised-material-production[-] = material-production[Mt] / material-production[Mt] for baseyear
    normalized_material_production = mcd(input_table_1=material_production_Mt_9, input_table_2=material_production_Mt_8, operation_selection='y / x', output_name='normalized-material-production[-]')
    # set to 0
    normalized_material_production = missing_value(df=normalized_material_production, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Determine energy demand for CC
    # Energy demand is attributed to excl-feedstock

    # Apply energy-efficiency (improve)
    # => energy-efficiency : determine the efficiency of each technology and then the reduction in energy consumption we can expect

    # normalised-material- production[-]
    normalized_material_production_2 = use_variable(input_table=normalized_material_production, selected_variable='normalized-material-production[-]')

    # Energy demand

    # Parameter
    # Expressed in TWh/Mt for all carriers

    # RCP specific-energy-consumption [TWh/Mt]
    specific_energy_consumption_TWh_per_Mt = import_data(trigram='ind', variable_name='specific-energy-consumption', variable_type='RCP')
    # energy-demand[TWh] = material-production[Mt] * specific-energy-consumption [TWh/Mt]
    energy_demand_TWh = mcd(input_table_1=material_production_Mt, input_table_2=specific_energy_consumption_TWh_per_Mt, operation_selection='x * y', output_name='energy-demand[TWh]')

    # Calibration
    # Calibration is executed in two stages (on group and then on total). This is done for a difference in granularity of calibration data from countries (EU countries are more detailled).
    # 
    # 1. Calibration on groups
    # Calibration groups are defined in CP in the google sheets, this CP is used to group the mat-route-tech by calibration-groups. While calibration values are already given by calibration-groups in GS.
    # 
    # 2. Calibration on total (only excl-feedstock, by energy-carrier)
    # Total energy is calibrated on total to insure their consistencies if data for calibration by group is not available.

    # Calibration

    # CP ind_calibration-groups
    ind_calibration_groups = import_data(trigram='ind', variable_name='ind_calibration-groups', variable_type='CP')
    # rename to feedstock-type
    out_9938_1 = ind_calibration_groups.rename(columns={'Eurostat-energy-calibration-group': 'excl-feedstock', 'Eurostat-non-energy-calibration-group': 'feedstock'})
    # Keep energy & non-energy -calibration-group
    out_9937_1 = unpivoting(df=out_9938_1, filter_type='STANDARD', id_values=['excl-feedstock', 'feedstock'], id_variables=['material', 'route', 'technology', 'CRF-process-calibration-group', 'CRF-combustion-calibration-group', 'UN-energy-calibration-group', 'data_type', 'module'], not_to_keep=['excl-feedstock', 'feedstock', 'CRF-process-calibration-group', 'CRF-combustion-calibration-group', 'UN-energy-calibration-group', 'data_type', 'module'], retained_type='STANDARD', to_keep=['material', 'route', 'technology'])
    # rename to group
    out_9939_1 = out_9937_1.rename(columns={'ColumnNames': 'feedstock-type', 'ColumnValues': 'group'})
    # Calibration energy-demand-by-group [TWh]
    energy_demand_by_group_TWh = import_data(trigram='ind', variable_name='energy-demand-by-group', variable_type='Calibration')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Add calibration-type = energy-group
    energy_demand_TWh['calibration-type'] = "energy-group"
    # LEFT OUTER
    out_9900_1 = joiner(df_left=energy_demand_TWh, df_right=out_9939_1, joiner='left', left_input=['material', 'route', 'technology', 'feedstock-type'], right_input=['material', 'route', 'technology', 'feedstock-type'])
    # Group by Country, Years, group, feedstock-type, energy-carrier, calibration-type (sum)
    out_9900_1_2 = group_by_dimensions(df=out_9900_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'feedstock-type', 'calibration-type', 'group'], aggregation_method='Sum')
    # Apply Calibration on energy-demand by calibration group
    _, out_9890_2, out_9890_3 = calibration(input_table=out_9900_1_2, cal_table=energy_demand_by_group_TWh, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand-by-group[TWh]')

    # Cal_rate for energy demand

    # Cal rate for energy-demand by group [TWh]
    cal_rate_energy_demand_TWh_2 = use_variable(input_table=out_9890_3, selected_variable='cal_rate_energy-demand[TWh]')
    # Apply cal-rate on energy-demand
    energy_demand_TWh = mcd(input_table_1=out_9900_1, input_table_2=out_9890_2, operation_selection='x * y', output_name='energy-demand[TWh]')

    # Calibration on totals

    # energy-demand [TWh] calibrated  by group
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # top : feedstock bottom : excl-feedstock
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['feedstock-type'].isin(['feedstock'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh.loc[~energy_demand_TWh['feedstock-type'].isin(['feedstock'])].copy()
    # Change calibration-type = energy-total
    energy_demand_TWh_excluded['calibration-type'] = "energy-total"
    # Group by Country, Years, group, feedstock-type, energy-carrier, calibration-type (sum)
    energy_demand_TWh_excluded_2 = group_by_dimensions(df=energy_demand_TWh_excluded, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'feedstock-type', 'calibration-type'], aggregation_method='Sum')
    # Calibration energy-demand-total [TWh]
    energy_demand_total_TWh = import_data(trigram='ind', variable_name='energy-demand-total', variable_type='Calibration')
    # Apply Calibration on energy-demand
    _, out_9960_2, out_9960_3 = calibration(input_table=energy_demand_TWh_excluded_2, cal_table=energy_demand_total_TWh, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand-total[TWh]')
    # Cal rate for energy-demand total [TWh]
    cal_rate_energy_demand_TWh = use_variable(input_table=out_9960_3, selected_variable='cal_rate_energy-demand[TWh]')
    cal_rate_energy_demand_TWh = pd.concat([cal_rate_energy_demand_TWh_2, cal_rate_energy_demand_TWh.set_index(cal_rate_energy_demand_TWh.index.astype(str) + '_dup')])
    cal_rate = pd.concat([cal_rate, cal_rate_energy_demand_TWh.set_index(cal_rate_energy_demand_TWh.index.astype(str) + '_dup')])
    # energy-demand[TWh] (replace) = energy-demand[TWh] * cal_rate[%]
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=out_9960_2, operation_selection='x * y', output_name='energy-demand[TWh]')
    energy_demand_TWh = pd.concat([energy_demand_TWh_2, energy_demand_TWh.set_index(energy_demand_TWh.index.astype(str) + '_dup')])
    # Remove calibration-type, group
    energy_demand_TWh = column_filter(df=energy_demand_TWh, columns_to_drop=['calibration-type', 'RowIDs', 'group'])
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # OTS/FTS energy-efficiency[%]
    energy_efficiency_percent = import_data(trigram='ind', variable_name='energy-efficiency')

    # For : Minerals
    # 
    # - Energy efficiency

    # energy-efficiency[%]
    energy_efficiency_percent_2 = use_variable(input_table=energy_efficiency_percent, selected_variable='energy-efficiency[%]')
    # Keep  material = aluminium, steel
    energy_efficiency_percent_2 = energy_efficiency_percent_2.loc[energy_efficiency_percent_2['material'].isin(['aluminium', 'steel'])].copy()

    # Pivot

    # by material, technology
    out_7868_1, _, _ = pivoting(df=energy_efficiency_percent_2, agg_dict={'energy-efficiency[%]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['material', 'technology'])
    out_7869_1 = missing_value_column_filter(df=out_7868_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Pathway Explorer
    out_7869_1 = column_rename_regex(df=out_7869_1, search_string='(.*)\\+(.*)\\[.*', replace_string='ind_$2_$1[%]')
    out_7871_1 = joiner(df_left=out_7869_1, df_right=out_7807_1, joiner='inner', left_input=['Country', 'Years'], right_input=['Country', 'Years'])
    # Module = Minerals
    out_7871_1 = column_filter(df=out_7871_1, pattern='^.*$')
    # Years
    out_7801_1 = out_7871_1.assign(Years=out_7871_1['Years'].astype(str))
    # energy-saving[TWh] = energy-demand[TWh] * energy-efficiency[%]
    energy_saving_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_efficiency_percent, operation_selection='x * y', output_name='energy-saving[TWh]')
    # EE-saving[TWh] = energy-saving[TWh] / normalized-material-production[-]  LEFT OUTER Join If missing : set 0 (for Years < baseyear)
    EE_saving_TWh = mcd(input_table_1=energy_saving_TWh, input_table_2=normalized_material_production, operation_selection='x / y', output_name='EE-saving[TWh]', fill_value_bool='Left [x] Outer Join')
    # Set to 0
    EE_saving_TWh = missing_value(df=EE_saving_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # EE-saving [TWh] from energy-demand
    EE_saving_TWh = use_variable(input_table=EE_saving_TWh, selected_variable='EE-saving[TWh]')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * (1 - energy-efficiency[%])  LEFT JOIN il missing : set to 0
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=energy_efficiency_percent, operation_selection='x * (1-y)', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join')

    # Apply fuel-switch (switch)
    # => Switch fuel for feedstock and excl-feedstock separatly
    # 
    # Feedstock :
    # 1	all	elec
    # 2	liquid-ff	gas-ff
    # 3	solid-ff	gas-ff
    # 4	ff	H2
    # 5	ff	bio
    # 6	ff	syn
    # 
    # 
    # Excl-Feedstock:
    # 1	ff	bio
    # 2	ff	syn

    # energy-demand[TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # CP fuel-switch params
    ind_fuel_switch_params = import_data(trigram='ind', variable_name='ind_fuel-switch-params', variable_type='CP')
    # Remove data_type and module
    ind_fuel_switch_params = column_filter(df=ind_fuel_switch_params, columns_to_drop=['data_type', 'module'])
    # ratio[-] = 1 (energy final, no need to  account for energy efficiency differences) 
    ratio = ind_fuel_switch_params.assign(**{'ratio[-]': 1.0})

    # QUICK FIX
    # => to remove when review of calibration

    # Fill missing with ""
    energy_demand_TWh = missing_value(df=energy_demand_TWh, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # Split on feedstock type
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh.loc[~energy_demand_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()

    # Switch applied only on excl-feedstock

    # Fuel Switch all to elec
    out_8083_1 = x_switch(demand_table=energy_demand_TWh_2, switch_table=fuel_switch_percent, correlation_table=ratio, category_from_selected='all', category_to_selected='electricity')
    # Fuel Switch liquid ff to gas ff
    out_8085_1 = x_switch(demand_table=out_8083_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_from_selected='liquid', category_to_selected='gaseous')
    # Fuel Switch solid ff to gas ff
    out_8086_1 = x_switch(demand_table=out_8085_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_from_selected='solid', category_to_selected='gaseous')
    # Fuel Switch ff (all) to H2
    out_8084_1 = x_switch(demand_table=out_8086_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='hydrogen')
    # Fuel Switch ff to bio (same phase)
    out_8087_1 = x_switch(demand_table=out_8084_1, switch_table=fuel_switch_percent, correlation_table=ratio)
    # Fuel Switch ff to syn (same phase)
    out_8088_1 = x_switch(demand_table=out_8087_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='synfuels')

    # Switch applied only on feedstock
    # Switch from CH4 to H2 for ammonia production is +/- consistent as 1t CH4 (14MWh) can produce 0.5t H2 (16.5MWh), hence the similar energy content. Thus a fuel switch (1 for 1) it +/- consistent. This switch is used to model the switch from steam-reforming H2 (here directly corresponding to its CH4 content) to green H2 (here corresponding to H2, which will be produce via electrolysis in ELC)

    # Fuel Switch CH4 to H2 Only for ammonia
    out_9462_1 = x_switch(demand_table=energy_demand_TWh_excluded, switch_table=fuel_switch_percent, correlation_table=ratio, category_from_selected='gaseous', category_to_selected='hydrogen')
    # Fuel Switch ff to bio (same phase)
    out_9802_1 = x_switch(demand_table=out_9462_1, switch_table=fuel_switch_percent, correlation_table=ratio)
    # Fuel Switch ff to syn (same phase)
    out_9457_1 = x_switch(demand_table=out_9802_1, switch_table=fuel_switch_percent, correlation_table=ratio, category_to_selected='synfuels')
    # fuels demand as feedstock and excl-feedstock (send to Power) without CC demand We add CC demand further in the model !
    out_1_2 = pd.concat([out_8088_1, out_9457_1.set_index(out_9457_1.index.astype(str) + '_dup')])
    # energy-demand [TWh] without CC demand
    energy_demand_TWh = export_variable(input_table=out_1_2, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Top: only chemichal olefin
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['material'].isin(['chemical-olefin'])].copy()
    # Top: only feedstock
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[energy_demand_TWh_2['feedstock-type'].isin(['feedstock'])].copy()
    # Top: only Tech
    energy_demand_TWh_3 = energy_demand_TWh_2.loc[energy_demand_TWh_2['technology'].isin(['tech'])].copy()
    # Group by country, years, (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Top: only syn/bio gas/liquid and hydrogen
    energy_demand_TWh_3 = energy_demand_TWh_3.loc[energy_demand_TWh_3['energy-carrier'].isin(['gaseous-bio', 'liquid-bio', 'hydrogen', 'gaseous-syn', 'liquid-syn'])].copy()
    # Group by country, years, (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # share-alt-fuel-feedstock[TWh/TWh] = energy-demand[TWh] (only alt fuel) / energy-demand[TWh]
    share_alt_fuel_feedstock_TWh_per_TWh = mcd(input_table_1=energy_demand_TWh_3, input_table_2=energy_demand_TWh_2, operation_selection='x / y', output_name='share-alt-fuel-feedstock[TWh/TWh]')
    # material-production[Mt] (only alternative fuel) = share-alt-fuel-feedstock[TWh/TWh] * material-production[Mt]
    material_production_Mt_8 = mcd(input_table_1=material_production_Mt_excluded_excluded, input_table_2=share_alt_fuel_feedstock_TWh_per_TWh, operation_selection='x * y', output_name='material-production[Mt]')
    material_production_Mt_5 = pd.concat([material_production_Mt_5, material_production_Mt_8.set_index(material_production_Mt_8.index.astype(str) + '_dup')])
    # Group by country, years, (sum)
    material_production_Mt_5 = group_by_dimensions(df=material_production_Mt_5, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # share-alt-plastics[t/t] = material-production[Mt] (alternative plastics only) / material-production[Mt]
    share_alt_plastics_t_per_t = mcd(input_table_1=material_production_Mt_5, input_table_2=material_production_Mt_2, operation_selection='x / y', output_name='share-alt-plastics[t/t]')
    # share-alt-plastics[t/t]
    share_alt_plastics_t_per_t = export_variable(input_table=share_alt_plastics_t_per_t, selected_variable='share-alt-plastics[t/t]')
    # Keep chemical-ammonia
    energy_demand_TWh_2 = energy_demand_TWh.loc[energy_demand_TWh['material'].isin(['chemical-ammonia'])].copy()
    # Keep feedstock
    energy_demand_TWh_3 = energy_demand_TWh_2.loc[energy_demand_TWh_2['feedstock-type'].isin(['feedstock'])].copy()

    # GHG emissions

    # PROCESS :
    # Emissions and carbon-capture

    # Apply Green Ammonia switch
    # => reduce process emission from Ammonia if switched to green ammonia.
    # It removes the share of ammonia which is made from hydrogen (green hydrogen) instead of CH4 (hydrogen from CH4 reforming)
    # to the material-production of ammonia. Thus the "green ammonia" is removed from the material-production to which is applied 
    # process emissions.

    # Group by country,years, material,route technology, ets-or-not (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'ets-or-not'], aggregation_method='Sum')
    # Keep hydrogen
    energy_demand_TWh_3 = energy_demand_TWh_3.loc[energy_demand_TWh_3['energy-carrier'].isin(['hydrogen'])].copy()
    # Group by country,years, material,route technology, ets-or-not (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'ets-or-not'], aggregation_method='Sum')
    # green-ammonia-share[%] = energy-carrier[TWh] (hydrogen) / energy-carrier[TWh]
    green_ammonia_share_percent = mcd(input_table_1=energy_demand_TWh_3, input_table_2=energy_demand_TWh_2, operation_selection='x / y', output_name='green-ammonia-share[%]')
    # material-production[Mt] (replace) = material-production[Mt] * (1 - green-ammonia-share[%])  LEFT JOIN il missing : set to 0
    material_production_Mt_2 = mcd(input_table_1=material_production_Mt, input_table_2=green_ammonia_share_percent, operation_selection='x * (1-y)', output_name='material-production[Mt]', fill_value_bool='Left [x] Outer Join')
    # RCP ind-process-emission-factor [Mt/Mt]
    ind_process_emission_factor_Mt_per_Mt = import_data(trigram='ind', variable_name='ind-process-emission-factor', variable_type='RCP')
    # emissions[Mt] = material-production[Mt] * ind-process-emission-factor [Mt/Mt]
    emissions_Mt = mcd(input_table_1=material_production_Mt_2, input_table_2=ind_process_emission_factor_Mt_per_Mt, operation_selection='x * y', output_name='emissions[Mt]')
    # append column energy-carrier-category with process
    emissions_Mt['energy-carrier-category'] = "process"
    # Exclude -bio / -syn fuels
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-bio', 'liquid-bio', 'solid-biomass', 'co2', 'gaseous-syn', 'liquid-syn'])].copy()
    energy_demand_TWh_2 = energy_demand_TWh.loc[~energy_demand_TWh['energy-carrier'].isin(['gaseous-bio', 'liquid-bio', 'solid-biomass', 'co2', 'gaseous-syn', 'liquid-syn'])].copy()
    # Top : Biofuels Bottom : Synfuels
    energy_demand_TWh_excluded_excluded = energy_demand_TWh_excluded.loc[energy_demand_TWh_excluded['energy-carrier'].isin(['gaseous-syn', 'liquid-syn'])].copy()
    energy_demand_TWh_excluded = energy_demand_TWh_excluded.loc[~energy_demand_TWh_excluded['energy-carrier'].isin(['gaseous-syn', 'liquid-syn'])].copy()
    # energy-carrier-category = biofuels
    energy_demand_TWh_excluded['energy-carrier-category'] = "biofuels"
    # energy-carrier-category = synfuels
    energy_demand_TWh_excluded_excluded['energy-carrier-category'] = "synfuels"
    energy_demand_TWh_excluded = pd.concat([energy_demand_TWh_excluded, energy_demand_TWh_excluded_excluded.set_index(energy_demand_TWh_excluded_excluded.index.astype(str) + '_dup')])
    # energy-carrier-category = ffuels
    energy_demand_TWh_2['energy-carrier-category'] = "ffuels"
    energy_demand_TWh_2 = pd.concat([energy_demand_TWh_2, energy_demand_TWh_excluded.set_index(energy_demand_TWh_excluded.index.astype(str) + '_dup')])

    # IN-SITU EMISSIONS (for carbon capture)
    # For carbon-capture and energy required for carbon capture : apply emission factor of fossil fuels !
    # As emission factor of bio- and syn-fuels are set to 0 to represent the fact that CO2 emissions were previously captured by vegetation / H2 syn, it is not possible to determine CO2 captured based on these emissions.
    # In order to have these values : we assume bio- / syn-fuels emit the same way as there fossil fuel equivalent and apply, then, carbon capture and energy required for CC on these emissions
    # => Liquid fuels (bio / syn / ff) : liquid fossil emission factor
    # => Gaseous fuels (bio / syn / ff) : gaseous fossil emission factor
    # => Solid fuels (bio / syn / ff) : solid fossil emission factor
    # 
    # This factor emission "correction" only applies for CO2 gaes and BioSyn fuels

    # energy-demand [TWh]
    energy_demand_TWh_3 = use_variable(input_table=energy_demand_TWh_2, selected_variable='energy-demand[TWh]')
    # Split on feedstock type Top : excluding-feedstock Bottom : feedstock
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[energy_demand_TWh_2['feedstock-type'].isin(['excl-feedstock'])].copy()

    # COMBUSTION : excluding-feedstock only
    # For emissions : apply emission factor (defined in technology module)
    # => Fossil fuels = fossil fuels emissions factors
    # => Biofuels = biofuels emissions factors
    # (usually set to 0 as the biofuels emit CO2 but this CO2 was previously capture by trees and other vegetation so we consider nul emissions)
    # => Synfuels = synfuels emissions factors
    # (usually set to 0 or not preset as equaled to 0)

    # RCP ind-combustion-emission-factor [Mt/TWh]
    ind_combustion_emission_factor_Mt_per_TWh = import_data(trigram='ind', variable_name='ind-combustion-emission-factor', variable_type='RCP')
    # emissions[Mt] excluding-feedstock only = energy-demand[TWh] * ind-combustion-emission-factor [Mt/TWh]
    emissions_Mt_2 = mcd(input_table_1=energy_demand_TWh_2, input_table_2=ind_combustion_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, material, technology, emission-type, energy-carrier-category, ets-or-not, gaes, route
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'ets-or-not', 'energy-carrier-category', 'emission-type', 'gaes'], aggregation_method='Sum')
    emissions_Mt = pd.concat([emissions_Mt, emissions_Mt_2.set_index(emissions_Mt_2.index.astype(str) + '_dup')])

    # Calibration
    # Calibration is executed in two stages (on group and then on total). This is done for a difference in granularity of calibration data from UNFCCC, depending on Annex I and non-Annex I.
    # 
    # 1. Calibration on groups
    # Calibration groups are defined in CP in the google sheets, this CP is used to group the mat-route-tech by calibration-groups. While calibration values are already given by calibration-groups in GS.
    # 
    # 2. Calibration on total (by emission-type and gaes)
    # Total emissions are calibrated on total to insure their consistencies if data for calibration by group is not available.

    # Calibration on groups

    # emissions[Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Add calibration-type = CRF-group
    emissions_Mt['calibration-type'] = "CRF-group"
    # rename to emission-type
    out_9946_1 = ind_calibration_groups.rename(columns={'CRF-process-calibration-group': 'process', 'CRF-combustion-calibration-group': 'combustion'})
    # Keep combustion & process -calibration-group
    out_9941_1 = unpivoting(df=out_9946_1, filter_type='STANDARD', id_values=['process', 'combustion'], id_variables=['material', 'route', 'technology', 'Eurostat-energy-calibration-group', 'Eurostat-non-energy-calibration-group', 'UN-energy-calibration-group', 'data_type', 'module'], not_to_keep=['Eurostat-energy-calibration-group', 'Eurostat-non-energy-calibration-group', 'process', 'combustion', 'UN-energy-calibration-group', 'data_type', 'module'], retained_type='STANDARD', to_keep=['material', 'route', 'technology'])
    # rename to group
    out_9942_1 = out_9941_1.rename(columns={'ColumnNames': 'emission-type', 'ColumnValues': 'group'})
    # LEFT OUTER
    out_9949_1 = joiner(df_left=emissions_Mt, df_right=out_9942_1, joiner='left', left_input=['material', 'route', 'technology', 'emission-type'], right_input=['material', 'route', 'technology', 'emission-type'])
    # Group by emission-type, calibration-type,  gaes (sum)
    out_9949_1_2 = group_by_dimensions(df=out_9949_1, groupby_dimensions=['Country', 'Years', 'emission-type', 'gaes', 'calibration-type', 'group'], aggregation_method='Sum')
    # Calibration emissions-by-group [Mt]
    emissions_by_group_Mt = import_data(trigram='ind', variable_name='emissions-by-group', variable_type='Calibration')
    # Apply Calibration on emissions by calibration group
    _, out_9945_2, out_9945_3 = calibration(input_table=out_9949_1_2, cal_table=emissions_by_group_Mt, data_to_be_cal='emissions[Mt]', data_cal='emissions-by-group[Mt]')

    # Cal_rate for emissions

    # Cal rate for emissions-by-group
    cal_rate_emissions_Mt_2 = use_variable(input_table=out_9945_3, selected_variable='cal_rate_emissions[Mt]')
    # Apply cal-rate on emissions
    emissions_Mt = mcd(input_table_1=out_9949_1, input_table_2=out_9945_2, operation_selection='x * y', output_name='emissions[Mt]')

    # Calibration on totals

    # emissions [Mt] calibrated  by group
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Change calibration-type = CRF-total
    emissions_Mt['calibration-type'] = "CRF-total"
    # Group by emission-type, calibration-type,  gaes (sum)
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'gaes', 'emission-type', 'calibration-type'], aggregation_method='Sum')
    # Calibration emissions-total [Mt]
    emissions_total_Mt = import_data(trigram='ind', variable_name='emissions-total', variable_type='Calibration')
    # Apply Calibration on emissions
    _, out_9364_2, out_9364_3 = calibration(input_table=emissions_Mt_2, cal_table=emissions_total_Mt, data_to_be_cal='emissions[Mt]', data_cal='emissions-total[Mt]')
    # Cal rate for emissions-total
    cal_rate_emissions_Mt = use_variable(input_table=out_9364_3, selected_variable='cal_rate_emissions[Mt]')
    cal_rate_emissions_Mt = pd.concat([cal_rate_emissions_Mt_2, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])
    cal_rate = pd.concat([cal_rate, cal_rate_emissions_Mt.set_index(cal_rate_emissions_Mt.index.astype(str) + '_dup')])
    # Module = CALIBRATION
    cal_rate = column_filter(df=cal_rate, pattern='^.*$')
    # emissions[Mt] (replace) = emissions[Mt] * cal_rate[%]
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_9364_2, operation_selection='x * y', output_name='emissions[Mt]')

    # Aggregate emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    ind_combustion_emission_factor_Mt_per_TWh = ind_combustion_emission_factor_Mt_per_TWh.loc[~ind_combustion_emission_factor_Mt_per_TWh['energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'liquid-bio', 'liquid-syn', 'solid-biomass'])].copy()

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
        enegy_carrier_list = output_table["energy-carrier"].unique()
        
        # For values needed : if not in list => copy values from corresponding
        for val in values_needed.keys():
            if val not in enegy_carrier_list:
                val_mask = values_needed[val]
                mask = (output_table["energy-carrier"].str.find(val_mask) >= 0)
                temp = output_table.loc[mask,:].copy()
                temp["energy-carrier"] = val
                output_table = pd.concat([output_table,temp], ignore_index=True)
        
        # Keep only Bio-Syn fuels and CO2
        mask = (output_table["energy-carrier"].isin(values_needed.keys())) & (output_table['gaes'] == "CO2")
        output_table = output_table.loc[mask, :]
        
        output_table.rename(columns={'ind-combustion-emission-factor[Mt/TWh]':'insitu-emission-factor[Mt/TWh]'}, inplace=True)
        return output_table
    # syn / bio : add them with ff values  Keep only Bio-syn-fuels and CO2  Rename as insitu-emission-factor [Mt/TWh] 
    out_8017_1 = helper_8017(input_table=ind_combustion_emission_factor_Mt_per_TWh)
    # insitu-emissions[Mt] = energy-demand[TWh] * insitu-emission-factor [Mt/TWh]
    insitu_emissions_Mt = mcd(input_table_1=energy_demand_TWh_3, input_table_2=out_8017_1, operation_selection='x * y', output_name='insitu-emissions[Mt]')
    # Split on feedstock type Top : feedstock Bottom : excluding-feedstock
    insitu_emissions_Mt_excluded = insitu_emissions_Mt.loc[insitu_emissions_Mt['feedstock-type'].isin(['excl-feedstock'])].copy()
    insitu_emissions_Mt_2 = insitu_emissions_Mt.loc[~insitu_emissions_Mt['feedstock-type'].isin(['excl-feedstock'])].copy()

    # COMBUSTION (excluding feedstock only) + PROCESS : Carbon capture
    # - Carbon capture while burning fuel
    # - Carbon capture during material process
    # 
    # Note : We consider calibrated emissions for carbon capture, but we add to these emission the insitu emissions (CO2 Bio-/Syn-fuels) as they are not taken into account in the calibrated emissions (set to 0 there)

    # insitu-emissions [Mt] (CO2 Bio-/Syn-fuels)
    insitu_emissions_Mt = use_variable(input_table=insitu_emissions_Mt_excluded, selected_variable='insitu-emissions[Mt]')
    # Group by  Country, Years, material, technology, emission-type, feedstock-type, energy-carrier-category, ets-or-not, gaes, route
    insitu_emissions_Mt = group_by_dimensions(df=insitu_emissions_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'ets-or-not', 'feedstock-type', 'energy-carrier-category', 'emission-type', 'gaes'], aggregation_method='Sum')

    # EMBEDDED FEEDSTOCK (Carbon capture): feedstock only
    # - Feedstock CO2 (CCU) (CO2 computed in Industry module > Industry uses CO2 to build materials)
    # - Embedded carbon in efulels and biofuels as feedstock (CO2 computed in Power supply module > fuels requires CO2)

    # insitu-emissions [Mt]
    insitu_emissions_Mt_2 = use_variable(input_table=insitu_emissions_Mt_2, selected_variable='insitu-emissions[Mt]')
    # replace insitu-emissions[Mt] by CCU[Mt]
    out_9853_1 = insitu_emissions_Mt_2.rename(columns={'insitu-emissions[Mt]': 'CCU[Mt]'})
    # CCU[Mt] from efuels / biofuels used as feedstock
    CCU_Mt_2 = use_variable(input_table=out_9853_1, selected_variable='CCU[Mt]')
    CCU_Mt_2 = pd.concat([CCU_Mt_2, CCU_Mt_3.set_index(CCU_Mt_3.index.astype(str) + '_dup')])
    # RCP ind-ccu-emission-factor [Mt/Mt]
    ind_ccu_emission_factor_Mt_per_Mt = import_data(trigram='ind', variable_name='ind-ccu-emission-factor', variable_type='RCP')
    # CCU[Mt] feedstock only = in-situ-emissions[Mt] * ind-ccu-emission-factor [Mt/Mt]
    CCU_Mt_2 = mcd(input_table_1=CCU_Mt_2, input_table_2=ind_ccu_emission_factor_Mt_per_Mt, operation_selection='x * y', output_name='CCU[Mt]')
    # emission-type =  embedded-feedstock
    CCU_Mt_2['emission-type'] = "embedded-feedstock"
    # emissions[Mt] = emissions[Mt] * (-1) (as we stock CO2  and do not emit it)
    emissions_Mt_2 = CCU_Mt_2.assign(**{'emissions[Mt]': CCU_Mt_2['CCU[Mt]']*(-1.0)})
    # emissions [Mt]
    emissions_Mt_2 = export_variable(input_table=emissions_Mt_2, selected_variable='emissions[Mt]')
    # emissions [Mt]
    emissions_Mt_2 = use_variable(input_table=emissions_Mt_2, selected_variable='emissions[Mt]')
    # Add CCU emissions (embedded feedstcok)
    emissions_Mt = pd.concat([emissions_Mt, emissions_Mt_2.set_index(emissions_Mt_2.index.astype(str) + '_dup')])
    # Group by material, tech, ets-or-not, feedstock-type, emission-type, gaes, energy-carrier-category, route (sum)
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'ets-or-not', 'feedstock-type', 'energy-carrier-category', 'emission-type', 'gaes'], aggregation_method='Sum')
    # Missing dimensions (string) set to " "
    emissions_Mt = missing_value(df=emissions_Mt, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # emissions [Mt]
    emissions_Mt = export_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # emissions [Mt] (after calibration)
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Keep emission-type = process and combustion
    emissions_Mt_2 = emissions_Mt.loc[emissions_Mt['emission-type'].isin(['process', 'combustion'])].copy()
    # emissions[Mt] (replace) = emissions[Mt] + insitu-emissions[Mt]  left Join if missing insitu : set 0
    emissions_Mt_2 = mcd(input_table_1=emissions_Mt_2, input_table_2=insitu_emissions_Mt, operation_selection='x + y', output_name='emissions[Mt]', fill_value_bool='Left [x] Outer Join')
    # Keep only CO2
    emissions_Mt_2 = emissions_Mt_2.loc[emissions_Mt_2['gaes'].isin(['CO2'])].copy()

    # Apply CC lever (avoid) - Only CO2
    # => Determine the share of CC applied to :
    # - Combustion emissions (only excl-feedstock)
    # - Process emissions

    # OTS/FTS CC [%]
    CC_percent = import_data(trigram='ind', variable_name='CC')
    # CC[Mt] = emissions[Mt] * CC[%] (excl-feedstock)
    CC_Mt = mcd(input_table_1=emissions_Mt_2, input_table_2=CC_percent, operation_selection='x * y', output_name='CC[Mt]')
    # CC[Mt]
    CC_Mt = export_variable(input_table=CC_Mt, selected_variable='CC[Mt]')
    # CC[Mt]
    CC_Mt = use_variable(input_table=CC_Mt, selected_variable='CC[Mt]')
    # Group by  gaes, energy-carrier-category (sum)
    CC_Mt_2 = group_by_dimensions(df=CC_Mt, groupby_dimensions=['Country', 'Years', 'gaes', 'material', 'energy-carrier-category'], aggregation_method='Sum')
    # energy-carrier-category to primary-energy-carrier  material to way-of-production
    out_9571_1 = CC_Mt_2.rename(columns={'energy-carrier-category': 'primary-energy-carrier', 'material': 'way-of-production'})
    # CP Global Warming Potential GWP
    clt_gwp = import_data(trigram='clt', variable_name='clt_gwp', variable_type='CP')
    # OTS/FTS wacc [%] from TEC
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = ind
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['ind'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Compute opex for CC[tCO2e]
    out_9631_1 = compute_costs(df_activity=material_production_kt, df_unit_costs=costs_for_industry_material_MEUR_per_kt, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='material-production[kt]', cost_type='OPEX')
    # Compute capex for CC[tCO2e]
    out_9629_1 = compute_costs(df_activity=material_production_kt, df_unit_costs=costs_for_industry_material_MEUR_per_kt, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='material-production[kt]', include_unit_costs='true')

    # Capex ratio: low carbon tech / conventional tech
    # - steel : H2-DRI (primary)/BF-BOF(primary)
    # - chemicals : eMTO (primary)/tech (primary)
    # - ceramic : carbstone (primary)/tech (primary)
    # - cement : geopolym (primary)/dry-kiln (primary)

    # unit-capex [MEUR/unit]
    unit_capex_MEUR_per_unit_2 = use_variable(input_table=out_9629_1, selected_variable='unit-capex[MEUR/unit]')
    # capex [MEUR]
    capex_MEUR = use_variable(input_table=out_9629_1, selected_variable='capex[MEUR]')
    # RCP industry-energy-cost-user [-] from TECH
    industry_energy_cost_user_ = import_data(trigram='ind', variable_name='industry-energy-cost-user', variable_type='RCP')
    # opex[MEUR] = opex[MEUR] * industry-energy-cost-user
    opex_MEUR_2 = mcd(input_table_1=out_9631_1, input_table_2=industry_energy_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    # Group by  Country, Years material, technology, route (SUM)
    opex_MEUR = group_by_dimensions(df=opex_MEUR_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # capex[MEUR] = capex[MEUR] * industry-energy-cost-user
    capex_MEUR_2 = mcd(input_table_1=capex_MEUR, input_table_2=industry_energy_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    # Group by  Country, Years material, technology, route (SUM)
    capex_MEUR = group_by_dimensions(df=capex_MEUR_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    MEUR_3 = pd.concat([capex_MEUR, opex_MEUR.set_index(opex_MEUR.index.astype(str) + '_dup')])
    # Switch variable to double
    gwp_100 = math_formula(df=clt_gwp, convert_to_int=False, replaced_column='gwp-100[-]', splitted='$gwp-100[-]$')

    # Emission per energy use [tCO2e/TWh]

    # gwp-100 [-]
    gwp_100_2 = use_variable(input_table=gwp_100, selected_variable='gwp-100[-]')
    # Convert Unit Mt to t
    CC_t = CC_Mt.drop(columns='CC[Mt]').assign(**{'CC[t]': CC_Mt['CC[Mt]'] * 1000000.0})
    # CC[tCO2eq]
    CC_tCO2eq = mcd(input_table_1=CC_t, input_table_2=gwp_100, operation_selection='x * y', output_name='CC[tCO2eq]')
    # Group by  Country, Years material, technology, route (SUM)
    CC_tCO2eq = group_by_dimensions(df=CC_tCO2eq, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route'], aggregation_method='Sum')
    # RCP costs-for-industry-CC [EUR/tCO2e] from TECH
    costs_for_industry_CC_EUR_per_tCO2e = import_data(trigram='tec', variable_name='costs-for-industry-CC', variable_type='RCP')
    # Compute capex for CC[tCO2e]
    out_9616_1 = compute_costs(df_activity=CC_tCO2eq, df_unit_costs=costs_for_industry_CC_EUR_per_tCO2e, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='CC[tCO2eq]', include_unit_costs='true')
    # unit-capex [MEUR/unit] (for CC)
    unit_capex_MEUR_per_unit = use_variable(input_table=out_9616_1, selected_variable='unit-capex[MEUR/unit]')
    # total-capex[MEUR/unit] = unit-capex[MEUR/unit] + unit-capex[MEUR/unit] (for CC)
    total_capex_MEUR_per_unit = mcd(input_table_1=unit_capex_MEUR_per_unit_2, input_table_2=unit_capex_MEUR_per_unit, operation_selection='x + y', output_name='total-capex[MEUR/unit]')
    # route TOP: primary
    total_capex_MEUR_per_unit = total_capex_MEUR_per_unit.loc[total_capex_MEUR_per_unit['route'].isin(['primary'])].copy()
    # Keep material = steel, ceramics-others,  cement, ch-olefin
    total_capex_MEUR_per_unit = total_capex_MEUR_per_unit.loc[total_capex_MEUR_per_unit['material'].isin(['cement', 'ceramic-and-others', 'chemical-olefin', 'steel'])].copy()
    # Keep technology = hydrogen-DRI, carbstone, geopolym, e-MTO
    total_capex_MEUR_per_unit_2 = total_capex_MEUR_per_unit.loc[total_capex_MEUR_per_unit['technology'].isin(['geopolym', 'carbstone', 'e-MTO', 'hydrogen-DRI'])].copy()
    # technology as technology-num
    out_9844_1 = total_capex_MEUR_per_unit_2.rename(columns={'technology': 'technology-num'})
    # Keep technology = BF-BOF, tech, dry-kiln
    total_capex_MEUR_per_unit = total_capex_MEUR_per_unit.loc[total_capex_MEUR_per_unit['technology'].isin(['dry-kiln', 'tech', 'BF-BOF'])].copy()
    # technology as technology-den
    out_9845_1 = total_capex_MEUR_per_unit.rename(columns={'technology': 'technology-den'})
    # capex-ratio[%] = total-cost[MEUR] / total-cost[MEUR]
    capex_ratio_percent = mcd(input_table_1=out_9844_1, input_table_2=out_9845_1, operation_selection='x / y', output_name='capex-ratio[%]')
    # Group by country, years, material, tech-num, tech-den(sum)
    capex_ratio_percent = group_by_dimensions(df=capex_ratio_percent, groupby_dimensions=['Country', 'Years', 'material', 'technology-num', 'technology-den'], aggregation_method='Sum')
    # capex-ratio[%]
    capex_ratio_percent = export_variable(input_table=capex_ratio_percent, selected_variable='capex-ratio[%]')
    # capex [MEUR]
    capex_MEUR = use_variable(input_table=out_9616_1, selected_variable='capex[MEUR]')
    # RCP industry-CC-cost-user [-] from TECH
    industry_CC_cost_user_ = import_data(trigram='ind', variable_name='industry-CC-cost-user', variable_type='RCP')
    # capex[MEUR] = capex[MEUR] * industry-CC-cost-user
    capex_MEUR = mcd(input_table_1=capex_MEUR, input_table_2=industry_CC_cost_user_, operation_selection='x * y', output_name='capex[MEUR]')
    capex_MEUR_2 = pd.concat([capex_MEUR, capex_MEUR_2.set_index(capex_MEUR_2.index.astype(str) + '_dup')])

    # Cost by user

    # CAPEX

    # Group by  Country, Years, material, technology, route (SUM)
    capex_MEUR_2 = group_by_dimensions(df=capex_MEUR_2, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    # Group by  Country, Years material, technology, route (SUM)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route'], aggregation_method='Sum')
    # Compute opex for CC[tCO2e]
    out_9624_1 = compute_costs(df_activity=CC_tCO2eq, df_unit_costs=costs_for_industry_CC_EUR_per_tCO2e, df_price_indices=price_indices_, df_wacc=wacc_percent, module_name=module_name, activity_variable='CC[tCO2eq]', cost_type='OPEX')
    # opex[MEUR] = opex[MEUR] * industry-CC-cost-user
    opex_MEUR = mcd(input_table_1=out_9624_1, input_table_2=industry_CC_cost_user_, operation_selection='x * y', output_name='opex[MEUR]')
    opex_MEUR_2 = pd.concat([opex_MEUR, opex_MEUR_2.set_index(opex_MEUR_2.index.astype(str) + '_dup')])

    # OPEX

    # Group by  Country, Years, material, technology, route (SUM)
    opex_MEUR_2 = group_by_dimensions(df=opex_MEUR_2, groupby_dimensions=['Country', 'Years', 'cost-user'], aggregation_method='Sum')
    MEUR_2 = pd.concat([opex_MEUR_2, capex_MEUR_2.set_index(capex_MEUR_2.index.astype(str) + '_dup')])
    # Group by  Country, Years material, technology, route (SUM)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route'], aggregation_method='Sum')
    MEUR = pd.concat([capex_MEUR, opex_MEUR.set_index(opex_MEUR.index.astype(str) + '_dup')])
    # opex [MEUR]
    opex_MEUR = use_variable(input_table=MEUR, selected_variable='opex[MEUR]')
    # replace opex[MEUR] by opex-CC[MEUR]
    out_10077_1 = opex_MEUR.rename(columns={'opex[MEUR]': 'opex-CC[MEUR]'})
    # Group by  Country, Years (SUM)
    out_10077_1 = group_by_dimensions(df=out_10077_1, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # capex [MEUR]
    capex_MEUR = use_variable(input_table=MEUR, selected_variable='capex[MEUR]')
    # replace capex[MEUR] by capex-CC[MEUR]
    out_10076_1 = capex_MEUR.rename(columns={'capex[MEUR]': 'capex-CC[MEUR]'})
    # Group by  Country, Years (SUM)
    out_10076_1 = group_by_dimensions(df=out_10076_1, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    out_1_2 = pd.concat([out_10076_1, out_10077_1.set_index(out_10077_1.index.astype(str) + '_dup')])
    # CP co2-concentration-mapping
    ind_co2_concentration_mapping = import_data(trigram='ind', variable_name='ind_co2-concentration-mapping', variable_type='CP')
    # Remove data_type and module
    ind_co2_concentration_mapping = column_filter(df=ind_co2_concentration_mapping, columns_to_drop=['data_type', 'module'])
    # LEFT OUTER Add co2 concentration
    out_9452_1 = joiner(df_left=CC_Mt, df_right=ind_co2_concentration_mapping, joiner='left', left_input=['material'], right_input=['material'])
    # RCP CC-specific-energy-consumption [TWh/Mt]  from TECH
    CC_specific_energy_consumption_TWh_per_Mt = import_data(trigram='tec', variable_name='CC-specific-energy-consumption', variable_type='RCP')
    # energy-demand[TWh] = CC[Mt] * CC-specific-energy-consumption [TWh/Mt]
    energy_demand_TWh_2 = mcd(input_table_1=out_9452_1, input_table_2=CC_specific_energy_consumption_TWh_per_Mt, operation_selection='x * y', output_name='energy-demand[TWh]')
    # OTS/FTS CC-energy-efficiency
    CC_energy_efficiency = import_data(trigram='ind', variable_name='CC-energy-efficiency')
    # energy-demand[TWh] (replace) = energy-demand[TWh] * (1 - energy-efficiency[%])  LEFT JOIN il missing : set to 0
    energy_demand_TWh_3 = mcd(input_table_1=energy_demand_TWh_2, input_table_2=CC_energy_efficiency, operation_selection='x * (1-y)', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join')
    # Group by  material, technology, energy-carrier, ets-or-not, route (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh_3, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route', 'ets-or-not', 'energy-carrier'], aggregation_method='Sum')
    # energy-demand [TWh] only for CC
    energy_demand_TWh_3 = export_variable(input_table=energy_demand_TWh_3, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh] only for CC
    energy_demand_TWh_4 = use_variable(input_table=energy_demand_TWh_3, selected_variable='energy-demand[TWh]')
    # Group by  material (sum)
    energy_demand_TWh_4 = group_by_dimensions(df=energy_demand_TWh_4, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Rename variable to CC-energy-demand -by-material[TWh]
    out_9583_1 = energy_demand_TWh_4.rename(columns={'energy-demand[TWh]': 'CC-energy-demand-by-material[TWh]'})
    # energy-saving[TWh] = energy-demand[TWh] * energy-efficiency[%]
    energy_saving_TWh = mcd(input_table_1=energy_demand_TWh_2, input_table_2=CC_energy_efficiency, operation_selection='x * y', output_name='energy-saving[TWh]')
    # EE-saving[TWh] = energy-saving[TWh] / normalized-material-production[-]  LEFT OUTER Join If missing : set 0 (for Years < baseyear)
    EE_saving_TWh_2 = mcd(input_table_1=energy_saving_TWh, input_table_2=normalized_material_production_2, operation_selection='x / y', output_name='EE-saving[TWh]', fill_value_bool='Left [x] Outer Join')
    # Set to 0
    EE_saving_TWh_2 = missing_value(df=EE_saving_TWh_2, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # EE-saving [TWh] from CC
    EE_saving_TWh_2 = use_variable(input_table=EE_saving_TWh_2, selected_variable='EE-saving[TWh]')
    EE_saving_TWh = pd.concat([EE_saving_TWh, EE_saving_TWh_2.set_index(EE_saving_TWh_2.index.astype(str) + '_dup')])
    # Group by  Country, Years, material, technology, energy-carrier, feedstock-type, route (sum)
    EE_saving_TWh = group_by_dimensions(df=EE_saving_TWh, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'energy-carrier', 'feedstock-type'], aggregation_method='Sum')
    # Lag EE-saving[TWh]
    out_8089_1, out_8089_2 = lag_variable(df=EE_saving_TWh, in_var='EE-saving[TWh]')
    # Timestep
    Timestep = use_variable(input_table=out_8089_2, selected_variable='Timestep')
    # EE-saving- lagged [TWh]
    EE_saving_lagged_TWh = use_variable(input_table=out_8089_1, selected_variable='EE-saving_lagged[TWh]')
    # EE-saving-delta[TWh] = EE-saving[TWh] - EE-saving-lagged[TWh]
    EE_saving_delta_TWh = mcd(input_table_1=EE_saving_TWh, input_table_2=EE_saving_lagged_TWh, operation_selection='x - y', output_name='EE-saving-delta[TWh]')
    # EE-saving-delta[TWh] (replace) = EE-saving-delta[TWh] / Timestep
    EE_saving_delta_TWh = mcd(input_table_1=EE_saving_delta_TWh, input_table_2=Timestep, operation_selection='x / y', output_name='EE-saving-delta[TWh]')
    # Set o 0
    EE_saving_delta_TWh = missing_value(df=EE_saving_delta_TWh, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # If < 0 set to 0  * 15 (capex is equalled to energy savings over 15 years)
    mask = EE_saving_delta_TWh['EE-saving-delta[TWh]']<0
    EE_saving_delta_TWh.loc[mask, 'EE-saving-delta[TWh]'] =  0
    EE_saving_delta_TWh.loc[~mask, 'EE-saving-delta[TWh]'] =  15*EE_saving_delta_TWh.loc[~mask, 'EE-saving-delta[TWh]']
    # OTS/FTS exogenous-energy-costs [EUR/MWh] from TECH
    exogenous_energy_costs_EUR_per_MWh = import_data(trigram='tec', variable_name='exogenous-energy-costs')
    # Convert Unit EUR/MWh to MEUR/TWh
    exogenous_energy_costs_MEUR_per_TWh = exogenous_energy_costs_EUR_per_MWh.drop(columns='exogenous-energy-costs[EUR/MWh]').assign(**{'exogenous-energy-costs[MEUR/TWh]': exogenous_energy_costs_EUR_per_MWh['exogenous-energy-costs[EUR/MWh]'] * 1.0})
    # capex[MEUR] = EE-saving-delta[TWh] * capex-EE
    capex_MEUR = mcd(input_table_1=EE_saving_delta_TWh, input_table_2=exogenous_energy_costs_MEUR_per_TWh, operation_selection='x * y', output_name='capex[MEUR]')
    # OTS/FTS primary-energy-costs [EUR/MWh] from TECH
    primary_energy_costs_EUR_per_MWh = import_data(trigram='tec', variable_name='primary-energy-costs')
    # Convert Unit EUR/MWh to MEUR/TWh
    primary_energy_costs_MEUR_per_TWh = primary_energy_costs_EUR_per_MWh.drop(columns='primary-energy-costs[EUR/MWh]').assign(**{'primary-energy-costs[MEUR/TWh]': primary_energy_costs_EUR_per_MWh['primary-energy-costs[EUR/MWh]'] * 1.0})
    # capex[MEUR] = EE-saving-delta[TWh] * capex-EE
    capex_MEUR_2 = mcd(input_table_1=EE_saving_delta_TWh, input_table_2=primary_energy_costs_MEUR_per_TWh, operation_selection='x * y', output_name='capex[MEUR]')
    capex_MEUR = pd.concat([capex_MEUR_2, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])
    # Assumption lifetime = 25 years
    out_9998_1 = spread_capital(output_table=capex_MEUR, df_wacc=wacc_percent)
    # Group by  Country, Years, material, technology, route (SUM)
    out_9998_1 = group_by_dimensions(df=out_9998_1, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # Keep Years >= baseyear
    out_9998_1, _ = filter_dimension(df=out_9998_1, dimension='Years', operation_selection='=', value_years=Globals.get().base_year)
    out_9643_1 = pd.concat([MEUR_3, out_9998_1.set_index(out_9998_1.index.astype(str) + '_dup')])
    out_9642_1 = pd.concat([MEUR, out_9643_1.set_index(out_9643_1.index.astype(str) + '_dup')])
    # opex [MEUR]
    opex_MEUR = use_variable(input_table=out_9642_1, selected_variable='opex[MEUR]')
    # Group by  Country, Years material, technology, route (SUM)
    opex_MEUR = group_by_dimensions(df=opex_MEUR, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route'], aggregation_method='Sum')
    # capex [MEUR]
    capex_MEUR = use_variable(input_table=out_9642_1, selected_variable='capex[MEUR]')
    # Group by  Country, Years material, technology, route (SUM)
    capex_MEUR = group_by_dimensions(df=capex_MEUR, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'route'], aggregation_method='Sum')
    MEUR = pd.concat([opex_MEUR, capex_MEUR.set_index(capex_MEUR.index.astype(str) + '_dup')])
    out_10081_1 = pd.concat([MEUR, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    out_9990_1 = pd.concat([out_10081_1, MEUR_2.set_index(MEUR_2.index.astype(str) + '_dup')])

    # For : Climate
    # 
    # - Emissions 
    # - CO2 embedded in feedstock (both CCU-material and CO2 embedded from bio/e-fuels) (CC are sent to Climate via Power Supply)

    # Remove group column
    emissions_Mt_2 = column_filter(df=emissions_Mt, pattern='^((?!group).)*$')
    # energy-carrier-category to primary-energy-carrier
    out_9579_1 = emissions_Mt_2.rename(columns={'energy-carrier-category': 'way-of-production'})
    # Exclude emission-type = embedded-feedstock (bottom)
    out_9579_1_excluded = out_9579_1.loc[out_9579_1['emission-type'].isin(['embedded-feedstock'])].copy()
    out_9579_1 = out_9579_1.loc[~out_9579_1['emission-type'].isin(['embedded-feedstock'])].copy()
    # Add emissions-or-capture = embedded-feedstock
    out_9579_1_excluded['emissions-or-capture'] = "embedded-feedstock"
    # Add emissions-or-capture = emissions
    out_9579_1['emissions-or-capture'] = "emissions"
    out_9579_1 = pd.concat([out_9579_1, out_9579_1_excluded.set_index(out_9579_1_excluded.index.astype(str) + '_dup')])
    # emissions[MtCO2e] = emissions[Mt] x gwp[unit]
    emissions_MtCO2eq = mcd(input_table_1=gwp_100_2, input_table_2=emissions_Mt, operation_selection='x * y', output_name='emissions[MtCO2eq]')
    # emissions[MtCO2e]
    emissions_MtCO2eq = export_variable(input_table=emissions_MtCO2eq, selected_variable='emissions[MtCO2eq]')
    # emissions[MtCO2e]
    emissions_MtCO2eq_2 = use_variable(input_table=emissions_MtCO2eq, selected_variable='emissions[MtCO2eq]')
    # Group by  Country, Years material, technology, route (SUM)
    emissions_MtCO2eq_2 = group_by_dimensions(df=emissions_MtCO2eq_2, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology'], aggregation_method='Sum')
    # emissions-per-produtction[tCO2e/t] = emissions[MtCO2e] / material-production[Mt]
    emissions_per_produtction_tCO2e_per_t = mcd(input_table_1=emissions_MtCO2eq_2, input_table_2=material_production_Mt_4, operation_selection='x / y', output_name='emissions-per-produtction[tCO2e/t]')
    emissions_per_produtction_tCO2e_per_t = missing_value(df=emissions_per_produtction_tCO2e_per_t, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # emissions-per-produtction[tCO2e/t]
    emissions_per_produtction_tCO2e_per_t = export_variable(input_table=emissions_per_produtction_tCO2e_per_t, selected_variable='emissions-per-produtction[tCO2e/t]')
    # Group by country, years, (sum)
    emissions_MtCO2eq = group_by_dimensions(df=emissions_MtCO2eq, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Exclude excl-feedstock (bottom) the rest (top)  We apply CC energy-demand only on excl-feedstock
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()
    # energy-demand[TWh] (replace) = energy-demand[TWh] + energy-demand[TWh] (for CC)  LEFT JOIN set to 0 if missing
    energy_demand_TWh_2 = mcd(input_table_1=energy_demand_TWh_excluded, input_table_2=energy_demand_TWh_3, operation_selection='x + y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join')
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_2.set_index(energy_demand_TWh_2.index.astype(str) + '_dup')])
    # energy-demand [TWh] (including for CC)
    energy_demand_TWh = export_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')
    # Group by country, years, (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # emission-per-energy-consumption[MtCO2/TWh] = emissions[MtCO2e] / energy-demand[TWh]
    emission_per_energy_consumption_MtCO2_per_TWh = mcd(input_table_1=emissions_MtCO2eq, input_table_2=energy_demand_TWh_2, operation_selection='x / y', output_name='emission-per-energy-consumption[MtCO2/TWh]')
    # Convert Unit Gpkm to pkm
    emission_per_energy_consumption_tCO2_per_TWh = emission_per_energy_consumption_MtCO2_per_TWh.drop(columns='emission-per-energy-consumption[MtCO2/TWh]').assign(**{'emission-per-energy-consumption[tCO2/TWh]': emission_per_energy_consumption_MtCO2_per_TWh['emission-per-energy-consumption[MtCO2/TWh]'] * 1000000.0})
    # emission-per-energy-consumption[tCO2/TWh]
    emission_per_energy_consumption_tCO2_per_TWh = export_variable(input_table=emission_per_energy_consumption_tCO2_per_TWh, selected_variable='emission-per-energy-consumption[tCO2/TWh]')
    # Group by  gaes, energy-carrier-category (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'material', 'route', 'technology', 'energy-carrier', 'feedstock-type'], aggregation_method='Sum')
    out_9995_1 = pd.concat([energy_demand_TWh_2, material_production_Mt_3.set_index(material_production_Mt_3.index.astype(str) + '_dup')])
    # Module = Air Quality
    out_9995_1 = column_filter(df=out_9995_1, pattern='^.*$')
    # Group by Country, Years, energy-carrier (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    out_9497_1 = pd.concat([energy_demand_TWh_2, out_9571_1.set_index(out_9571_1.index.astype(str) + '_dup')])
    out_9516_1 = pd.concat([out_9497_1, CCU_Mt.set_index(CCU_Mt.index.astype(str) + '_dup')])
    # Module = Power
    out_9516_1 = column_filter(df=out_9516_1, pattern='^.*$')
    # LEFT join on materials
    out_7717_1 = joiner(df_left=energy_demand_TWh, df_right=out_7718_1, joiner='left', left_input=['material'], right_input=['material'])
    # Group by  carrier, feestock, group (sum)
    out_7717_1 = group_by_dimensions(df=out_7717_1, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'feedstock-type', 'material-group'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-carrier-feedstock -group[TWh]
    out_9261_1 = out_7717_1.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier-feedstock-group[TWh]'})
    # Group by  energy-carrier, feedstock-type (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'feedstock-type'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-carrier-feedstock[TWh]
    out_9256_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier-feedstock[TWh]'})

    # Share of alternative fuel in Final Energy Consumption excluding and including feedstock [%]

    # energy-demand-by- carrier-feedstock[TWh]
    energy_demand_by_carrier_feedstock_TWh = use_variable(input_table=out_9256_1, selected_variable='energy-demand-by-carrier-feedstock[TWh]')
    # Top: only excl-feedstock
    energy_demand_by_carrier_feedstock_TWh_2 = energy_demand_by_carrier_feedstock_TWh.loc[energy_demand_by_carrier_feedstock_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()
    energy_demand_by_carrier_feedstock_TWh_excluded_2 = energy_demand_by_carrier_feedstock_TWh.loc[~energy_demand_by_carrier_feedstock_TWh['feedstock-type'].isin(['excl-feedstock'])].copy()
    # Group by country, years, feedstock-type (sum)
    energy_demand_by_carrier_feedstock_TWh_excluded = group_by_dimensions(df=energy_demand_by_carrier_feedstock_TWh_excluded_2, groupby_dimensions=['Country', 'Years', 'feedstock-type'], aggregation_method='Sum')
    # Top: syn/bio liquid and gas plus hydrogen
    energy_demand_by_carrier_feedstock_TWh_excluded_2 = energy_demand_by_carrier_feedstock_TWh_excluded_2.loc[energy_demand_by_carrier_feedstock_TWh_excluded_2['energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'hydrogen', 'liquid-bio', 'liquid-syn'])].copy()
    # Group by country, years, feedstock-type (sum)
    energy_demand_by_carrier_feedstock_TWh_excluded_2 = group_by_dimensions(df=energy_demand_by_carrier_feedstock_TWh_excluded_2, groupby_dimensions=['Country', 'Years', 'feedstock-type'], aggregation_method='Sum')
    # alternative-fuel-share[kWh/kWh] = energy-demand-by- carrier-feedstock[TWh] (only alternative fuel) / energy-demand-by- carrier-feedstock[TWh]
    alternative_fuel_share_kWh_per_kWh = mcd(input_table_1=energy_demand_by_carrier_feedstock_TWh_excluded_2, input_table_2=energy_demand_by_carrier_feedstock_TWh_excluded, operation_selection='x / y', output_name='alternative-fuel-share[kWh/kWh]')
    # Group by country, years, feedstock-type (sum)
    energy_demand_by_carrier_feedstock_TWh = group_by_dimensions(df=energy_demand_by_carrier_feedstock_TWh_2, groupby_dimensions=['Country', 'Years', 'feedstock-type'], aggregation_method='Sum')
    # Top: syn/bio liquid and gas plus hydrogen
    energy_demand_by_carrier_feedstock_TWh_3 = energy_demand_by_carrier_feedstock_TWh_2.loc[energy_demand_by_carrier_feedstock_TWh_2['energy-carrier'].isin(['gaseous-bio', 'gaseous-syn', 'hydrogen', 'liquid-bio', 'liquid-syn'])].copy()
    # Group by country, years, feedstock-type (sum)
    energy_demand_by_carrier_feedstock_TWh_3 = group_by_dimensions(df=energy_demand_by_carrier_feedstock_TWh_3, groupby_dimensions=['Country', 'Years', 'feedstock-type'], aggregation_method='Sum')
    # alternative-fuel-share[kWh/kWh] = energy-demand-by- carrier-feedstock[TWh] (only alternative fuel) / energy-demand-by- carrier-feedstock[TWh]
    alternative_fuel_share_kWh_per_kWh_2 = mcd(input_table_1=energy_demand_by_carrier_feedstock_TWh_3, input_table_2=energy_demand_by_carrier_feedstock_TWh, operation_selection='x / y', output_name='alternative-fuel-share[kWh/kWh]')
    # KPI's
    alternative_fuel_share_kWh_per_kWh = pd.concat([alternative_fuel_share_kWh_per_kWh_2, alternative_fuel_share_kWh_per_kWh.set_index(alternative_fuel_share_kWh_per_kWh.index.astype(str) + '_dup')])
    # alternative-fuel-share[kWh/kWh]
    alternative_fuel_share_kWh_per_kWh = export_variable(input_table=alternative_fuel_share_kWh_per_kWh, selected_variable='alternative-fuel-share[kWh/kWh]')
    # KPI's
    share_per = pd.concat([share_alt_plastics_t_per_t, alternative_fuel_share_kWh_per_kWh.set_index(alternative_fuel_share_kWh_per_kWh.index.astype(str) + '_dup')])
    # KPI's
    per = pd.concat([share_per, material_saved_t_per_t.set_index(material_saved_t_per_t.index.astype(str) + '_dup')])
    # KPI's
    per = pd.concat([per, material_foot_print_t_per_cap.set_index(material_foot_print_t_per_cap.index.astype(str) + '_dup')])
    # KPI's
    per = pd.concat([per, emission_per_energy_consumption_tCO2_per_TWh.set_index(emission_per_energy_consumption_tCO2_per_TWh.index.astype(str) + '_dup')])
    # KPI's
    out_10007_1 = pd.concat([per, capex_ratio_percent.set_index(capex_ratio_percent.index.astype(str) + '_dup')])
    # KPI's
    out_9701_1 = pd.concat([out_10007_1, share_route_material_production_percent.set_index(share_route_material_production_percent.index.astype(str) + '_dup')])
    # Top: only elec
    energy_demand_by_carrier_feedstock_TWh_2 = energy_demand_by_carrier_feedstock_TWh_2.loc[energy_demand_by_carrier_feedstock_TWh_2['energy-carrier'].isin(['electricity'])].copy()

    # Share of Electricity in Final Energy Consumption excluding feedstock [%]

    # share-elec-energy-demand-by-carrier[%] = energy-demand-by-carrier[Mt] (elec) / energy-demand-by-carrier[Mt] (total)
    share_elec_energy_demand_by_carrier_percent = mcd(input_table_1=energy_demand_by_carrier_feedstock_TWh_2, input_table_2=energy_demand_by_carrier_feedstock_TWh, operation_selection='x / y', output_name='share-elec-energy-demand-by-carrier[%]')
    # share-elec-energy- demand-by-carrier[%]
    share_elec_energy_demand_by_carrier_percent = export_variable(input_table=share_elec_energy_demand_by_carrier_percent, selected_variable='share-elec-energy-demand-by-carrier[%]')
    # KPI's
    out_9609_1 = pd.concat([out_9701_1, share_elec_energy_demand_by_carrier_percent.set_index(share_elec_energy_demand_by_carrier_percent.index.astype(str) + '_dup')])
    # KPI's
    out_9608_1 = pd.concat([out_9609_1, emissions_per_produtction_tCO2e_per_t.set_index(emissions_per_produtction_tCO2e_per_t.index.astype(str) + '_dup')])
    out_9980_1 = pd.concat([out_9608_1, material_demand_t.set_index(material_demand_t.index.astype(str) + '_dup')])
    # Group by  material (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'material'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-material[TWh]
    out_9258_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-material[TWh]'})
    # Group by  material, feedstock-type (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'material', 'feedstock-type'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-material-feedstock[TWh]
    out_9257_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-material-feedstock[TWh]'})
    # Concatenate to Pathway Explorer
    out_1_2 = pd.concat([out_9258_1, out_9257_1.set_index(out_9257_1.index.astype(str) + '_dup')])
    # Group by  material, technology, feedstock-type (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'material', 'technology', 'feedstock-type'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-material-tech -feedstock[TWh]
    out_9259_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-material-tech-feedstock[TWh]'})
    # Concatenate to Pathway Explorer
    out_1_2 = pd.concat([out_9259_1, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1_2 = pd.concat([out_1_2, out_9261_1.set_index(out_9261_1.index.astype(str) + '_dup')])
    # Group by  energy-carrier, ets (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'ets-or-not', 'energy-carrier'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-carrier-ets[TWh]
    out_9253_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier-ets[TWh]'})
    # Group by  material, ets (sum)
    energy_demand_TWh_3 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'material', 'ets-or-not'], aggregation_method='Sum')
    # Rename variable to energy-demand -by-material-ets[TWh]
    out_9254_1 = energy_demand_TWh_3.rename(columns={'energy-demand[TWh]': 'energy-demand-by-material-ets[TWh]'})
    # Concatenate to Pathway Explorer
    out_1_4 = pd.concat([out_9253_1, out_9254_1.set_index(out_9254_1.index.astype(str) + '_dup')])
    # Rename variable to energy-demand -by-carrier[TWh]
    out_9255_1 = energy_demand_TWh_2.rename(columns={'energy-demand[TWh]': 'energy-demand-by-carrier[TWh]'})
    # Concatenate to Pathway Explorer
    out_1_3 = pd.concat([out_9255_1, out_9256_1.set_index(out_9256_1.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1_3 = pd.concat([out_1_3, out_1_4.set_index(out_1_4.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1_2 = pd.concat([out_1_3, out_1_2.set_index(out_1_2.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1_3 = pd.concat([out_1_2, out_9583_1.set_index(out_9583_1.index.astype(str) + '_dup')])
    out_9654_1 = pd.concat([energy_demand_TWh, material_production_Mt.set_index(material_production_Mt.index.astype(str) + '_dup')])
    out_1_2 = pd.concat([out_9654_1, out_9579_1.set_index(out_9579_1.index.astype(str) + '_dup')])
    # Module = Climate
    out_1_2 = column_filter(df=out_1_2, pattern='^.*$')
    # material-import-share [%]
    material_import_share_percent_demand = use_variable(input_table=material_import_share_percent_demand, selected_variable='material-import-share[%demand]')
    # Concatenate to Pathway Explorer
    material_share_percent_demand = pd.concat([material_import_share_percent_demand, material_export_share_percent_demand.set_index(material_export_share_percent_demand.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_9225_1 = pd.concat([out_9224_1, material_share_percent_demand.set_index(material_share_percent_demand.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1 = pd.concat([out_1, out_9225_1.set_index(out_9225_1.index.astype(str) + '_dup')])
    # Concatenate to Pathway Explorer
    out_1 = pd.concat([out_1_3, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # KPI's
    out_1 = pd.concat([out_9980_1, out_1.set_index(out_1.index.astype(str) + '_dup')])
    # Costs
    out_1 = pd.concat([out_1, out_9990_1.set_index(out_9990_1.index.astype(str) + '_dup')])
    out_9251_1 = add_trigram(module_name=module_name, df=out_1)
    # Module = Pathway Explorer
    out_9251_1 = column_filter(df=out_9251_1, pattern='^.*$')

    return out_9251_1, cal_rate, material_production_Mt_7, out_9516_1, out_1_2, out_9995_1, out_7801_1, material_production_Mt_6, import_


