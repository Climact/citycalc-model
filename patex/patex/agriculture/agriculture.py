import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *

from .metanode_9481 import metanode_9481


# Agriculture module
def agriculture(lifestyle):
    # To Do List - energy:
    # 
    # -Mix evolution depedning on the practices


    # Disable electricity calibration
    # (delete ; was used only for ELIA)


    # gaseous-ff-natural = gaseous-ff-natural + gaseous-ff-lpg => STILL REQUIRED ?


    # We do not model kerosene demand !!
    # So emissions are a little bit lower in model than in reality !
    # (But kerosene = marginal => donc calibration fix cela)


    # Livestock byproducts use 
    # 
    # Objective: the node is computing how the agri-food industry byrpoducts (livestock) is used across the different sectors (fertilizer, bioenergy, biomaterial). 
    # 
    # 1. Summing up the agri-food byproducts supplies [t, kcal]
    # 2. Driving the biomass towards the different markets [%, t, kcal]
    # 
    # 
    # Main inputs: 
    # 
    # - Agri-food byproduct supplies [t, kcal]
    # - Byproduct use split [%, lever]
    # 
    # Main outputs: 
    # - Feedtock availble in the different markets [t, kcal]
    # 
    # 
    # NOT ACCURATE and not used => we should also add byproduct from beverage and biofuels ??


    # To Do List:
    # 
    # - Remove the offal/afat consumed by humans 
    # - Add the yields evolution in the lever


    # We add : oil-crop = vegetal-oil / processing yield
    # But emit the hypothesis that processing factor are false ?
    # we correct that doing 1/processing yield for oil and cake in the data sheet


    # - To refine -


    # Attention :
    # Données de calibration :
    # - meat-bovine : correspond à meat-bovine + dairy-milk (donc on devrait sommer les deux dans notre modèle avant la calibration !)
    # - Idem pour egg et poultry (tout est mis dans poultry dans les données de calibration mais pas dans notre modèle !)
    # 
    # Du coup ; suite du modèle supprimer le filtre sur dairy-milk car tout est compris dans meat-bovine


    # Mettre un quality check ? :
    # % of grazing and alt proteins = < 100% ??


    # Note :
    # For soirl residues : no LTS values (we need to complete years after baseyear)


    # 3.E. Prescribed burning of savannas
    # 
    # Not implemented yet => Should be reviewed in the future !


    # Biomass coming from :
    #   - energy crops
    #   - settlement
    #   - forest
    # => computed in land-use sub-module


    # Costs


    # From biofuel demand to biofuel costs - not accurate and not used in old module
    # 
    # Objective: to compute the cost of biofuels for the different sectors.
    # 
    # Old module : btl + hvo = advanced biodiesel / ezm = advanced bioethanol / est = conventional biodiesel / fer = conventional bioethanol
    # 
    # Calculation tree: 
    # 1. Aggregate the different liquid technology biofuels according to the granularity available for prices
    # 2. Aggregate the gas and solid biofuels by sector and fuel type
    # 3. Multiply the liquid, gas and solid biofuel demand by corresponding prices 
    # 4. Aggregate the liquid fuel cost technologies, and convert costs to MEUR.
    # 
    # Main inputs (from sectors): 
    # - Liquid biofuel demand by technology [TWh]
    # - Solid biofuel demand [TWh]
    # - Gas biofuel demand [TWh]
    # - Biofuel prices (from technology) [USD/TWh]
    # 
    # Main outputs: 
    # - Annual cost of biofuel by sector and fuel type [MEUR]


    # QUICK FIX


    # TEMP > delete when previous has been migrated


    # For : Air Pollution
    #  - Livestock population


    # For : Land-use
    #  - Cropland[ha]
    #  - Pasture[ha]


    # For : Land-use
    #  - Cropland[ha] with details


    # Pivot


    # Aggregation based on some columns
    # 
    # Here : Country / Years / energy-carrier / sector


    # For : Ammonia
    #  - Fertilizer demand



    module_name = 'agriculture'

    # Energy demand

    # Energy consumption linked to cropland management   
    # 
    # Objective: the node is computing the energy demand from the agriculture sector. Energy demand depends on the agricultue practices.
    # 
    # 1. Computing energy demand [TWh]
    # 
    # Main inputs: 
    # 
    # - Energy-use by hectare [TWh/ha]
    # - Land-use for agriculture[ha]
    # - Energy rate for irrigation and crops [%]
    # 
    # Main outputs: 
    # 1. Energy demand by vector [TWh]

    # Hard-coded here 
    # => To move in google sheet ??

    out_9561_1 = pd.DataFrame(columns=['category-from', 'energy-carrier-from', 'category-to', 'energy-carrier-to'], data=[['fffuels', 'gaseous-ff-natural', 'biofuels', 'gaseous-bio'], ['fffuels', 'liquid-ff-diesel', 'biofuels', 'liquid-bio'], ['fffuels', 'liquid-ff-gasoline', 'biofuels', 'liquid-bio'], ['fffuels', 'liquid-ff-oil', 'biofuels', 'liquid-bio'], ['fffuels', 'solid-ff-coal', 'biofuels', 'solid-biomass']])
    # ratio[-] = 1 (energy final, no need to  account for energy efficiency differences)
    ratio = out_9561_1.assign(**{'ratio[-]': 1.0})

    # Adapt data from other module => pivot and co

    # Potential for bio-energy production
    # Note : Some of the bioenergy production is made further, inside Land-Use node (when production comes from forest, settlement, bioenergy-crops)

    # Biomass coming from wastes (other than food wastes)

    # energy-production [TWh] (from lifestyle)
    energy_production_TWh = use_variable(input_table=lifestyle, selected_variable='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = other-wastes
    energy_production_TWh['origin'] = "other-wastes"

    # Product / material / ressource demand

    # Food demand (meat, vegetables, ...)

    # From food demand to food domestic production 
    # 
    # Objective: to compute the domestic production volume in kcal by accounting for consumer food-wastes and for import-export balance.
    # 
    # Calculation tree: 
    # 1. Summing the food demand to the food consumer wastes
    # 2. Accounting for the import/export flows 
    # 
    # Main inputs (from Lifestyles): 
    # - Food demand by food group & product  [kcal] 
    # - Food wastes by food group & product [kcal]
    # - Net Import/Export balance [%]
    # 
    # Main outputs: 
    # - Domestic food production accounting for waste [kcal]  (livestock)
    # - Domestic food production linked to import/export [kcal] (all categories)

    # food-waste [kcal] (from lifestyle)
    food_waste_kcal = use_variable(input_table=lifestyle, selected_variable='food-waste[kcal]')

    # Biomass coming from food wastes
    # We only consider food waste linked tocrops (not beverage, livestock, ...)

    # food-waste [kcal] (from lifestyle)
    food_waste_kcal_2 = use_variable(input_table=food_waste_kcal, selected_variable='food-waste[kcal]')

    # Apply biomass supply valorisation of co-products and co levers (switch)
    # => determine % of food waste collected for biogaz production

    # OTS/FTS food-waste-collection-rate [%]
    food_waste_collection_rate_percent = import_data(trigram='agr', variable_name='food-waste-collection-rate')
    # food-waste-production[kcal] = food-waste[kcal] * food-waste-collection-rate[%]
    food_waste_production_kcal = mcd(input_table_1=food_waste_kcal_2, input_table_2=food_waste_collection_rate_percent, operation_selection='x * y', output_name='food-waste-production[kcal]')
    # Group by Country, Years, category (sum)
    food_waste_production_kcal = group_by_dimensions(df=food_waste_production_kcal, groupby_dimensions=['Country', 'Years', 'category'], aggregation_method='Sum')
    # RCP food-waste-bioenergy-conv-factors [TWh/kcal]
    food_waste_bioenergy_conv_factors_TWh_per_kcal = import_data(trigram='agr', variable_name='food-waste-bioenergy-conv-factors', variable_type='RCP')
    # energy-production[TWh] = food-waste-production [kcal] * food-waste-bioenergy-conv-factors [TWh/kcal]
    energy_production_TWh_2 = mcd(input_table_1=food_waste_production_kcal, input_table_2=food_waste_bioenergy_conv_factors_TWh_per_kcal, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_2 = group_by_dimensions(df=energy_production_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = food-wastes
    energy_production_TWh_2['origin'] = "food-wastes"
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh])
    # food-demand [kcal] (from lifestyle)
    food_demand_kcal = use_variable(input_table=lifestyle, selected_variable='food-demand[kcal]')
    # overall-food-demand[kcal] = food-demand[kcal] + food-waste[kcal]
    overall_food_demand_kcal = mcd(input_table_1=food_waste_kcal, input_table_2=food_demand_kcal, operation_selection='x + y', output_name='overall-food-demand[kcal]')
    # overall-food-demand [kcal]
    overall_food_demand_kcal = export_variable(input_table=overall_food_demand_kcal, selected_variable='overall-food-demand[kcal]')

    # Apply food net import levers (switch)
    # => determine the amount of food that is imported

    # OTS/FTS food-net-import-product [kcal]
    food_net_import_product_kcal = import_data(trigram='agr', variable_name='food-net-import-product')

    # Formating data for other modules + Pathway Explorer

    # For : Scope 2/3
    #  - Food import

    # food-net-import-product [kcal]
    food_net_import_product_kcal_2 = use_variable(input_table=food_net_import_product_kcal, selected_variable='food-net-import-product[kcal]')
    # include positive  values
    out_9595_1 = row_filter(df=food_net_import_product_kcal_2, filter_type='RangeVal_RowFilter', that_column='food-net-import-product[kcal]', include=True, lower_bound_bool=True, upper_bound_bool=False, lower_bound='0.0', upper_bound=0)
    # Set to 0 (no need to account for export)
    food_net_import_product_kcal_3 = out_9595_1.assign(**{'food-net-import-product[kcal]': 0.0})
    # exclude positive  values
    out_9593_1 = row_filter(df=food_net_import_product_kcal_2, filter_type='RangeVal_RowFilter', that_column='food-net-import-product[kcal]', include=False, lower_bound_bool=True, upper_bound_bool=False, lower_bound='0.0', upper_bound=0)
    # * -1 (convert in positive values)
    food_net_import_product_kcal_2 = out_9593_1.assign(**{'food-net-import-product[kcal]': out_9593_1['food-net-import-product[kcal]']*(-1.0)})
    food_net_import_product_kcal_2 = pd.concat([food_net_import_product_kcal_3, food_net_import_product_kcal_2])
    # Group by  Country, Years, product (sum)
    food_net_import_product_kcal_2 = group_by_dimensions(df=food_net_import_product_kcal_2, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # domestic production[kcal] = overall-food-demand[kcal] + food-net-import-product[kcal]
    domestic_production_kcal = mcd(input_table_1=overall_food_demand_kcal, input_table_2=food_net_import_product_kcal, operation_selection='x + y', output_name='domestic-production[kcal]')
    # If < 0 Set 0 (no net production)
    mask = domestic_production_kcal['domestic-production[kcal]']<0
    domestic_production_kcal.loc[mask, 'domestic-production[kcal]'] = 0
    domestic_production_kcal.loc[~mask, 'domestic-production[kcal]'] = domestic_production_kcal.loc[~mask, 'domestic-production[kcal]']
    # domestic-production [kcal]
    domestic_production_kcal = export_variable(input_table=domestic_production_kcal, selected_variable='domestic-production[kcal]')

    # Livestock

    # From food domestic production to livestock domestic production
    # 
    # Objective: to compute the domestic production volume in kcal for livestock consumption (accounting for wasted food)
    # 
    # Calculation tree: 
    # 1. Calibrate livestock domestic production
    # 2. Accounting for improvement of livestock production (climate smart crop levers => reduce livestock losses)
    # 
    # Main inputs (from Lifestyles): 
    # - Overall food domestic production (including livestock production)
    # - Smart livestock losses[%] => should be >= 1 (1 = no losses)
    # 
    # Main outputs: 
    # - Domestic livestock production accounting for waste [kcal]  (livestock)

    # domestic-production [kcal]
    domestic_production_kcal = use_variable(input_table=domestic_production_kcal, selected_variable='domestic-production[kcal]')
    # cal livestock-domestic-production
    livestock_domestic_production = import_data(trigram='agr', variable_name='livestock-domestic-production', variable_type='Calibration')

    # Calibration
    # On domestic livestock production

    # Apply Calibration on domestic livestock production
    domestic_production_kcal_2, _, out_9207_3 = calibration(input_table=domestic_production_kcal, cal_table=livestock_domestic_production, data_to_be_cal='domestic-production[kcal]', data_cal='livestock-domestic-production[kcal]')
    # domestic-production [kcal] (calibrated on lifestock)
    domestic_production_kcal_2 = export_variable(input_table=domestic_production_kcal_2, selected_variable='domestic-production[kcal]')

    # Calibration RATES

    # Cal_rate for livestock-domestic-production[kcal]

    # cal_rate for livestock-domestic-production
    cal_rate_domestic_production_kcal = use_variable(input_table=out_9207_3, selected_variable='cal_rate_domestic-production[kcal]')
    # Keep livestock
    cal_rate_domestic_production_kcal = cal_rate_domestic_production_kcal.loc[cal_rate_domestic_production_kcal['category'].isin(['livestock'])].copy()
    # Add livestock to variable name
    out_9433_1 = cal_rate_domestic_production_kcal.rename(columns={'cal_rate_domestic-production[kcal]': 'cal_rate_livestock-domestic-production[kcal]'})

    # Apply climate smart livestock levers (improve)
    # => determine the losses ; we reduce losses thnaks to smart farming

    # OTS/FTS smart-losses-livestock [%]
    smart_losses_livestock_percent = import_data(trigram='agr', variable_name='smart-losses-livestock')
    # domestic-production-afw[kcal] = domestic-production[kcal] * smart-losses-livestock[%]
    domestic_production_afw_kcal = mcd(input_table_1=domestic_production_kcal_2, input_table_2=smart_losses_livestock_percent, operation_selection='x * y', output_name='domestic-production-afw[kcal]')
    # domestic-production-afw [kcal]
    domestic_production_afw_kcal = export_variable(input_table=domestic_production_afw_kcal, selected_variable='domestic-production-afw[kcal]')

    # Industry by-products (from livestock, beverage, alternative proteins)

    # From Livestock Population to Livestock based by-products supply
    # 
    # Objective:  to compute the byproducts supply from the livestock industry. 
    # 
    # 1.Computing the supplies of byproducts (using conversion factors, yields).
    # The byproducts may then be used as feedstock for bioenergy, fertilizers, animal feed or industrial uses.
    # 
    # => Byproduct :
    # = What remains of offals / animal fats after feeling the demand
    # = Production of offals / animal fats (produced with domestic lifestock) - demand (of offals prod and animal fats)
    # 
    # Main inputs: 
    # 
    # - Livestock population [lsu]
    # - Domestic production of afw (abp + meat) [kcal]
    # - Livestock by-product yields [kcal/lsu] = quantity of by-products we can have depending of the livestock size (lsu)
    # 
    # Main outputs: 
    # - Industry byproduct [kcal] :
    # 	* Offal production
    # 	* Animal fats

    # domestic-production-afw [kcal]
    domestic_production_afw_kcal = use_variable(input_table=domestic_production_afw_kcal, selected_variable='domestic-production-afw[kcal]')

    # Livestock feed requirements

    # From Meat demand to Animal Feed demand
    # 
    # Objective: the node is computing the animal feed requierement given the livestock population using the energy conversion efficiency ratio. 
    # 
    # 1. From livestock population by type to feed requierement by livestock type [kcal]
    # 2. Summing the animal feed requierement to compute the overal demand 
    # NOT ACCURATE
    # 
    # 
    # Main inputs: 
    # 
    # - Domestic production accounting for waste [kcal]
    # - Energy efficiency conversion ratio [kcal/kcal]
    # 
    # Main outputs: 
    # - Feed requierement per livestock type [kcal]
    # - Overall feed requierement [kcal]

    # RCP efficiency [%]
    efficiency_percent = import_data(trigram='agr', variable_name='efficiency', variable_type='RCP')
    # feed-requierement[kcal] =  domestic-production-afw[kcal] / efficiency[%]
    feed_requierement_kcal = mcd(input_table_1=domestic_production_afw_kcal, input_table_2=efficiency_percent, operation_selection='x / y', output_name='feed-requierement[kcal]')
    feed_requierement_kcal = feed_requierement_kcal.loc[~feed_requierement_kcal['product'].isin(['abp-dairy-milk'])].copy()
    # feed-requierement [kcal]
    feed_requierement_kcal = export_variable(input_table=feed_requierement_kcal, selected_variable='feed-requierement[kcal]')

    # Animal feed demand : from grazing and alternative proteins
    # 
    # Objective: the node is computing the demand for grazing [kcal] and alternative proteins [kcal] based on the livestock feed requirements and % of feed coming from grazing and alternative proteins (inclusing microalgae and insects based feed)
    # 
    # 1. The share of grass in the ration is set for each grazing livestock type and for each alternative proteins [%, kcal] 
    # 
    # 
    # Main inputs: 
    # 
    # - Livestock feed requierement [kcal] 
    # - Farming practices [lever; %]
    # - Alternative proteins [%] : Microalgae & insect meals demand [kcal] ??  (ALL = 0)
    # 
    # Main outputs: 
    # - Feed grass [kcal]
    # - Feed alternative proteins [kcal]

    # feed-requierement [kcal]
    feed_requierement_kcal = use_variable(input_table=feed_requierement_kcal, selected_variable='feed-requierement[kcal]')

    # Other animals feed demand (animal typical ration) - Others than from grazing / beverage ibp cereals / alternative proteins
    # 
    # Objective: the nodes are computing the typical animal ration based on the agriculture practices, the alternative protien sources, and the agri-food byproducts use. 
    # 
    # 1. Summing the supplies of alteranative and low-carbon animal feed
    # 2. Balancing the feed requierement based on alteranative and low-carbon animal feed supplies (1)
    # 3. Computing the crop demand based on the remaining feed requierement (2)
    # 
    # 
    # Main inputs: 
    # 
    # - Alternative protein sources supplies for animal feed [kcal]
    # - Feed grass [kcal]
    # - Feed from beverages by-products [kcal]
    # - Biomass industry by-products feed : ?? => was defined in formulae in old module but was not existing (thus not used) in old module .. ??
    # - Feed requierement [kcal] 
    # 
    # Main outputs: 
    # - Feed demand by type (crop) [kcal]

    # feed-requierement [kcal]
    feed_requierement_kcal_2 = use_variable(input_table=feed_requierement_kcal, selected_variable='feed-requierement[kcal]')
    # Group by  country, Years (SUM)
    feed_requierement_kcal_2 = group_by_dimensions(df=feed_requierement_kcal_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Apply climate smart livestock levers (switch)
    # => determine the % of feed requirements furnished by grazing

    # OTS/FTS land-man-pasture [%]
    land_man_pasture_percent = import_data(trigram='agr', variable_name='land-man-pasture')
    # feed-grass[kcal] =  feed-requierement[kcal] * land-man-pasture[%]
    feed_grass_kcal = mcd(input_table_1=feed_requierement_kcal, input_table_2=land_man_pasture_percent, operation_selection='x * y', output_name='feed-grass[kcal]')
    # feed-grass[kcal]
    feed_grass_kcal = export_variable(input_table=feed_grass_kcal, selected_variable='feed-grass[kcal]')

    # Land demand (crop and pasture)

    # Pasture demand [ha]
    # 
    # Objective: the node is computing the demand for pasture lands based on the livestock population, and grazing intensity (climate-smart-livestock lever)
    # 
    # 1. The overall grass requierement is summed [kcal]
    # 2. The livestock population [lsu] is divided by the livestock grazing intensity [kcal/ha]
    # 
    # Main inputs: 
    # - Grazing feed requierement [kcal] 
    # - Farming practices [lever; %]
    # - Pasturland/livestock grazing intensity [kcal/ha]
    # 
    # Main outputs: 
    # - Pastureland demand [ha]

    # Group by Country, Years (SUM)
    feed_grass_kcal = group_by_dimensions(df=feed_grass_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Apply climate smart livestock grazing intensity levers (improve)
    # => determine the kcal of livestock feed / ha in a pasture

    # OTS/FTS smart-livestock-grazing-intensity [kcal/ha]
    smart_livestock_grazing_intensity_kcal_per_ha = import_data(trigram='agr', variable_name='smart-livestock-grazing-intensity')
    # land-management[ha] = feed-grass[kcal] (total) / smart-livestock-grazing-intensity[kcal/ha]
    land_management_ha = mcd(input_table_1=feed_grass_kcal, input_table_2=smart_livestock_grazing_intensity_kcal_per_ha, operation_selection='x / y', output_name='land-management[ha]')

    # Apply climate smart livestock levers (switch)
    # => determine the % of feed requirements furnished by alternative protein

    # OTS/FTS alt-protein [%]
    alt_protein_percent = import_data(trigram='agr', variable_name='alt-protein')
    # feed-aps[kcal] = feed-requierement[kcal] * alt-protein[%]
    feed_aps_kcal = mcd(input_table_1=feed_requierement_kcal, input_table_2=alt_protein_percent, operation_selection='x * y', output_name='feed-aps[kcal]')
    # feed-aps [kcal]
    feed_aps_kcal = export_variable(input_table=feed_aps_kcal, selected_variable='feed-aps[kcal]')

    # By-product of alternative proteins
    # 
    # Objective: the node is computing the demand for alternative protein sources for livestock, inclusing microalgae and insects based feed
    # 
    # 1. Merging demand for algae & insect meals [kcal]
    # 2. Biorefinery node: Computing demand for crops/feedstock and supplies of byproducts (using conversion factors, yields)
    # 3. Overall oil production as byproducts of APS [kcal]
    # 
    # 
    # Main inputs: 
    # - Alternative protein feed requirements [kcal]
    # - By-product ratio [%]
    # 
    # Main outputs: 
    # - Alternative protein source Byproduct supplies [kcal]

    # Group by  Country, Years,  alternative-protein-type (SUM)
    feed_aps_kcal_2 = group_by_dimensions(df=feed_aps_kcal, groupby_dimensions=['Country', 'Years', 'alternative-protein-type'], aggregation_method='Sum')
    # RCP alt-protein-by-product-ratio [%]
    alt_protein_by_product_ratio_percent = import_data(trigram='agr', variable_name='alt-protein-by-product-ratio', variable_type='RCP')
    # alt-protein-by-product[kcal] = feed-aps[kcal] * alt-protein-by-product-ratio[%]
    alt_protein_by_product_kcal = mcd(input_table_1=feed_aps_kcal_2, input_table_2=alt_protein_by_product_ratio_percent, operation_selection='x * y', output_name='alt-protein-by-product[kcal]')
    # alt-protein-by-product [kcal]
    alt_protein_by_product_kcal = export_variable(input_table=alt_protein_by_product_kcal, selected_variable='alt-protein-by-product[kcal]')

    # Crop production demand

    # From unprocessed/processed feed/food & others to crop demand 
    # 
    # Objective: the node is computing the crop demand and the final domestic production
    # 1. From processed feed to crop demand [kcal] 
    # 2. From processed food to crop demand [kcal] 
    # 
    # 
    # Main inputs: 
    # 
    # - Domestic production (without losses) [kcal] 
    # - Byproducts [kcal] 
    # - Feed and food demand [kcal]
    # - Energy crop demand [kcal]
    # 
    # Main outputs: 
    # - Crop demand for processed food/feed [kcal]
    # - Domestic production accouting for waste for food and bioenergy [kcal]

    # Crop demand for alternative proteins

    # alt-protein-by-product [kcal]
    alt_protein_by_product_kcal = use_variable(input_table=alt_protein_by_product_kcal, selected_variable='alt-protein-by-product[kcal]')
    # Keep  origin = crop
    alt_protein_by_product_kcal = alt_protein_by_product_kcal.loc[alt_protein_by_product_kcal['origin'].isin(['crop'])].copy()
    # Group by  Country, Years,  raw-material (sum)
    alt_protein_by_product_kcal = group_by_dimensions(df=alt_protein_by_product_kcal, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # alt-protein-by-product =>  domestic-crop-production[kcal]
    out_8909_1 = alt_protein_by_product_kcal.rename(columns={'alt-protein-by-product[kcal]': 'domestic-crop-production[kcal]'})
    # Group by Country, Years (SUM)
    feed_aps_kcal = group_by_dimensions(df=feed_aps_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # sum[kcal] = feed-aps[kcal] + feed-grass[kcal]
    sum_kcal = mcd(input_table_1=feed_grass_kcal, input_table_2=feed_aps_kcal, operation_selection='x + y', output_name='sum[kcal]')

    # From Meat Demand to Livestock Population 
    # 
    # Objective: the node is turning the domestic meat demand into a livestock demand, and then considers the slaughting rate to compute and evaulate the cattle population by livestock type. The slaughting ratio are also used to compute the production of animal by-products as well as the feed-demand per animal type.
    # 
    # Note : slaught-rate can be > 100% meanings we grow more animals during one year that the amount living at the same time during this year.
    # For example : we can grow 6 sheeps during 6 months and 6 other sheeps during the 6 next months => in total we have 12 (6+6) slaughtered sheeps but we only use a growing place of 6 lsu (only 6 at the same time in a place) 
    # 
    # 1. From meat demand to livestock population being slaughtered
    # 2. From livestock population being slaughtered to the total livestock population 
    # 
    # 
    # Main inputs: 
    # 
    # 1. Meat demand [kcal] (= domestic production livestock afw)
    # 2. The slaughter rate [%]
    # 3. Livestock yields [kcal/lsu]
    # 
    # Main outputs: 
    # 1. Livestock population per group [lsu]
    # 2.  Livestock population per group being slaughtered [lsu] ??

    # Apply climate smart livestock levers (improve)
    # => determine the amount of kcal by livestock unit

    # OTS/FTS smart-yield-livestock
    smart_yield_livestock = import_data(trigram='agr', variable_name='smart-yield-livestock')
    # slaughtered-population[lsu] =  domestic-production-afw[kcal] / smart-yield-livestock[kcal/lsu]
    slaughtered_population_lsu = mcd(input_table_1=domestic_production_afw_kcal, input_table_2=smart_yield_livestock, operation_selection='x / y', output_name='slaughtered-population[lsu]')
    # Set to 0 (when / by 0)
    slaughtered_population_lsu = missing_value(df=slaughtered_population_lsu, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # Apply climate smart livestock levers (improve ?)
    # => determine the share of the animal population that are slaughtered each year

    # OTS/FTS smart-livestock-slaughtered [%]
    smart_livestock_slaughtered_percent = import_data(trigram='agr', variable_name='smart-livestock-slaughtered')
    # livestock-population[lsu] =  slaughtered-population[lsu] /  smart-livestock-slaughtered[%]  LEFT OUTER JOIN if missing value = 1 (meaning 100% of the population is slaughtered each year ?!)
    livestock_population_lsu_2 = mcd(input_table_1=slaughtered_population_lsu, input_table_2=smart_livestock_slaughtered_percent, operation_selection='x / y', output_name='livestock-population[lsu]', fill_value_bool='Left [x] Outer Join', fill_value=1.0)
    # cal livestock-population [lsu]
    livestock_population_lsu = import_data(trigram='agr', variable_name='livestock-population', variable_type='Calibration')

    # Calibration
    # Livestock population

    # Apply Calibration on livestock-population
    livestock_population_lsu, _, out_9208_3 = calibration(input_table=livestock_population_lsu_2, cal_table=livestock_population_lsu, data_to_be_cal='livestock-population[lsu]', data_cal='livestock-population[lsu]')

    # Cal_rate for livestock-population[lsu]

    # cal_rate for livestock-population[lsu]
    cal_rate_livestock_population_lsu = use_variable(input_table=out_9208_3, selected_variable='cal_rate_livestock-population[lsu]')
    out_9426_1 = pd.concat([out_9433_1, cal_rate_livestock_population_lsu])
    # livestock-population [lsu]
    livestock_population_lsu = export_variable(input_table=livestock_population_lsu, selected_variable='livestock-population[lsu]')
    # livestock-population [lsu]
    livestock_population_lsu_2 = use_variable(input_table=livestock_population_lsu, selected_variable='livestock-population[lsu]')

    # For : Water
    #  - Livestock population

    # Group by  Country, Years, product (sum)
    livestock_population_lsu_3 = group_by_dimensions(df=livestock_population_lsu_2, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')

    # Biomass coming from livestock residues (= manure)

    # OTS (only) manure-production [tonnes/LSU]
    manure_production_tonnes_per_LSU = import_data(trigram='agr', variable_name='manure-production', variable_type='OTS (only)')
    # Same as last available year
    manure_production_tonnes_per_LSU = add_missing_years(df_data=manure_production_tonnes_per_LSU)
    # manure-production[t] = population[lsu] * manure-production[t/lsu]
    manure_production_t = mcd(input_table_1=livestock_population_lsu_2, input_table_2=manure_production_tonnes_per_LSU, operation_selection='x * y', output_name='manure-production[t]')
    # Group by  Country, Years, product (sum)
    manure_production_t = group_by_dimensions(df=manure_production_t, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # RCP livestock-residues-bioenergy-conv-factors [TWh/ton]
    livestock_residues_bioenergy_conv_factors_TWh_per_ton = import_data(trigram='agr', variable_name='livestock-residues-bioenergy-conv-factors', variable_type='RCP')
    # potential-energy-production[TWh] = manure-production[t] * livestock-residues-bioenergy-conv-factors [TWh/ton]
    potential_energy_production_TWh = mcd(input_table_1=manure_production_t, input_table_2=livestock_residues_bioenergy_conv_factors_TWh_per_ton, operation_selection='x * y', output_name='potential-energy-production[TWh]')

    # Apply biomass supply valorisation of co-products and co levers (switch)
    # => determine % of manure which are effectively used to produce biogaz

    # OTS/FTS manure-bioenergy-share [%]
    manure_bioenergy_share_percent = import_data(trigram='agr', variable_name='manure-bioenergy-share')
    # energy-production[TWh] = potentiel-energy-production[TWh] * manure-bioenergy-share [%]
    energy_production_TWh_2 = mcd(input_table_1=potential_energy_production_TWh, input_table_2=manure_bioenergy_share_percent, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_2 = group_by_dimensions(df=energy_production_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = livestock-residues
    energy_production_TWh_2['origin'] = "livestock-residues"
    energy_production_TWh_2 = pd.concat([energy_production_TWh_2, energy_production_TWh])
    livestock_population_lsu = livestock_population_lsu_2.loc[~livestock_population_lsu_2['product'].isin(['abp-dairy-milk', 'abp-hens-egg'])].copy()

    # Emissions

    # 3.A Enteric fermentation emissions / 3.B. Manure management
    # 
    # Objective: the nodes are computing the GHG emissions linked to enteric fermentation (3A) and manure management (3B)
    # 
    # 1. Computing enteric emissions (N2O) and manure treatement (N2O/CH4) from the livestock population per livestock type [Mt gaes]
    # 
    # Main inputs: 
    # 
    # - Livestock population [lsu]
    # - Livestock emission factors [MtCH4/lsu and MtN2O/lsu]
    # 
    # Main outputs: 
    # - Enteric and manure emission [MtCH4 and MtN2O/lsu]

    # Enteric emissions and Treated manure emissions
    # Note : should not include dairy-milk to avoid double counting with meat-bovine => Set quality check to be sure it is the case ?

    # Apply climate smart livestock levers (switch)
    # => determine the quantity of CH4 emissions linked to livestock (linked to their living activity)

    # OTS/FTS livestock-emission-factor [Mt/lsu]
    livestock_emission_factor_Mt_per_lsu = import_data(trigram='agr', variable_name='livestock-emission-factor')
    # emissions[Mt] = population[lsu] * livestock-emission-factor[Mt/lsu]
    emissions_Mt = mcd(input_table_1=livestock_population_lsu, input_table_2=livestock_emission_factor_Mt_per_lsu, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years,  gaes, emission-type
    emissions_Mt = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years', 'emission-type', 'gaes'], aggregation_method='Sum')

    # Formating data for KPI's

    # For : KPIs (Pathway Explorer)
    #  - GHG emission per unit of food production (cropland + pasture)
    # 
    # Note : missing Lulucf 4B/4C emissions !
    # Note : missing 3E emissions ! (not modelised)

    # domestic-production [kcal] (livestock)
    domestic_production_kcal_2 = use_variable(input_table=domestic_production_kcal, selected_variable='domestic-production[kcal]')
    # Group by Country, Years (sum)
    domestic_production_kcal_2 = group_by_dimensions(df=domestic_production_kcal_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Crop demand for processed human food & livestock feed

    # RCP processing-yield [%]
    processing_yield_percent = import_data(trigram='agr', variable_name='processing-yield', variable_type='RCP')
    # crop-demand[kcal] (food) =  domestic-production[kcal] / processing-yield[%]
    crop_demand_kcal = mcd(input_table_1=domestic_production_kcal, input_table_2=processing_yield_percent, operation_selection='x / y', output_name='crop-demand[kcal]')
    # Group by  Country, Years,  raw-material (sum)
    crop_demand_kcal = group_by_dimensions(df=crop_demand_kcal, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')

    # From food demand to food industry demand
    # 
    # Objective: to compute the demand of food [kt] for industry
    # 
    # Calculation tree: 
    # 1. Divide the production by convertor factor (kcal => tons of food)
    # 2. Sum all food demand (tons)
    # 
    # Main inputs (from Lifestyles): 
    # - Domestic production  [kcal] 
    # - Conversion factor [kcal/t]
    # 
    # Main outputs: 
    # - Domestic food demand for industry [t]

    # RCP food-conv-factors [kcal/t]
    food_conv_factors_kcal_per_t = import_data(trigram='agr', variable_name='food-conv-factors', variable_type='RCP')
    # food-demand[t] = domestic-production[kcal] / food-conv-factors[kcal/t]
    food_demand_t = mcd(input_table_1=domestic_production_kcal, input_table_2=food_conv_factors_kcal_per_t, operation_selection='x / y', output_name='food-demand[t]')
    # Group by Country, Years (sum)
    food_demand_t = group_by_dimensions(df=food_demand_t, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # food-demand [t]
    food_demand_t = export_variable(input_table=food_demand_t, selected_variable='food-demand[t]')

    # For : Ammonia
    #  - Food demand

    # food-demand [t]
    food_demand_t = use_variable(input_table=food_demand_t, selected_variable='food-demand[t]')

    # By-products of alcoholic beverages 
    # 
    # Objective: to compute the demand for crops and the byproducts supply from the alcoholic beverage industry. 
    # 
    # 1.Computing demand for crops and supplies of by-products (using conversion factors, yields) 
    # 
    # 
    # Main inputs: 
    # 
    # - Alchoholic beverages demand [kcal] 
    # - Production yields (beverage by-products ratio)
    # 
    # Main outputs: 
    # - By-product demand for beverage [kcal]

    # RCP beverage-by-product-ratio [%]
    beverage_by_product_ratio_percent = import_data(trigram='agr', variable_name='beverage-by-product-ratio', variable_type='RCP')
    # beverage-by-product[kcal] = domestic-production[kcal] * beverage-by-product-ratio[%]
    beverage_by_product_kcal = mcd(input_table_1=domestic_production_kcal, input_table_2=beverage_by_product_ratio_percent, operation_selection='x * y', output_name='beverage-by-product[kcal]')
    # beverage-by-product [kcal]
    beverage_by_product_kcal = export_variable(input_table=beverage_by_product_kcal, selected_variable='beverage-by-product[kcal]')

    # Animal feed demand : from beverage industry-by-products cereals
    # 
    # Objective: the node is computing the production of animal feeds linked to by-products linked to beverage

    # beverage-by-product [kcal]
    beverage_by_product_kcal = use_variable(input_table=beverage_by_product_kcal, selected_variable='beverage-by-product[kcal]')
    # keep  raw-material =  cereals, yeast
    beverage_by_product_kcal_2 = beverage_by_product_kcal.loc[beverage_by_product_kcal['raw-material'].isin(['cereal', 'yeast'])].copy()
    # keep  product =  beer
    beverage_by_product_kcal_2 = beverage_by_product_kcal_2.loc[beverage_by_product_kcal_2['product'].isin(['beer'])].copy()
    # keep  origin =  fdk
    beverage_by_product_kcal_2 = beverage_by_product_kcal_2.loc[beverage_by_product_kcal_2['origin'].isin(['fdk'])].copy()
    # Group by  Country, Years (Sum) => Cereals & yeast
    beverage_by_product_kcal_2 = group_by_dimensions(df=beverage_by_product_kcal_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # alternative-feed-ration[kcal] = sum[kcal] + beverage-by-product[kcal] (yeast and cereals from  feedstock beer only)
    alternative_feed_ration_kcal = mcd(input_table_1=beverage_by_product_kcal_2, input_table_2=sum_kcal, operation_selection='x + y', output_name='alternative-feed-ration[kcal]')
    # remaining-feed-demand[kcal] = feed-requierement[kcal] - alternative-feed-ration[kcal]
    remaining_feed_demand_kcal = mcd(input_table_1=feed_requierement_kcal_2, input_table_2=alternative_feed_ration_kcal, operation_selection='x - y', output_name='remaining-feed-demand[kcal]')
    # OTS (only) feed-share [%]
    feed_share_percent = import_data(trigram='agr', variable_name='feed-share', variable_type='OTS (only)')
    # Same as last available year
    feed_share_percent = add_missing_years(df_data=feed_share_percent)
    # remaining-feed-demand[kcal] (replace)  = remaining-feed-demand[kcal] * feed-share[%]
    remaining_feed_demand_kcal_2 = mcd(input_table_1=remaining_feed_demand_kcal, input_table_2=feed_share_percent, operation_selection='x * y', output_name='remaining-feed-demand[kcal]', fill_value_bool='Inner Join')
    # cal remaining-feed-demand [kcal]
    remaining_feed_demand_kcal = import_data(trigram='agr', variable_name='remaining-feed-demand', variable_type='Calibration')

    # calibration
    # Based on remaining feed demand
    # (= feed requirements other than feed coming from beverage by-product, grazing and alternative proteins)

    # Apply Calibration on remaining feed demand
    remaining_feed_demand_kcal, _, out_9209_3 = calibration(input_table=remaining_feed_demand_kcal_2, cal_table=remaining_feed_demand_kcal, data_to_be_cal='remaining-feed-demand[kcal]', data_cal='remaining-feed-demand[kcal]')

    # Cal_rate for remaining-feed-demand[kcal]

    # cal_rate for remaining-feed-demand[kcal]
    cal_rate_remaining_feed_demand_kcal = use_variable(input_table=out_9209_3, selected_variable='cal_rate_remaining-feed-demand[kcal]')
    out_9428_1 = pd.concat([out_9426_1, cal_rate_remaining_feed_demand_kcal])
    # remaining-feed-demand [kcal]
    remaining_feed_demand_kcal = export_variable(input_table=remaining_feed_demand_kcal, selected_variable='remaining-feed-demand[kcal]')
    # remaining-feed-demand [kcal]
    remaining_feed_demand_kcal = use_variable(input_table=remaining_feed_demand_kcal, selected_variable='remaining-feed-demand[kcal]')

    # Crop demand for bioenergy (biogas, liquid-bio)
    # (Note : crop-demand for bio-solid energy-carrier is defined in ha and not kcal => so we add it furter in this model)

    # Apply biomass supply dedicated crops levers (avoid ?)
    # => determine the kcal demand by raw material for bioenergy production

    # OTS/FTS crops-consumption-bioenergy [kcal]
    crops_consumption_bioenergy_kcal = import_data(trigram='agr', variable_name='crops-consumption-bioenergy')

    # Domestic crop production
    # 
    # Objective: the node is computing the  domestic crop production given the net-import setting, and the food, feed and non-food consumption.
    # 
    # Main inputs: 
    # 
    # 1. Merging the crop demand (food, feed, non-food) [kcal]
    # 2. Computing the crop domestic-production [kcal] given the net-import setting [%]
    # 3. Computing the crop domestic-production [kcal] while accounting for losses [production losses, kcal]
    # 
    # Main outputs: 
    # - Domestic production by food group [kcal]

    # Crop other than bioenergy and alternative-proteins

    # Group by  Country, Years,  raw-material (sum)
    crops_consumption_bioenergy_kcal_2 = group_by_dimensions(df=crops_consumption_bioenergy_kcal, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')

    # Biomass coming from crops products

    # crops-consumption-bioenergy [kcal]
    crops_consumption_bioenergy_kcal = use_variable(input_table=crops_consumption_bioenergy_kcal, selected_variable='crops-consumption-bioenergy[kcal]')
    # RCP crop-product-bioenergy-conv-factors [TWh/kcal]
    crop_product_bioenergy_conv_factors_TWh_per_kcal = import_data(trigram='agr', variable_name='crop-product-bioenergy-conv-factors', variable_type='RCP')
    # energy-production[TWh] = crops-consumption-bioenergy [kcal] * crop-product-bioenergy-conv-factors [TWh/kcal]
    energy_production_TWh = mcd(input_table_1=crops_consumption_bioenergy_kcal, input_table_2=crop_product_bioenergy_conv_factors_TWh_per_kcal, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = crops-product
    energy_production_TWh['origin'] = "crops-product"
    # Keep  treatement = unprocessed
    remaining_feed_demand_kcal_2 = remaining_feed_demand_kcal.loc[remaining_feed_demand_kcal['treatment'].isin(['unprocessed'])].copy()
    # Keep  origin = crop
    remaining_feed_demand_kcal_2 = remaining_feed_demand_kcal_2.loc[remaining_feed_demand_kcal_2['origin'].isin(['crop'])].copy()

    # Crop demand for unprocessed livestock feed

    # Group by  Country, Years,  product (sum)
    remaining_feed_demand_kcal_2 = group_by_dimensions(df=remaining_feed_demand_kcal_2, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # product => raw-material remaining-feed-demand => crop-demand
    out_8899_1 = remaining_feed_demand_kcal_2.rename(columns={'product': 'raw-material', 'remaining-feed-demand[kcal]': 'crop-demand[kcal]'})
    # crop-demand[kcal] (feed) =  remaining-feed-demand[kcal] / processing-yield[%]
    crop_demand_kcal_2 = mcd(input_table_1=remaining_feed_demand_kcal, input_table_2=processing_yield_percent, operation_selection='x / y', output_name='crop-demand[kcal]')
    # Group by  Country, Years,  raw-material (sum)
    crop_demand_kcal_2 = group_by_dimensions(df=crop_demand_kcal_2, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    crop_demand_kcal = pd.concat([crop_demand_kcal, crop_demand_kcal_2])
    crop_demand_kcal = crop_demand_kcal.loc[~crop_demand_kcal['raw-material'].isin(['oil-crop'])].copy()
    out_8894_1 = pd.concat([crop_demand_kcal, out_8899_1])
    # Keep  origin = crop
    beverage_by_product_kcal = beverage_by_product_kcal.loc[beverage_by_product_kcal['origin'].isin(['crop'])].copy()

    # Crop demand for unprocessed beverage by-products

    # Group by  Country, Years,  raw-material (sum)
    beverage_by_product_kcal = group_by_dimensions(df=beverage_by_product_kcal, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # beverage-by-product => crop-demand
    out_8891_1 = beverage_by_product_kcal.rename(columns={'beverage-by-product[kcal]': 'crop-demand[kcal]'})

    # Crop demand for unprocessed human food

    # overall-food-demand [kcal]
    overall_food_demand_kcal = use_variable(input_table=overall_food_demand_kcal, selected_variable='overall-food-demand[kcal]')
    # Keep  treatement = unprocessed
    overall_food_demand_kcal = overall_food_demand_kcal.loc[overall_food_demand_kcal['treatment'].isin(['unprocessed'])].copy()
    # Keep  category = crop
    overall_food_demand_kcal = overall_food_demand_kcal.loc[overall_food_demand_kcal['category'].isin(['crop'])].copy()
    # Group by  Country, Years,  product (sum)
    overall_food_demand_kcal = group_by_dimensions(df=overall_food_demand_kcal, groupby_dimensions=['Country', 'Years', 'product'], aggregation_method='Sum')
    # product => raw-material ovaerall-food-demand => crop-demand
    out_8890_1 = overall_food_demand_kcal.rename(columns={'product': 'raw-material', 'overall-food-demand[kcal]': 'crop-demand[kcal]'})
    out_1 = pd.concat([out_8890_1, out_8891_1])
    out_1 = pd.concat([out_1, out_8894_1])
    # Group by  Country, Years,  raw-material (sum)
    out_1 = group_by_dimensions(df=out_1, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # crop-demand[kcal] (replace) = crop-demand[kcal] + crops-consumption-bioenergy [kcal]  LEFT JOIN => If missing set 0 (no additional crop demand for this raw-material)
    crop_demand_kcal_2 = mcd(input_table_1=out_1, input_table_2=crops_consumption_bioenergy_kcal_2, operation_selection='x + y', output_name='crop-demand[kcal]')
    # cal crop-demand [kcal]
    crop_demand_kcal = import_data(trigram='agr', variable_name='crop-demand', variable_type='Calibration')

    # calibration
    # Crop activity

    # Apply Calibration on crop demand
    crop_demand_kcal, _, _ = calibration(input_table=crop_demand_kcal_2, cal_table=crop_demand_kcal, data_to_be_cal='crop-demand[kcal]', data_cal='crop-demand[kcal]')

    # Apply food net import levers (avoid ?)
    # => determine the % of crops required to reach the crop demand accounting for the crop losses

    # OTS/FTS crop-losses [%]
    crop_losses_percent = import_data(trigram='agr', variable_name='crop-losses')
    # domestic-crop-demand [kcal] = domestic-demand[kcal] * crop-losses[%]
    domestic_crop_demand_kcal = mcd(input_table_1=crop_demand_kcal, input_table_2=crop_losses_percent, operation_selection='x * y', output_name='domestic-crop-demand[kcal]')

    # Apply food net import levers (avoid ?)
    # => determine the % of food/feed crops demand that is imported

    # OTS/FTS food-net-import [kcal]
    food_net_import_kcal = import_data(trigram='agr', variable_name='food-net-import')

    # For : Scope 2/3
    #  - Food import

    # food-net-import [kcal]
    food_net_import_kcal_2 = use_variable(input_table=food_net_import_kcal, selected_variable='food-net-import[kcal]')
    # include positive  values
    out_9596_1 = row_filter(df=food_net_import_kcal_2, filter_type='RangeVal_RowFilter', that_column='food-net-import[kcal]', include=True, lower_bound_bool=True, upper_bound_bool=False, lower_bound='0.0', upper_bound=0)
    # Set to 0 (no need to account for export)
    food_net_import_kcal_3 = out_9596_1.assign(**{'food-net-import[kcal]': 0.0})
    # exclude positive  values
    out_9597_1 = row_filter(df=food_net_import_kcal_2, filter_type='RangeVal_RowFilter', that_column='food-net-import[kcal]', include=False, lower_bound_bool=True, upper_bound_bool=False, lower_bound='0.0', upper_bound=0)
    # * -1 (convert in positive values)
    food_net_import_kcal_2 = out_9597_1.assign(**{'food-net-import[kcal]': out_9597_1['food-net-import[kcal]']*(-1.0)})
    food_net_import_kcal_2 = pd.concat([food_net_import_kcal_3, food_net_import_kcal_2])
    # Group by  Country, Years, raw-material (sum)
    food_net_import_kcal_2 = group_by_dimensions(df=food_net_import_kcal_2, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    food_net_import_kcal_2 = pd.concat([food_net_import_kcal_2, food_net_import_product_kcal_2])
    # Module = Scope 2/3
    food_net_import_kcal_2 = column_filter(df=food_net_import_kcal_2, pattern='^.*$')
    # domestic-crop-production[kcal] = domestic-crop-demand[kcal] + food-net-import[kcal]
    domestic_crop_production_kcal = mcd(input_table_1=domestic_crop_demand_kcal, input_table_2=food_net_import_kcal, operation_selection='x + y', output_name='domestic-crop-production[kcal]')
    # If < 0 Set 0 (no net production)
    mask = domestic_crop_production_kcal['domestic-crop-production[kcal]']<0
    domestic_crop_production_kcal.loc[mask, 'domestic-crop-production[kcal]'] = 0
    domestic_crop_production_kcal.loc[~mask, 'domestic-crop-production[kcal]'] = domestic_crop_production_kcal.loc[~mask, 'domestic-crop-production[kcal]']
    # Group by  Country, Years,  raw-material (sum)
    domestic_crop_production_kcal = group_by_dimensions(df=domestic_crop_production_kcal, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # add crops for alt-proteins
    out_8197_1 = pd.concat([domestic_crop_production_kcal, out_8909_1])
    # cal domestic-crop-production [kcal]
    domestic_crop_production_kcal = import_data(trigram='agr', variable_name='domestic-crop-production', variable_type='Calibration')

    # calibration
    # Crop activity

    # Apply Calibration on crop production
    out_8197_1, _, out_9210_3 = calibration(input_table=out_8197_1, cal_table=domestic_crop_production_kcal, data_to_be_cal='domestic-crop-production[kcal]', data_cal='domestic-crop-production[kcal]')

    # Cal_rate for domestic-crop-production[kcal]

    # cal_rate for domestic-crop-production[kcal]
    cal_rate_domestic_crop_production_kcal = use_variable(input_table=out_9210_3, selected_variable='cal_rate_domestic-crop-production[kcal]')
    out_9430_1 = pd.concat([out_9428_1, cal_rate_domestic_crop_production_kcal])
    # domestic-crop-production [kcal]   (all crops demand + calibrated)
    domestic_crop_production_kcal = export_variable(input_table=out_8197_1, selected_variable='domestic-crop-production[kcal]')
    # domestic-crop-production [kcal]
    domestic_crop_production_kcal = use_variable(input_table=domestic_crop_production_kcal, selected_variable='domestic-crop-production[kcal]')

    # For : Water
    #  - Raw material demand

    # domestic-crop-production [kcal]
    domestic_crop_production_kcal_2 = use_variable(input_table=domestic_crop_production_kcal, selected_variable='domestic-crop-production[kcal]')
    out_9579_1 = pd.concat([domestic_crop_production_kcal_2, livestock_population_lsu_3])
    # Module = Water
    out_9579_1 = column_filter(df=out_9579_1, pattern='^.*$')
    # Group by Country, Years (sum)
    domestic_crop_production_kcal_3 = group_by_dimensions(df=domestic_crop_production_kcal, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # total-domestic-production[kcal] = domestic-production[kcal] + domestic-crop-production[kcal]
    total_domestic_production_kcal = mcd(input_table_1=domestic_crop_production_kcal_3, input_table_2=domestic_production_kcal_2, operation_selection='x + y', output_name='total-domestic-production[kcal]')

    # 3.F. Field burning of agricultural residues​
    # 
    # This category represents the CH4 and N2O emissions that occur during the field burning of agricultural residues.
    # For the moment, only cereals production are modelised
    # => We should add other type of residues in the model
    # => To fix this, we calibrate residues burnt (cereal crop) with all type of residues burnt (3F total)​​
    # 
    # Input : 
    # - Cereal production [kcal]
    # - Residues burnt [Mt/kcal]
    # - burnt-residues-emission-factor[Mt/Mt]
    # 
    # Output :
    # CH4 / N2O brunt-residues emission [Mt]

    # OTS (only) residues-burnt [Mt/kcal]
    residues_burnt_Mt_per_kcal = import_data(trigram='agr', variable_name='residues-burnt', variable_type='OTS (only)')
    # Same as last available year
    residues_burnt_Mt_per_kcal = add_missing_years(df_data=residues_burnt_Mt_per_kcal)
    # burned-residues[Mt] = residues-burnt[Mt/kcal] * domestic-crop-production[kcal]
    burned_residues_Mt = mcd(input_table_1=domestic_crop_production_kcal_2, input_table_2=residues_burnt_Mt_per_kcal, operation_selection='x * y', output_name='burned-residues[Mt]')
    # OTS (only) burnt-residues-emission-factor [Mt/Mt]
    burnt_residues_emission_factor_Mt_per_Mt = import_data(trigram='agr', variable_name='burnt-residues-emission-factor', variable_type='OTS (only)')
    # Same as last available year
    burnt_residues_emission_factor_Mt_per_Mt = add_missing_years(df_data=burnt_residues_emission_factor_Mt_per_Mt)
    # emissions[Mt] = burned-residues[Mt] * burnt-residues-emission-factor [Mt/Mt]
    emissions_Mt_2 = mcd(input_table_1=burned_residues_Mt, input_table_2=burnt_residues_emission_factor_Mt_per_Mt, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, amendment-type, gaes, raw-material, emission-type
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt_2, groupby_dimensions=['Country', 'Years', 'raw-material', 'amendment-type', 'emission-type', 'gaes'], aggregation_method='Sum')

    # 3.D. Agricultural soils
    # 
    # Objective:
    # This category represents the N2O emissions due to N fertilizers applications on agricultural soils.
    # In the model we consider 4 different N inputs: 
    # - N coming from manure applied on agricultural soils,
    # - N coming from urine and dung left on pasture
    # - N coming from crop residues left on fields
    # - N coming from all the other sources (mainly inorganic fertilizers)
    # Calibration is done separately for these 4 categories.

    # Emissions : 
    # - N2O due to residues applied on agricultural soils
    # 
    # Logic : 
    # 1. Compute residues left on soil, and so the amount of N left on soil
    # 2. Define emissions linked to N left on soil
    # 
    # Main inputs: 
    # 
    # - Crop production [kcal]
    # - Crop residue used [%] (correspond to % of residues that are left on soil) 
    # - Residue N content MtN / Mt]
    # - Crop residue emission factor [Mt gaes / Mt N]
    # 
    # Main outputs: 
    # - Residues left on soil [Mt]
    # - N left on soil [MtN]
    # - Residues emission [MtN2O]

    # Apply biomass hierarchy levers (switch ?)
    # => determine the use of crop residues

    # OTS/FTS crop-residues-use [Mt/kcal]
    crop_residues_use_Mt_per_kcal = import_data(trigram='agr', variable_name='crop-residues-use')
    # residues-left-on-soil[Mt] = domestic-crop-production[kcal] * crop-residues-use[Mt/kcal]
    residues_left_on_soil_Mt = mcd(input_table_1=domestic_crop_production_kcal, input_table_2=crop_residues_use_Mt_per_kcal, operation_selection='x * y', output_name='residues-left-on-soil[Mt]')
    # OTS (only) residue-N-content [MtN/Mt]
    residue_N_content_MtN_per_Mt = import_data(trigram='agr', variable_name='residue-N-content', variable_type='OTS (only)')
    # Same as last available year
    residue_N_content_MtN_per_Mt = add_missing_years(df_data=residue_N_content_MtN_per_Mt)
    # N-left-on-soil[MtN] = residues-left-on-soil[Mt] * residue-N-content[MtN/Mt]
    N_left_on_soil_Mt = mcd(input_table_1=residues_left_on_soil_Mt, input_table_2=residue_N_content_MtN_per_Mt, operation_selection='x * y', output_name='N-left-on-soil[Mt]')

    # Cropland  
    # 
    # Objective: the node is computing the demand for cropland to supply crop demand.  
    # 
    # 1. Computing crop yields given the climate scenario [kcal/ha]
    # 2. Computing the crop yields given the land multi-use impact [kcal/ha]
    # 3. Computing cropland demand for each crop [ha]
    # 4. Summing up the cropland demand [ha]
    # 
    # Main inputs: 
    # 
    # - Overall crop demand by type [kcal] from energy and food production
    # - Crop yields [kcal/ha]
    # 
    # Main outputs: 
    # - Demand for cropland [ha]

    # Apply smart crop levers (reduce)
    # => determine crop yield (kcal/ha) leading to reducing the cropland surface if yield increases

    # OTS/FTS smart-crop-yield [kcal/ha]
    smart_crop_yield_kcal_per_ha = import_data(trigram='agr', variable_name='smart-crop-yield')
    # land-management[ha] = domestic-crop-production[kcal] / smart-crop-yield[kcal/ha]
    land_management_ha_2 = mcd(input_table_1=domestic_crop_production_kcal, input_table_2=smart_crop_yield_kcal_per_ha, operation_selection='x / y', output_name='land-management[ha]')
    # Set 0 if divide by 0
    land_management_ha_2 = missing_value(df=land_management_ha_2, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    # Calibration land-management-by-raw-material [ha]
    land_management_by_raw_material_ha = import_data(trigram='agr', variable_name='land-management-by-raw-material', variable_type='Calibration')

    # calibration
    # Crop land by raw material
    # We use this calibration to be sure the proportion of each raw-material is correct

    # Apply Calibration on land-management
    land_management_ha_2, _, _ = calibration(input_table=land_management_ha_2, cal_table=land_management_by_raw_material_ha, data_to_be_cal='land-management[ha]', data_cal='land-management-by-raw-material[ha]')
    # Group by  Country, Years, land-use (sum)
    land_management_ha_4 = group_by_dimensions(df=land_management_ha_2, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # cal land-management [ha]
    land_management_ha_3 = import_data(trigram='agr', variable_name='land-management', variable_type='Calibration')

    # calibration
    # Pasture land

    # Apply Calibration on land management
    land_management_ha, _, out_9213_3 = calibration(input_table=land_management_ha, cal_table=land_management_ha_3, data_to_be_cal='land-management[ha]', data_cal='land-management[ha]')

    # Cal_rate for land-management[ha]

    # cal_rate for land-management[ha] for pasture
    cal_rate_land_management_ha = use_variable(input_table=out_9213_3, selected_variable='cal_rate_land-management[ha]')

    # calibration
    # Crop land
    # We use this calibration to be sure the sum of all raw-material correspond to cropland

    # Apply Calibration on land-management
    land_management_ha_3, out_9212_2, out_9212_3 = calibration(input_table=land_management_ha_4, cal_table=land_management_ha_3, data_to_be_cal='land-management[ha]', data_cal='land-management[ha]')
    # Apply calibration factor on land-management[ha] (by raw material)
    land_management_ha_2 = mcd(input_table_1=land_management_ha_2, input_table_2=out_9212_2, operation_selection='x * y', output_name='land-management[ha]')
    # land-management[ha] to cropland-management[ha]
    out_9094_1 = land_management_ha_2.rename(columns={'land-management[ha]': 'cropland-management[ha]'})

    # 3.C. Rice cultivation
    # 
    # The nodes are computing the GHG emissions linked to rice crops.
    # Flooded rice fields emit CH4 when organic matter decompose in the water under anaerobic conditions.​
    # 
    # Objective : Calculate the CH4 emission due to rice cultivation
    # 
    # Input : 
    # - Rice cropland [ha]
    # - Emission factor of rice [Mt/ha]
    # 
    # Output :
    # CH4 crop rice emission [Mt]

    # land-management [ha]
    land_management_ha_2 = use_variable(input_table=land_management_ha_2, selected_variable='land-management[ha]')
    # OTS (only) rice-emission-factor [Mt/ha]
    rice_emission_factor_Mt_per_ha = import_data(trigram='agr', variable_name='rice-emission-factor', variable_type='OTS (only)')

    # Emissions : 
    # - N2O due to urine and dung left on pastures
    # - N2O / CH4 due to organic fertilizers applied on agricultural soils
    # 
    # Logic : 
    # 1. Computing the manure production per livestock type [MtN-equivalent]
    # 2. Setting the split between manure management practices per livestock type [%,MtN]
    # 3. Computing N2O emissions from manure per livestock type [MtN2O]
    # 4. Computing CH4 emissions fro mmanure per livestock type [MtCH4]
    # 
    # Main inputs: 
    # 
    # - Livestock population [lsu]
    # - Manure emission factors [MtN2O/lsu, MtCH4/lsu]
    # - Manure treatment split [%]
    # 
    # Main outputs: 
    # - Manure emission [MtCH4, MtN2O]

    # Apply climate smart livestock levers (reduce ?)
    # => determine the quantity of N produced by livestock (used as manure : applied on field or leave it on the pasture)

    # OTS/FTS N-excretion-rate [kgN/lsu/year]
    N_excretion_rate_kgN_per_lsu_per_year = import_data(trigram='agr', variable_name='N-excretion-rate')
    # N-manure-quantity[kgN] = livestock-population[lsu] * N-excretion-rate[kgN/lsu/year]
    N_manure_quantity_kgN = mcd(input_table_1=livestock_population_lsu, input_table_2=N_excretion_rate_kgN_per_lsu_per_year, operation_selection='x * y', output_name='N-manure-quantity[kgN]')
    # Group by Country, Years
    N_manure_quantity_kgN = group_by_dimensions(df=N_manure_quantity_kgN, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')

    # Apply climate smart livestock levers (switch)
    # => determine the quantity of N left on pasture and the one applied on fields

    # OTS/FTS manure-management-share [%]
    manure_management_share_percent = import_data(trigram='agr', variable_name='manure-management-share')
    # N-manure-quantity[kgN] (replace) = N-manure-quantity[kgN] * manure-management-share[%]
    N_manure_quantity_kgN = mcd(input_table_1=N_manure_quantity_kgN, input_table_2=manure_management_share_percent, operation_selection='x * y', output_name='N-manure-quantity[kgN]')

    # For : Air Quality
    #  - Energy demand
    #  - Amendement application
    #  - Livestock population
    #  - N-manure-quantity

    # N-manure-quantity [kgN]
    N_manure_quantity_kgN_2 = export_variable(input_table=N_manure_quantity_kgN, selected_variable='N-manure-quantity[kgN]')

    # Apply climate smart livestock levers (switch)
    # => determine the quantity of N left on pasture and the one applied on fields

    # OTS/FTS N-amendment-emission-factor [Mt/kgN]
    N_amendment_emission_factor_Mt_per_kgN = import_data(trigram='agr', variable_name='N-amendment-emission-factor')
    # Keep emission-type = 3D-agricultural-soils-residues
    N_amendment_emission_factor_Mt_per_kgN_2 = N_amendment_emission_factor_Mt_per_kgN.loc[N_amendment_emission_factor_Mt_per_kgN['emission-type'].isin(['3D-agricultural-soils-residues'])].copy()
    # Same as last available year
    N_amendment_emission_factor_Mt_per_kgN_2 = add_missing_years(df_data=N_amendment_emission_factor_Mt_per_kgN_2)
    # emissions[Mt] = N-left-on-soil[MtN] * left-residues-emission-factor[Mt/Mt]
    emissions_Mt_3 = mcd(input_table_1=N_amendment_emission_factor_Mt_per_kgN_2, input_table_2=N_left_on_soil_Mt, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, gaes, emission-type, amendment-type
    emissions_Mt_4 = group_by_dimensions(df=emissions_Mt_3, groupby_dimensions=['Country', 'Years', 'emission-type', 'gaes', 'amendment-type'], aggregation_method='Sum')
    # emissions[Mt] =  N-manure-quantity[kgN] * N-amendment-emission-factor[Mt/kgN]
    emissions_Mt_3 = mcd(input_table_1=N_manure_quantity_kgN, input_table_2=N_amendment_emission_factor_Mt_per_kgN, operation_selection='x * y', output_name='emissions[Mt]')
    # Same as last available year
    rice_emission_factor_Mt_per_ha = add_missing_years(df_data=rice_emission_factor_Mt_per_ha)
    # emissions[Mt] =  land-management[ha] * rice-emission-factor[Mt/ha]
    emissions_Mt_5 = mcd(input_table_1=land_management_ha_2, input_table_2=rice_emission_factor_Mt_per_ha, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, gaes, raw-material, emission-type
    emissions_Mt_5 = group_by_dimensions(df=emissions_Mt_5, groupby_dimensions=['Country', 'Years', 'raw-material', 'emission-type', 'gaes'], aggregation_method='Sum')

    # Biomass coming from dedicated double crops

    # land-management [ha] (detailled by raw-material)
    land_management_ha_4 = use_variable(input_table=land_management_ha_2, selected_variable='land-management[ha]')

    # Apply biomass supply dedicated crops levers (switch)
    # => determine % of surface with dedicated double crops

    # OTS / FTS dedicated-double-crops-share [%]
    dedicated_double_crops_share_percent = import_data(trigram='agr', variable_name='dedicated-double-crops-share')
    # dedicated-double-crops[ha] = land-management[ha] * dedicated-double-crops-share[%]
    dedicated_double_crops_ha = mcd(input_table_1=land_management_ha_4, input_table_2=dedicated_double_crops_share_percent, operation_selection='x * y', output_name='dedicated-double-crops[ha]')
    # OTS (only) dedicated-double-crops-yield [t/ha]
    dedicated_double_crops_yield_t_per_ha = import_data(trigram='agr', variable_name='dedicated-double-crops-yield', variable_type='OTS (only)')
    # Same as last available year
    dedicated_double_crops_yield_t_per_ha = add_missing_years(df_data=dedicated_double_crops_yield_t_per_ha)
    # potential-energy-crops[t] = dedicated-double-crops[ha] * dedicated-double-crops-yield [t/ha]
    potential_energy_crops_t = mcd(input_table_1=dedicated_double_crops_ha, input_table_2=dedicated_double_crops_yield_t_per_ha, operation_selection='x * y', output_name='potential-energy-crops[t]')
    # RCP dedicated-double-crops-conversion-factor [TWh/ton]
    dedicated_double_crops_conversion_factor_TWh_per_ton = import_data(trigram='agr', variable_name='dedicated-double-crops-conversion-factor', variable_type='RCP')
    # Group by  Country, Years, land-use (sum)
    potential_energy_crops_t = group_by_dimensions(df=potential_energy_crops_t, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # energy-production[TWh] = potentiel-energy-crops[t] * dedicated-double-crops-conversion-factor [TWh/ton]
    energy_production_TWh_3 = mcd(input_table_1=potential_energy_crops_t, input_table_2=dedicated_double_crops_conversion_factor_TWh_per_ton, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_3 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = dedicated-double-crops
    energy_production_TWh_3['origin'] = "dedicated-double-crops"
    energy_production_TWh_2 = pd.concat([energy_production_TWh_3, energy_production_TWh_2])

    # Biomass coming from crops co-products

    # OTS (only) co-products-yields [tonnes/ha]
    co_products_yields_tonnes_per_ha = import_data(trigram='agr', variable_name='co-products-yields', variable_type='OTS (only)')
    # Same as last available year
    co_products_yields_tonnes_per_ha = add_missing_years(df_data=co_products_yields_tonnes_per_ha)
    # co-products-production[t] = land-management[ha] * co-products-yields[tonnes/ha]
    co_products_production_t = mcd(input_table_1=land_management_ha_2, input_table_2=co_products_yields_tonnes_per_ha, operation_selection='x * y', output_name='co-products-production[t]')
    # Group by  Country, Years, raw-material (sum)
    co_products_production_t = group_by_dimensions(df=co_products_production_t, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # RCP co-product-bioenergy-conv-factors [TWh/ton]
    co_product_bioenergy_conv_factors_TWh_per_ton = import_data(trigram='agr', variable_name='co-product-bioenergy-conv-factors', variable_type='RCP')
    # potential-energy-production[TWh] = co-products-production[t] * co-product-bioenergy-conv-factors [TWh/ton]
    potential_energy_production_TWh = mcd(input_table_1=co_products_production_t, input_table_2=co_product_bioenergy_conv_factors_TWh_per_ton, operation_selection='x * y', output_name='potential-energy-production[TWh]')

    # Apply biomass supply valorisation of co-products and co levers (switch)
    # => determine % of crops residues which are effectively used to produce biogaz

    # OTS/FTS crops-co-products-bioenergy-share [%]
    crops_co_products_bioenergy_share_percent = import_data(trigram='agr', variable_name='crops-co-products-bioenergy-share')
    # energy-production[TWh] = potentiel-energy-production[TWh] * crops-co-products-bioenergy-share [%]
    energy_production_TWh_3 = mcd(input_table_1=potential_energy_production_TWh, input_table_2=crops_co_products_bioenergy_share_percent, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_3 = group_by_dimensions(df=energy_production_TWh_3, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = crops-co-product
    energy_production_TWh_3['origin'] = "crops-co-product"
    energy_production_TWh_2 = pd.concat([energy_production_TWh_3, energy_production_TWh_2])
    energy_production_TWh = pd.concat([energy_production_TWh, energy_production_TWh_2])
    # cal_rate for land-management[ha] for cropland
    cal_rate_land_management_ha_2 = use_variable(input_table=out_9212_3, selected_variable='cal_rate_land-management[ha]')
    cal_rate_land_management_ha = pd.concat([cal_rate_land_management_ha_2, cal_rate_land_management_ha])
    out_9434_1 = pd.concat([out_9430_1, cal_rate_land_management_ha])
    land_management_ha = pd.concat([land_management_ha_3, land_management_ha])
    # land-management [ha]
    land_management_ha = export_variable(input_table=land_management_ha, selected_variable='land-management[ha]')

    # For : KPIs (Pathway Explorer)
    #  - GHG emission per farmland
    # 
    # Note : missing Lulucf 4B and Lulucf 4C emissions !
    # Note : missing 3E emissions ! (not modelised)
    # Note : not considered : emissions linked to energy demand
    # Note : for grassland, KPI's underestimates emissions per ha as hectars of grassland are not all pastured

    # land-management [ha]
    land_management_ha = use_variable(input_table=land_management_ha, selected_variable='land-management[ha]')
    # Top : land-use = cropland Bottom : land-use = pasture
    land_management_ha_2 = land_management_ha.loc[land_management_ha['land-use'].isin(['cropland'])].copy()
    land_management_ha_excluded = land_management_ha.loc[~land_management_ha['land-use'].isin(['cropland'])].copy()

    # Apply cliamte smart crop levers (reduce)
    # => determine quantity of TWh by cropland management (TWh/ha)

    # OTS/FTS cropland-energy-consumption [TWh/ha]
    cropland_energy_consumption_TWh_per_ha = import_data(trigram='agr', variable_name='cropland-energy-consumption')
    # energy-demand[TWh] = land-management[ha] * cropland-energy-consumption[TWh/ha]
    energy_demand_TWh = mcd(input_table_1=land_management_ha, input_table_2=cropland_energy_consumption_TWh_per_ha, operation_selection='x * y', output_name='energy-demand[TWh]')
    # OTS (only) cropland-fuel-mix [%]
    cropland_fuel_mix_percent = import_data(trigram='agr', variable_name='cropland-fuel-mix', variable_type='OTS (only)')
    # Same as last available year
    cropland_fuel_mix_percent = add_missing_years(df_data=cropland_fuel_mix_percent)
    # energy-demand[TWh] (replace) =  energy-demand[TWh] * cropland-fuel-mix[%]
    energy_demand_TWh_2 = mcd(input_table_1=energy_demand_TWh, input_table_2=cropland_fuel_mix_percent, operation_selection='x * y', output_name='energy-demand[TWh]')

    # Apply cliamte smart crop levers (switch)
    # => determine which fuel is used for irrigation management energy demand (NOT ONLY FUEL MIX BUT ALSO % CROPLAND for which there is IRRIGATION ??)

    # OTS/FTS irrigation-fuel-mix [%]
    irrigation_fuel_mix_percent = import_data(trigram='agr', variable_name='irrigation-fuel-mix')
    # energy-demand-irrigation =  energy-demand * smart-crop-irrigation-energy-demand[%]
    irrigation_energy_demand_TWh = mcd(input_table_1=energy_demand_TWh, input_table_2=irrigation_fuel_mix_percent, operation_selection='x * y', output_name='irrigation-energy-demand[TWh]')
    # energy-demand[TWh] (replace) = energy-demand[TWh] + irrigation-energy-demand[TWh]  LEFT OUTER JOIN if no irrigation demand ; set to 0
    energy_demand_TWh = mcd(input_table_1=energy_demand_TWh_2, input_table_2=irrigation_energy_demand_TWh, operation_selection='x + y', output_name='energy-demand[TWh]', fill_value_bool='Left [x] Outer Join')
    # Remove energy-carrier = LPG
    energy_demand_TWh_excluded = energy_demand_TWh.loc[energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-lpg'])].copy()
    energy_demand_TWh = energy_demand_TWh.loc[~energy_demand_TWh['energy-carrier'].isin(['gaseous-ff-lpg'])].copy()
    # energy-carrier = gaseous-ff-natural
    energy_demand_TWh_excluded['energy-carrier'] = "gaseous-ff-natural"
    energy_demand_TWh = pd.concat([energy_demand_TWh, energy_demand_TWh_excluded])
    # Group by  Country, Years,  energy-carrier
    energy_demand_TWh = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # cal energy-demand [TWh]
    energy_demand_TWh_2 = import_data(trigram='agr', variable_name='energy-demand', variable_type='Calibration')
    # Remove energy-carrier = LPG
    energy_demand_TWh_excluded = energy_demand_TWh_2.loc[energy_demand_TWh_2['energy-carrier'].isin(['gaseous-ff-lpg'])].copy()
    energy_demand_TWh_2 = energy_demand_TWh_2.loc[~energy_demand_TWh_2['energy-carrier'].isin(['gaseous-ff-lpg'])].copy()
    # energy-carrier = gaseous-ff-natural
    energy_demand_TWh_excluded['energy-carrier'] = "gaseous-ff-natural"
    energy_demand_TWh_2 = pd.concat([energy_demand_TWh_2, energy_demand_TWh_excluded])
    # Group by  Country, Years,  energy-carrier
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')

    # calibration
    # Energy-demand

    # Apply Calibration on energy-demand
    energy_demand_TWh, _, out_9214_3 = calibration(input_table=energy_demand_TWh, cal_table=energy_demand_TWh_2, data_to_be_cal='energy-demand[TWh]', data_cal='energy-demand[TWh]')

    # Cal_rate for energy-demand[TWh]

    # cal_rate for energy-demand[TWh]
    cal_rate_energy_demand_TWh = use_variable(input_table=out_9214_3, selected_variable='cal_rate_energy-demand[TWh]')
    out_9439_1 = pd.concat([out_9434_1, cal_rate_energy_demand_TWh])

    # Apply cliamte smart crop levers (reduce ?)
    # => Determine the % of gaseous-ff-natural than can be converted to gaseous-bio

    # OTS/FTS fuel-switch [%]
    fuel_switch_percent = import_data(trigram='agr', variable_name='fuel-switch')
    # fuel-switch[%] = 1 - fuel-switch[%]
    fuel_switch_percent['fuel-switch[%]'] = 1.0-fuel_switch_percent['fuel-switch[%]']
    # Fuel Switch fffuels to biofuels
    out_8983_1 = x_switch(demand_table=energy_demand_TWh, switch_table=fuel_switch_percent, correlation_table=ratio)
    # Add sector = agr
    out_8983_1['sector'] = "agr"
    # energy-demand [TWh]
    energy_demand_TWh = export_variable(input_table=out_8983_1, selected_variable='energy-demand[TWh]')
    # energy-demand [TWh]
    energy_demand_TWh = use_variable(input_table=energy_demand_TWh, selected_variable='energy-demand[TWh]')

    # For : Electricity supply
    #  - Energy demand

    # Group by Country, Years, energy-carrier, sector (sum)
    energy_demand_TWh_2 = group_by_dimensions(df=energy_demand_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier', 'sector'], aggregation_method='Sum')
    # Module = Electricity
    energy_demand_TWh_2 = column_filter(df=energy_demand_TWh_2, pattern='^.*$')

    # 1.A.4.c. Energy emissions
    # 
    # Agriculture direct GHG emission linked to energy consumption
    # 
    # Main inputs: 
    # 
    # - Energy demand [TWh]
    # - Emission factor CO2 [Mt/TWh]
    # 
    # Main outputs: 
    # - Overall CO2 emission [Mt]
    # - Gas demand for bioenergy module [TWh]

    # RCP agr-combustion-emission-factor [Mt/TWh]
    agr_combustion_emission_factor_Mt_per_TWh = import_data(trigram='agr', variable_name='agr-combustion-emission-factor', variable_type='RCP')
    # Keep gaes = CO2 (only)
    agr_combustion_emission_factor_Mt_per_TWh = agr_combustion_emission_factor_Mt_per_TWh.loc[agr_combustion_emission_factor_Mt_per_TWh['gaes'].isin(['CO2'])].copy()
    # emissions[Mt] = energy-demand[TWh] * agr-combustion-emission-factor[Mt/TWh]
    emissions_Mt_6 = mcd(input_table_1=energy_demand_TWh, input_table_2=agr_combustion_emission_factor_Mt_per_TWh, operation_selection='x * y', output_name='emissions[Mt]')
    # emission-type = energy-demand
    emissions_Mt_6['emission-type'] = "energy-demand"

    # Calibration
    # Emissions

    # Emission linked to energy demand (CO2 only)

    # Group by  Country, Years, gaes
    emissions_Mt_8 = group_by_dimensions(df=emissions_Mt_6, groupby_dimensions=['Country', 'Years', 'gaes', 'emission-type'], aggregation_method='Sum')
    # Calibration emissions [Mt]
    emissions_Mt_7 = import_data(trigram='agr', variable_name='emissions', variable_type='Calibration')
    emissions_Mt_7 = emissions_Mt_7.loc[~emissions_Mt_7['emission-type'].isin(['c-stock'])].copy()
    # Split emission-type energy-demand
    emissions_Mt_9 = emissions_Mt_7.loc[emissions_Mt_7['emission-type'].isin(['energy-demand'])].copy()
    emissions_Mt_excluded = emissions_Mt_7.loc[~emissions_Mt_7['emission-type'].isin(['energy-demand'])].copy()
    # Remove empty dimensions
    out_9472_1 = missing_value_column_filter(df=emissions_Mt_9, missing_threshold=1.0, type_of_pattern='Manual')
    # Apply Calibration on emissions coming from energy-demand
    _, out_9471_2, out_9471_3 = calibration(input_table=emissions_Mt_8, cal_table=out_9472_1, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')
    # Apply cal rate to emissions[Mt]
    emissions_Mt_6 = mcd(input_table_1=emissions_Mt_6, input_table_2=out_9471_2, operation_selection='x * y', output_name='emissions[Mt]')
    # Remove sector
    emissions_Mt_6 = column_filter(df=emissions_Mt_6, columns_to_drop=['sector'])

    # Other Emissions

    # Fill missing by ""
    emissions_Mt_excluded = missing_value(df=emissions_Mt_excluded, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # Group by  Country, Years, gaes, amendment-type, raw-material, emission-type
    emissions_Mt_excluded = group_by_dimensions(df=emissions_Mt_excluded, groupby_dimensions=['Country', 'Years', 'emission-type', 'gaes', 'amendment-type', 'raw-material'], aggregation_method='Sum')

    # Biomass coming from grass (pasture)

    # OTS (only) grass-yields [tonnes DM/ha]
    grass_yields_tonnes_DM_per_ha = import_data(trigram='agr', variable_name='grass-yields', variable_type='OTS (only)')
    # Same as last available year
    grass_yields_tonnes_DM_per_ha = add_missing_years(df_data=grass_yields_tonnes_DM_per_ha)
    # grass-production[t] = land-management[ha] * grass-yields[tonnes DM/ha]
    grass_production_t = mcd(input_table_1=land_management_ha, input_table_2=grass_yields_tonnes_DM_per_ha, operation_selection='x * y', output_name='grass-production[t]')
    # Group by  Country, Years, raw-material (sum)
    grass_production_t = group_by_dimensions(df=grass_production_t, groupby_dimensions=['Country', 'Years', 'raw-material'], aggregation_method='Sum')
    # RCP grass-bioenergy-conv-factor [TWh/t]
    grass_bioenergy_conv_factor_TWh_per_t = import_data(trigram='agr', variable_name='grass-bioenergy-conv-factor', variable_type='RCP')
    # potential-energy-production[TWh] = grass-production[t] * grass-bioenergy-conv-factor [TWh/t]
    potential_energy_production_TWh = mcd(input_table_1=grass_production_t, input_table_2=grass_bioenergy_conv_factor_TWh_per_t, operation_selection='x * y', output_name='potential-energy-production[TWh]')

    # Apply biomass supply valorisation of co-products and co levers (switch)
    # => determine % of pasture grass which are effectively used to produce biogaz

    # OTS/FTS grass-bioenergy-share [%]
    grass_bioenergy_share_percent = import_data(trigram='agr', variable_name='grass-bioenergy-share')
    # energy-production[TWh] = potentiel-energy-production[TWh] * grass-bioenergy-share [%]
    energy_production_TWh_2 = mcd(input_table_1=potential_energy_production_TWh, input_table_2=grass_bioenergy_share_percent, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_2 = group_by_dimensions(df=energy_production_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = grass-pasture
    energy_production_TWh_2['origin'] = "grass-pasture"
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh])
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # For : Bioenergy
    #  - Energy production

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    # Module = Bioenergy
    energy_production_TWh = column_filter(df=energy_production_TWh, pattern='^.*$')
    out_9090_1 = pd.concat([land_management_ha, out_9094_1])
    # Module = Land-use
    out_9090_1 = column_filter(df=out_9090_1, pattern='^.*$')

    # Fertilizer and other amendments demand

    # Fertilizer, nitrogen balance and other amendments 
    # 
    # Objective: the node is computing the demand for fertilizer and others amendment (lime, urea, ...) based on the demand for crops and the agriculture practices given the nitrogen balance (N export through cultivation, N import through fertilizer application). 
    # 
    # 
    # Main inputs: 
    # 
    # - Crop production [t]
    # - Amendment application rate [t/ha]
    # 
    # Main outputs: 
    # 1. Amendment application [t]

    # Apply cliamte smart crop levers (reduce)
    # => determine quantity of fertilizer applied per ha (could be reduce with smart crop practices)
    # => only available for fertilzers (nitrogen phospohore, potash) but not for other amendments (urea, liming). For these last categories : we need to compute year after baseyear then.

    # OTS/FTS amendment-application-rate [t/ha]
    amendment_application_rate_t_per_ha = import_data(trigram='agr', variable_name='amendment-application-rate')
    # Top : amendment-type = nitrogen, potash, phosphore Bottom : rest
    amendment_application_rate_t_per_ha_2 = amendment_application_rate_t_per_ha.loc[amendment_application_rate_t_per_ha['amendment-type'].isin(['nitrogen', 'phosphore', 'potash'])].copy()
    amendment_application_rate_t_per_ha_excluded = amendment_application_rate_t_per_ha.loc[~amendment_application_rate_t_per_ha['amendment-type'].isin(['nitrogen', 'phosphore', 'potash'])].copy()
    # Same as last available year
    amendment_application_rate_t_per_ha_excluded = add_missing_years(df_data=amendment_application_rate_t_per_ha_excluded)
    amendment_application_rate_t_per_ha_2 = pd.concat([amendment_application_rate_t_per_ha_2, amendment_application_rate_t_per_ha_excluded])
    # amendment-application[t]  = land-management[ha] * amendment-application-rate[t/ha]
    amendment_application_t = mcd(input_table_1=land_management_ha, input_table_2=amendment_application_rate_t_per_ha_2, operation_selection='x * y', output_name='amendment-application[t]')
    # amendment-application [t]
    amendment_application_t = export_variable(input_table=amendment_application_t, selected_variable='amendment-application[t]')

    # Emissions : 
    # - N2O due to other sources (mainly inorganic fertilizers)
    # 
    # Objective :
    # Calculate the N2O emission due to fertilizer application (nitrogen only)
    # 
    # Input : 
    # - Fertilizer application [t]
    # - Emission factor of fertilizer application [Mt/t]
    # 
    # Output :
    # - Fertilizer emissions [Mt]

    # amendment-application [t]
    amendment_application_t = use_variable(input_table=amendment_application_t, selected_variable='amendment-application[t]')
    # RCP fertilizer-emission-factor[Mt/t] ( * 10e9 = 15.7kg/t ) 
    fertilizer_emission_factor_Mt_per_t = import_data(trigram='agr', variable_name='fertilizer-emission-factor', variable_type='RCP')
    # emissions[Mt] = amendment-application[t] * fertilizer-emission-factor[Mt/t]
    emissions_Mt_7 = mcd(input_table_1=amendment_application_t, input_table_2=fertilizer_emission_factor_Mt_per_t, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, gaes, amendment-type
    emissions_Mt_7 = group_by_dimensions(df=emissions_Mt_7, groupby_dimensions=['Country', 'Years', 'amendment-type', 'gaes'], aggregation_method='Sum')
    # emission-type = 3D-agricultural-soils-fertilizers
    emissions_Mt_7['emission-type'] = "3D-agricultural-soils-fertilizers"

    # 3.G. Liming / 3.H. Urea application​​
    # 
    # This category represents the CO2 emissions that occur when liming is realized or when urea is applied on agricultural soils.
    # 
    # Liming : 
    # When CaCO3 comes in contact with strong acid sources such as nitric acid in the soil, chemical reaction is triggered and some of the CaCO3 degrades, releasing CO2 emissions. ​
    # Equation : CaCO3+2HNO3 => H2O + CO2 + Ca(NO3)2
    # 
    # Urea fertilization :
    # ​Fertilization with urea can lead to a loss of carbon dioxide (CO2) that was fixed during the industrial production process.​
    # 
    # In a first version, we propose to make it simple ! Should be reviewed ?​
    # 
    # Input : 
    # - Amendment application rate [t/ha] (dolomite, lime, urea)
    # - C-amendment-emission-factor[Mt/t]
    # 
    # Output :
    # - CO2 liming and urea emission [Mt]

    # amendment-application [t]
    amendment_application_t_2 = use_variable(input_table=amendment_application_t, selected_variable='amendment-application[t]')
    # Group by  Country, Years, amendment-type
    amendment_application_t_3 = group_by_dimensions(df=amendment_application_t_2, groupby_dimensions=['Country', 'Years', 'amendment-type'], aggregation_method='Sum')
    # OTS (only) C-amendment-emission-factor [Mt/t]
    C_amendment_emission_factor_Mt_per_t = import_data(trigram='agr', variable_name='C-amendment-emission-factor', variable_type='OTS (only)')
    # Same as last available year
    C_amendment_emission_factor_Mt_per_t = add_missing_years(df_data=C_amendment_emission_factor_Mt_per_t)
    # emissions[Mt] = amendment-application[t] * C-amendment-emission-factor [Mt/t]
    emissions_Mt_8 = mcd(input_table_1=amendment_application_t_3, input_table_2=C_amendment_emission_factor_Mt_per_t, operation_selection='x * y', output_name='emissions[Mt]')
    # Group by  Country, Years, gaes, amendment-type, emission-type
    emissions_Mt_8 = group_by_dimensions(df=emissions_Mt_8, groupby_dimensions=['Country', 'Years', 'amendment-type', 'emission-type', 'gaes'], aggregation_method='Sum')
    out_9481_1 = metanode_9481(port_03=emissions_Mt_3, port_01=emissions_Mt, port_02=emissions_Mt_5, port_04=emissions_Mt_4, port_06=emissions_Mt_2, port_07=emissions_Mt_8, port_05=emissions_Mt_7)
    # Fill missing by ""
    out_9481_1 = missing_value(df=out_9481_1, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # Apply Calibration on emissions coming from energy-demand
    out_9481_1, _, out_9456_3 = calibration(input_table=out_9481_1, cal_table=emissions_Mt_excluded, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')
    # Group by  Country, Years, gaes, emission-type
    out_9481_1 = group_by_dimensions(df=out_9481_1, groupby_dimensions=['Country', 'Years', 'gaes', 'emission-type'], aggregation_method='Sum')
    out_9477_1 = pd.concat([emissions_Mt_6, out_9481_1])
    # Set "" if missing
    out_9477_1 = missing_value(df=out_9477_1, dimension_rx='^.*\\[.*•\\]$', DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedStringValueMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory']], FixedValue='')
    # emissions [Mt]
    emissions_Mt = export_variable(input_table=out_9477_1, selected_variable='emissions[Mt]')

    # For : Climate
    #  - Emissions

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Add emissions-or-capture
    emissions_Mt_2 = emissions_Mt.assign(**{'emissions-or-capture': "emissions"})
    # Module = Climate emissions
    emissions_Mt_2 = column_filter(df=emissions_Mt_2, pattern='^.*$')
    # CP gwp [-] (Global Warming Potential) from Climate Emissions
    clt_gwp_ = import_data(trigram='clt', variable_name='clt_gwp', variable_type='CP')
    # Switch  variable to double
    gwp_100 = math_formula(df=clt_gwp_, convert_to_int=False, replaced_column='gwp-100[-]', splitted='$gwp-100[-]$')
    # Convert Unit Mt to t
    emissions_t = emissions_Mt.drop(columns='emissions[Mt]').assign(**{'emissions[t]': emissions_Mt['emissions[Mt]'] * 1000000.0})
    # emissions[tCO2e] = emissions[t] x gwp[-]
    emissions_tCO2e = mcd(input_table_1=emissions_t, input_table_2=gwp_100, operation_selection='x * y', output_name='emissions[tCO2e]')
    # Top : emission-type = 3C, 3D, 3E, 3F, 3G, 3H (linked to cropland)
    emissions_tCO2e_2 = emissions_tCO2e.loc[emissions_tCO2e['emission-type'].isin(['3C-rice-cultivation', '3D-agricultural-soils-fertilizers', '3D-agricultural-soils-manure', '3D-agricultural-soils-residues', '3F-burnt-residues', '3G-liming', '3H-urea-application'])].copy()
    emissions_tCO2e_excluded = emissions_tCO2e.loc[~emissions_tCO2e['emission-type'].isin(['3C-rice-cultivation', '3D-agricultural-soils-fertilizers', '3D-agricultural-soils-manure', '3D-agricultural-soils-residues', '3F-burnt-residues', '3G-liming', '3H-urea-application'])].copy()
    # Top : emission-type = 3A, 3B (linked to pasture)
    emissions_tCO2e_excluded = emissions_tCO2e_excluded.loc[emissions_tCO2e_excluded['emission-type'].isin(['3A-enteric-fermentation', '3B-manure-management'])].copy()
    # Group by Country, Years, gaes (sum)
    emissions_tCO2e_excluded = group_by_dimensions(df=emissions_tCO2e_excluded, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # emissions-per-fermland[tCO2e/ha] = emissions[tCO2e] / land-management[ha]
    emissions_per_fermland_tCO2e_per_ha = mcd(input_table_1=land_management_ha_excluded, input_table_2=emissions_tCO2e_excluded, operation_selection='y / x', output_name='emissions-per-fermland[tCO2e/ha]')
    # Group by Country, Years, gaes (sum)
    emissions_tCO2e_2 = group_by_dimensions(df=emissions_tCO2e_2, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # emissions-per-fermland[tCO2e/ha] = emissions[tCO2e] / land-management[ha]
    emissions_per_fermland_tCO2e_per_ha_2 = mcd(input_table_1=land_management_ha_2, input_table_2=emissions_tCO2e_2, operation_selection='y / x', output_name='emissions-per-fermland[tCO2e/ha]')
    emissions_per_fermland_tCO2e_per_ha = pd.concat([emissions_per_fermland_tCO2e_per_ha_2, emissions_per_fermland_tCO2e_per_ha])
    # Group by Country, Years, gaes (sum)
    emissions_tCO2e = group_by_dimensions(df=emissions_tCO2e, groupby_dimensions=['Country', 'Years', 'gaes'], aggregation_method='Sum')
    # Convert Unit t to g
    emissions_gCO2e = emissions_tCO2e.drop(columns='emissions[tCO2e]').assign(**{'emissions[gCO2e]': emissions_tCO2e['emissions[tCO2e]'] * 1000000.0})
    # emissions-per-food-production[gCO2e/kcal] = emissions[gCO2e] / total-domestic-production[kcal]
    emissions_per_food_production_gCO2e_per_kcal = mcd(input_table_1=emissions_gCO2e, input_table_2=total_domestic_production_kcal, operation_selection='x / y', output_name='emissions-per-food-production[gCO2e/kcal]')
    emissions_per_per = pd.concat([emissions_per_fermland_tCO2e_per_ha, emissions_per_food_production_gCO2e_per_kcal])

    # Cal_rate for emissions[Mt]


    def helper_18(input_table) -> pd.DataFrame:
        import numpy as np
        
        # Copy input to output
        output_table = input_table.copy()
        
        # Select dimensions
        dimensions = list(output_table.select_dtypes(['object']).columns)
        
        # Select the given columns
        output_table[dimensions] = output_table[dimensions].replace({"": np.nan})
        return output_table
    # Remove empty strings
    out_18_1 = helper_18(input_table=out_9456_3)
    out = pd.concat([out_9471_3, out_18_1])
    # cal_rate for emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out, selected_variable='cal_rate_emissions[Mt]')
    out_9441_1 = pd.concat([out_9439_1, cal_rate_emissions_Mt])
    # Module = Calibration
    out_9441_1 = column_filter(df=out_9441_1, pattern='^.*$')
    out_9603_1 = pd.concat([amendment_application_t, energy_demand_TWh])
    out_9604_1 = pd.concat([livestock_population_lsu_2, out_9603_1])
    out_9605_1 = pd.concat([out_9604_1, N_manure_quantity_kgN_2])
    # Module = Air Pollution
    out_9605_1 = column_filter(df=out_9605_1, pattern='^.*$')
    # Keep amendment-type = nitrogen, phosphore, potash
    amendment_application_t_2 = amendment_application_t_2.loc[amendment_application_t_2['amendment-type'].isin(['nitrogen', 'phosphore', 'potash'])].copy()
    # amendment-application[t] as fertilizer-application[t] ------------------------ amendment-type as fertilizer
    out_9493_1 = amendment_application_t_2.rename(columns={'amendment-application[t]': 'fertilizer-application[t]', 'amendment-type': 'fertilizer'})

    # For : Pathways Explorer
    #  - Fertilizer demand (should we include as well pesticide and other fertilizer ?)

    # fertilizer-application [t]
    fertilizer_application_t = use_variable(input_table=out_9493_1, selected_variable='fertilizer-application[t]')
    # Convert Unit t to Mt
    fertilizer_application_Mt = fertilizer_application_t.drop(columns='fertilizer-application[t]').assign(**{'fertilizer-application[Mt]': fertilizer_application_t['fertilizer-application[t]'] * 1e-06})

    # For : Minerals
    #  - Fertilizer demand

    # Pivot

    out_8958_1, _, _ = pivoting(df=fertilizer_application_Mt, agg_dict={'fertilizer-application[Mt]': 'sum'}, column_name_option='Pivot name+Aggregation name', column_name_policy='Keep original name(s)', list_group_columns=['Country', 'Years'], list_pivots=['fertilizer'])
    out_8959_1 = missing_value_column_filter(df=out_8958_1, missing_threshold=0.9, type_of_pattern='Manual')
    # Same as in Minerals
    out_8959_1 = column_rename_regex(df=out_8959_1, search_string='(.*)\\+(.*)(\\[.*)', replace_string='agr_demand_$1$3')
    # phosphore to phosphate
    out_8961_1 = out_8959_1.rename(columns={'agr_demand_phosphore[Mt]': 'agr_demand_phosphate[Mt]'})
    # Keep only the phosphate and potass
    out_8961_1 = column_filter(df=out_8961_1, columns_to_drop=['agr_demand_nitrogen[Mt]'])
    # Module = Minerals
    out_8961_1 = column_filter(df=out_8961_1, pattern='^.*$')
    # Years
    out_8605_1 = out_8961_1.assign(Years=out_8961_1['Years'].astype(str))

    # For Water & Employement - Not used anymore
    # Previously exported variables for this module :
    # agr_liv-population_abp_dairy-milk[lsu]
    # agr_liv-population_abp_hens-egg[lsu]
    # agr_liv-population_meat_oth-animals[lsu]
    # agr_liv-population_meat_bovine[lsu]
    # agr_liv-population_meat_pig[lsu]
    # agr_liv-population_meat_poultry[lsu]
    # agr_liv-population_meat_sheep[lsu]
    # agr_domestic-production_afw_algae[kcal]
    # agr_domestic-production_afw_insect[kcal]
    # agr_domestic-production_afw_pulse[kcal]
    # agr_domestic-production_afw_fruit[kcal]
    # agr_domestic-production_afw_veg[kcal]
    # agr_domestic-production_afw_starch[kcal]
    # agr_domestic-production_afw_sugarcrop[kcal]
    # agr_domestic-production_afw_oilcrop[kcal]
    # agr_domestic-production_afw_energycrop[t]
    # agr_domestic-production_afw_energycrop[kcal]
    # agr_domestic-production_afw_cereal[kcal]

    # Module = Pathway Explorer
    fertilizer_application_Mt = column_filter(df=fertilizer_application_Mt, pattern='^Country$|^Years$')
    # Group by Country, Years (sum)
    fertilizer_application_t = group_by_dimensions(df=fertilizer_application_t, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    t = pd.concat([food_demand_t, fertilizer_application_t])
    # Module = Industry Ammonia
    t = column_filter(df=t, pattern='^.*$')

    # For : KPIs (Pathway Explorer)
    #  - Fertilizer and pesticides consumptions

    # Group by  Country, Years (sum)
    amendment_application_t = group_by_dimensions(df=amendment_application_t, groupby_dimensions=['Country', 'Years', 'amendment-type'], aggregation_method='Sum')
    # amendment-application-rate [t/ha]
    amendment_application_rate_t_per_ha = use_variable(input_table=amendment_application_rate_t_per_ha, selected_variable='amendment-application-rate[t/ha]')
    # Top: amendment-type = nitrogen, phosphore,  potash
    amendment_application_rate_t_per_ha = amendment_application_rate_t_per_ha.loc[amendment_application_rate_t_per_ha['amendment-type'].isin(['nitrogen', 'phosphore', 'potash'])].copy()
    # Group by  Country, Years (sum)
    amendment_application_rate_t_per_ha = group_by_dimensions(df=amendment_application_rate_t_per_ha, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Convert Unit t/ha to kg/ha (*1000)
    amendment_application_rate_kg_per_ha = amendment_application_rate_t_per_ha.drop(columns='amendment-application-rate[t/ha]').assign(**{'amendment-application-rate[kg/ha]': amendment_application_rate_t_per_ha['amendment-application-rate[t/ha]'] * 1000.0})
    amendment_application = pd.concat([amendment_application_rate_kg_per_ha, amendment_application_t])
    out_9557_1 = pd.concat([amendment_application, emissions_per_per])
    # Module = Pathway Explorer (KPIs)
    out_9557_1 = column_filter(df=out_9557_1, pattern='^.*$')
    # KPI's + graphs metrics
    out_9558_1 = pd.concat([out_9557_1, fertilizer_application_Mt])
    out_9337_1 = add_trigram(module_name=module_name, df=out_9558_1)

    return out_9337_1, out_9441_1, t, out_9090_1, energy_demand_TWh_2, energy_production_TWh, emissions_Mt_2, out_9605_1, out_8605_1, food_net_import_kcal_2, out_9579_1


