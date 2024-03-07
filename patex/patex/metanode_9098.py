import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *


# Land-Use module
# (module-name = agriculture)
def metanode_9098(port_01):
    # Calibration RATES


    # Carbon dynamics
    # 
    # Objective: the node is computing the carbon dynamics given the change in land usage.
    # Carbon stock changes due to change in :
    # 1. Biomass (above / below groud)
    # 2. Dead organic matter (dead wood and litter)
    # 3. Soils (mineral soils, organic soils, inorganic soils)
    # 4. Non-CO2 emissions (eg. forest fire)


    # Aggregate results


    # Aggregate results


    # Note :
    # Il faut éviter d'appliquer des % de disease sur les foret déjà brulée l'année précédente (elles ne brulent pas deux années consécutives ? Ou alors la perte en C n'est pas la même d'une année à l'autre ?), ... : Comment gérer cela ?
    # 
    # Quand une foret a brulée ; on doit al faire passer en new forest ?


    # Aggregate results


    # Aggregate results


    # Aggregate results


    # Aggregate results


    # FOREST


    # Carbon dynamics - 1.B) Biomass : Losses due to disturbance
    # Note : no disturbance applied on grassland ?!


    # Carbon dynamics - 1.C) Biomass : Losses due to wood removal and fuel wood
    # Note : no wood / fuel wood removal from grassland
    # SHOULD WE INCLUDE GRASS HARVEST (used for bioenergy) ?? CFR BIOMASS COMING FROM GRASS (pasture) : in agriculture : rappatrier ici ??


    # Add caracteristics linked to forest computation


    # Carbon dynamics - 2) DOM (only on changes (new / lost land) - cfr IPCC Guidances)
    # 
    # The Tier 1 method assumes that the dead wood and litter stocks are at equilibrium, so there is no need to estimate the carbon stock changes for these pools. Thus, there is no worksheet provided for DOM in Grassland Remaining Grassland.​
    # For new lands : Cn and C0 = 0 => so, DOM = 0 (no need to compute here)
    # Pay attention : if slash and burnt agriculture is widely practiced, this assumption is not true anymore !


    # Add non-CO2 (fire !)


    # GRASSLAND
    # (including pasture and natural prairies)


    # Carbon dynamics - 2) DOM (only on changes (new / lost land) - cfr IPCC Guidances)
    # 
    # The Tier 1 method assumes tThe Tier 1 method assumes that the dead wood and litter stocks are not present in Cropland or are at equilibrium as in agroforestry systems and orchards. Thus, there is no need to estimate the carbon stock changes for these pools.​
    # For new lands : Cn and C0 = 0 => so, DOM = 0 (no need to compute here)
    # Pay attention : if slash and burnt agriculture is widely practiced, this assumption is not true anymore !


    # Carbon dynamics - 1.B) Biomass : Losses due to disturbance
    # Note : no disturbance applied on cropland ?!


    # Add caracteristics linked to cropland computation


    # Add caracteristics linked to cropland computation


    # Note :
    # Default value are used (cfr IPCC Guidances - Table 5.1).
    # 
    # We don't have default values for boreal and subtropical climate.
    # For boreal : we use temperate values
    # For Subtropical : we use tropical values
    # 
    # This could lead to wrong output


    # Convert in tC and apply it to the rest of the tC computation


    # CROPLANDS
    # (including industrial and energy crops)


    # Carbon dynamics - 3.A) Organic soils
    #  No computation for settlement => We consider 100% mineral soils


    # Carbon dynamics - 1.B) Biomass : Losses due to disturbance
    # Note : no disturbance applied on settlement


    # Add caracteristics linked to settlement computation


    # Carbon dynamics - 2) DOM (only on changes (new / lost land) - cfr IPCC Guidances)
    # 
    # No computation for settlement ?


    # To be consider in tC computation ?
    # 
    # + Add :
    # - burnt crops and co


    # SETTLEMENT


    # For : Pathway Explorer
    # 
    # - Land management [ha]
    # => Detailled by type of land-use


    # For : Air Quality
    # 
    # - Land management [ha]
    # => Detailled by type of land-use



    module_name = 'agriculture'

    # Adapt data from other module => pivot and co

    # AGRICULTURE
    out_5965_1 = port_01

    # Emissions

    # Land remaining and changing
    # & climate / sub-climate / ecological-zone caracteristics
    # & type of grapland : woody (=fruits) or not
    # 
    # Objective: Split cropland in woody- and non-woody-cropland
    # 
    # Equation : We assume the evolution of woody cropland follows the evolution of fruit consumption ! However, it should include more than fruits as area of woody cropland is higher than fruit cropland (ratio between woody cropland vs fruit cropland is then higher than 1 !)
    # 1) Area of woody cropland [ha] = fruit area[ha] * woody-cropland-ratio[%]
    # 2) Convert this area in % of total cropland ( = woody-cropland[ha] / total cropland[ha])
    # 3) Apply this % to get woody-cropland (% x total-cropland) and non-woody-cropland ((1-%) x total-cropland)

    # Get % of woody cropland (woody => fruit cropland) and apply it to climatic / ecological zone split

    # cropland-management [ha]
    cropland_management_ha_2 = use_variable(input_table=out_5965_1, selected_variable='cropland-management[ha]')
    # OTS (only) woody-fruit-ratio [%]
    woody_fruit_ratio_percent = import_data(trigram='agr', variable_name='woody-fruit-ratio', variable_type='OTS (only)')
    # Same as last available year
    woody_fruit_ratio_percent = add_missing_years(df_data=woody_fruit_ratio_percent)
    # Group by  Country, Years (sum)
    woody_fruit_ratio_percent = group_by_dimensions(df=woody_fruit_ratio_percent, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Group by  Country, Years (sum)
    cropland_management_ha = group_by_dimensions(df=cropland_management_ha_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Keep raw-material = fruit
    cropland_management_ha_2 = cropland_management_ha_2.loc[cropland_management_ha_2['raw-material'].isin(['fruit'])].copy()
    # Group by  Country, Years (sum)
    cropland_management_ha_2 = group_by_dimensions(df=cropland_management_ha_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # cropland-management[ha] (woody-fruit cropland) = cropland-management[ha] (fruit only) * woody-fruit-ratio[%]
    cropland_management_ha_2 = mcd(input_table_1=cropland_management_ha_2, input_table_2=woody_fruit_ratio_percent, operation_selection='x * y', output_name='cropland-management[ha]')
    # fruit-pct[%] = cropland-management[ha] (woody fruit only) / cropland-management[ha]
    fruit_pct_percent = mcd(input_table_1=cropland_management_ha_2, input_table_2=cropland_management_ha, operation_selection='x / y', output_name='fruit-pct[%]')

    # Product / material / ressource demand

    # Agriculture Land
    # 
    # Objective: the node is computing the land demand for agriculture (cropland, pasturelands), including frozen lands for biodiversity. 
    # 
    # Main inputs: 
    # - Cropland demand [ha]
    # - Pastureland [ha]
    # - Frozen agriculture land [ha]
    # 
    # Main outputs: 
    # - Demand for agriculture land, including biodiversity frozen lands [ha]

    # land-management [ha] (cropland and pasture)
    land_management_ha = use_variable(input_table=out_5965_1, selected_variable='land-management[ha]')

    # Other land management (settlements, industrial-crops, others, ...)

    # Apply land-man-others levers (switch ??)
    # => determine the land use made for settlements
    # => for other uses (other, industrial-crops, ...) ; future values = same as last year

    # OTS/FTS land-management [ha]
    land_management_ha_2 = import_data(trigram='agr', variable_name='land-management')

    # Total land use
    # 
    # Objective: the node is computing the land dynamics given the demand for land-uses.
    # 
    # 1. Computing the overall land demand (all uses) [ha]
    # 2. Computing land use for lands made availble or land scarcity depending on the lever setting [ha].
    # 3. Filtering land surplus [ha] and deforestation [ha]
    # 4. Allocating land-use/cover to the land made availble 
    # 
    # Main inputs: 
    # 
    # - Agriculture land demand [ha] (cropland / pasture / frozen)
    # - Forest land [ha]
    # - Other land [ha] (artifical land / wetlands / others)
    # - Overall land area [ha]
    # - Land allocation [%]
    # 
    # Main outputs: 
    # - Forest area added/removed [ha]
    # - Unmanaged land [ha]
    # - Natural prairie [ha]

    # OTS (only) total-land-surface [ha]
    total_land_surface_ha = import_data(trigram='agr', variable_name='total-land-surface', variable_type='OTS (only)')
    # Same as last available year
    total_land_surface_ha = add_missing_years(df_data=total_land_surface_ha)
    # Group by  Country, Years (sum)
    total_land_surface_ha = group_by_dimensions(df=total_land_surface_ha, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    land_management_ha_2 = land_management_ha_2.loc[~land_management_ha_2['land-use'].isin(['frozen-forest'])].copy()
    # Same as last available year
    land_management_ha_2 = add_missing_years(df_data=land_management_ha_2)
    land_management_ha = pd.concat([land_management_ha, land_management_ha_2.set_index(land_management_ha_2.index.astype(str) + '_dup')])
    # land-management [ha]
    land_management_ha_2 = export_variable(input_table=land_management_ha, selected_variable='land-management[ha]')

    # Forest Land => Apply re-/de-forestation
    # Recompute forests land according to deforestation (land use > land surface) or reforestation (land use < land surface)

    # land-management [ha]
    land_management_ha = use_variable(input_table=land_management_ha_2, selected_variable='land-management[ha]')
    # Group by  Country, Years (sum)
    land_management_ha_2 = group_by_dimensions(df=land_management_ha_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # buffer-land[ha] = total-land-surface[ha] - land-management[ha]
    buffer_land_ha = mcd(input_table_1=land_management_ha_2, input_table_2=total_land_surface_ha, operation_selection='y - x', output_name='buffer-land[ha]')
    # ROUND buffer-land[ha] at 3 decimals (avoid accounting for little changes)
    buffer_land_ha_2 = math_formula(df=buffer_land_ha, convert_to_int=False, replaced_column='buffer-land[ha]', splitted='round($buffer-land[ha]$, 3)')
    # If < 0 => set 0
    buffer_land_ha = buffer_land_ha_2.copy()
    mask = buffer_land_ha['buffer-land[ha]']<0
    buffer_land_ha.loc[mask, 'buffer-land[ha]'] = 0
    buffer_land_ha.loc[~mask, 'buffer-land[ha]'] = buffer_land_ha.loc[~mask, 'buffer-land[ha]']

    # Apply land-man-forest (switch ??)
    # => determine the % of remaining land that are reforest

    # OTS/FTS reforestation-land-man [%]
    reforestation_land_man_percent = import_data(trigram='agr', variable_name='reforestation-land-man')
    # land-management[ha] = buffer-land[ha] * (1 - reforestation-land-man[%])
    land_management_ha_2 = mcd(input_table_1=buffer_land_ha, input_table_2=reforestation_land_man_percent, operation_selection='x * (1-y)', output_name='land-management[ha]')

    # Apply land-man-crops (switch ??)
    # => determine the % of remaining land that are converted into natural prairies and industrial-energy-crops (after reforestation)

    # OTS/FTS dynamic-land-man [%]
    dynamic_land_man_percent = import_data(trigram='agr', variable_name='dynamic-land-man')
    # Group by  Country, Years (sum)
    land_management_ha_2 = group_by_dimensions(df=land_management_ha_2, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # land-management[ha] (replace) = land-management[ha] * dynamic-land-man [%]
    land_management_ha_2 = mcd(input_table_1=land_management_ha_2, input_table_2=dynamic_land_man_percent, operation_selection='x * y', output_name='land-management[ha]')
    # land-management[ha] = buffer-land[ha] * reforestation-land-man[%]
    land_management_ha_3 = mcd(input_table_1=buffer_land_ha, input_table_2=reforestation_land_man_percent, operation_selection='x * y', output_name='land-management[ha]')
    # forest-decrease[ha] = 0 if buffer > 0 else : abs(buffer)
    forest_decrease_ha = buffer_land_ha_2.copy()
    mask = forest_decrease_ha['buffer-land[ha]']>0
    forest_decrease_ha.loc[mask, 'forest-decrease[ha]'] = 0
    forest_decrease_ha.loc[~mask, 'forest-decrease[ha]'] = abs(forest_decrease_ha.loc[~mask, 'buffer-land[ha]'])
    # forest-decrease [ha]
    forest_decrease_ha = use_variable(input_table=forest_decrease_ha, selected_variable='forest-decrease[ha]')
    # land-use = forest
    forest_decrease_ha['land-use'] = "forest"
    # land-management[ha] (replace) = land-management[ha] - forest-decrease[ha]  LEFT JOIN If deforestation is missing => Set 0
    land_management_ha = mcd(input_table_1=land_management_ha, input_table_2=forest_decrease_ha, operation_selection='x - y', output_name='land-management[ha]', fill_value_bool='Left [x] Outer Join')
    # land-management[ha] (replace) = land-management[ha] + land-management[ha]  LEFT JOIN If reforestation is missing => Set 0
    land_management_ha = mcd(input_table_1=land_management_ha, input_table_2=land_management_ha_3, operation_selection='x + y', output_name='land-management[ha]', fill_value_bool='Left [x] Outer Join')
    # Add other buffer land (other than forest)
    land_management_ha = pd.concat([land_management_ha, land_management_ha_2.set_index(land_management_ha_2.index.astype(str) + '_dup')])
    # Group by  Country, Years, land-use (sum)
    land_management_ha = group_by_dimensions(df=land_management_ha, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # land-management [ha]
    land_management_ha = export_variable(input_table=land_management_ha, selected_variable='land-management[ha]')
    # land-management [ha]
    land_management_ha = use_variable(input_table=land_management_ha, selected_variable='land-management[ha]')
    # RCP climatic-area-share [%]
    climatic_area_share_percent = import_data(trigram='agr', variable_name='climatic-area-share', variable_type='RCP')
    # Remove 0.0  values (to avoid having several  unused lines)
    out_10046_1 = row_filter(df=climatic_area_share_percent, filter_type='RangeVal_RowFilter', that_column='climatic-area-share[%]', include=False, lower_bound_bool=True, upper_bound_bool=True, lower_bound='0.0', upper_bound='0.0')
    # land-management[ha] (replace) = land-management[ha] * climatic-area-share[%]
    land_management_ha_3 = mcd(input_table_1=land_management_ha, input_table_2=out_10046_1, operation_selection='x * y', output_name='land-management[ha]')
    # Top : cropland Bottom : rest
    land_management_ha_2 = land_management_ha_3.loc[land_management_ha_3['land-use'].isin(['cropland'])].copy()
    land_management_ha_excluded = land_management_ha_3.loc[~land_management_ha_3['land-use'].isin(['cropland'])].copy()
    # land-management[ha] (replace) = land-management[ha] * fruit-pct[%]
    land_management_ha_3 = mcd(input_table_1=fruit_pct_percent, input_table_2=land_management_ha_2, operation_selection='x * y', output_name='land-management[ha]')
    # land-use = woody-cropland
    land_management_ha_3['land-use'] = "woody-cropland"
    # land-management[ha] (replace) = land-management[ha] * (1-fruit-pct[%])
    land_management_ha_2 = mcd(input_table_1=fruit_pct_percent, input_table_2=land_management_ha_2, operation_selection='(1-x) * y', output_name='land-management[ha]')
    # land-use = non-woody-cropland
    land_management_ha_2['land-use'] = "non-woody-cropland"
    land_management_ha_2 = pd.concat([land_management_ha_3, land_management_ha_2.set_index(land_management_ha_2.index.astype(str) + '_dup')])
    land_management_ha_2 = pd.concat([land_management_ha_2, land_management_ha_excluded.set_index(land_management_ha_excluded.index.astype(str) + '_dup')])
    # CP (agr_)land-lifespan [years]
    agr_land_lifespan_years = import_data(trigram='agr', variable_name='agr_land-lifespan', variable_type='CP')
    # Remove source, data_type, module
    agr_land_lifespan_years = column_filter(df=agr_land_lifespan_years, columns_to_drop=['source', 'module', 'data_type'])

    def helper_688(input_table_1, input_table_2) -> pd.DataFrame:
        import numpy as np
        import re
        
        # Parameters
        new_duration = 20 #By default : 20 ans = new duration (could change in future, depends on Climate / Country ?)
        
        # Years list
        min_yr = input_table_1["Years"].min()
        max_yr = input_table_1["Years"].max()
        yrs = list(range(min_yr, max_yr+1))
        ALL_YRS = yrs #pd.DataFrame(data={"Years": yrs})
        INI_YRS = input_table_1["Years"].unique().tolist()
        
        # 
        DIMENSIONS_TYPES = ['int16', 'int32', 'int64', 'object']
        FILL_METHOD = "Constant"
        
        # -------------------------------------------------------------------------------------------------------------------
        #                                        FUNCTIONS
        # -------------------------------------------------------------------------------------------------------------------
        # Function lagged
        def lagged_variable(df, variable, lag_variable, shift_time, fill_val='current'):
            # If shift_time = dataframe
            if isinstance(shift_time, pd.DataFrame):
                # Dimensions and co
                dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
                kept_columns = dimensions + [variable]
                # Join shift_time to input df
                lifespan_dim = list(shift_time.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
                lifespan_metric = list(shift_time.select_dtypes(['float']).columns)[0]
                if 'Timestep' in df.columns:
                    del df['Timestep']
                df_lagged_variable = df[kept_columns]
                df_lagged_variable = df_lagged_variable.merge(shift_time, on=lifespan_dim, how='inner')
                df_lagged_variable.rename(columns={lifespan_metric: "Timestep"}, inplace=True)
                # Apply Timestep to lagged variable
                df_lagged_variable['Years'] = (df_lagged_variable['Years'].astype(int) + df_lagged_variable['Timestep'].astype(int)).astype(int)
                # Select variable to lag
                df_variable_to_lag = df[kept_columns]
                used_dimensions = list(df_variable_to_lag.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
            else:
                # Lag Years column
                df_years = pd.DataFrame(df['Years'].drop_duplicates().sort_values(ascending=False))
                df_years['Timestep'] = (df_years.shift(shift_time) - df_years)
                df_years.dropna(subset=['Timestep'], inplace=True)
                # Select variable to lag
                dimensions = list(df.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
                kept_columns = dimensions + [variable]
                df_variable_to_lag = df[kept_columns]
                df_variable_to_lag = df_variable_to_lag[df_variable_to_lag[variable].notnull()]
                for col in df_variable_to_lag.columns:
                    if df_variable_to_lag[col].isnull().all():
                        del df_variable_to_lag[col]
                used_dimensions = list(df_variable_to_lag.select_dtypes(['object', 'int', 'int32', 'int64']).columns)
                # Apply Timestep to lagged variable
                df_lagged_variable = df_variable_to_lag.merge(df_years, on='Years')
                df_lagged_variable['Years'] = (df_lagged_variable['Years'] + df_lagged_variable['Timestep']).astype(int)
            # Rename variable as lagged
            df_lagged_variable.rename(columns={variable: lag_variable}, inplace=True)
            # Merge lagged variable to original variable
            df_lagged_variable = df_variable_to_lag.merge(df_lagged_variable, on=used_dimensions, how='left')
            df_lagged_variable['Timestep'].fillna(0, inplace=True)
            # Fill missing value (start year) with current value
            if fill_val == 'current':
                dims = df_lagged_variable.select_dtypes(include=DIMENSIONS_TYPES).columns
                dims_wo_years = [d for d in dimensions if d != "Years"]
                dims_wt_years = dims_wo_years.copy()
                dims_wt_years.append("Years")
                df_lagged_variable = df_lagged_variable.sort_values(by=dims_wt_years)
                df_lagged_variable[lag_variable].fillna(method='bfill', inplace=True)
            else:
                df_lagged_variable[lag_variable].fillna(fill_val, inplace=True)
        
            return df_lagged_variable
        
        # Function add all years
        def add_all_years(df, metric_name, all_years):
            # List dimensions
            dimensions = df.select_dtypes(include=DIMENSIONS_TYPES).columns
            dimensions_wo_years = [d for d in dimensions if d != "Years"]
            dimensions_wt_years = dimensions_wo_years.copy()
            dimensions_wt_years.append("Years")
            # Add max years as default value column
            df_max_years = df.groupby(dimensions_wo_years)[["Years"]].max()
            df_max_years.columns = ["MaxYear"]
            df = df.merge(df_max_years.reset_index(), on=dimensions_wo_years)
            data_to_duplicate = df[df["Years"] == df["MaxYear"]]
            del data_to_duplicate["MaxYear"]
            del df["MaxYear"]
            # Set data_to_duplicate value = to missing
            data_to_duplicate[metric_name] = np.nan
            # Duplicates data for missing years
            df["concat-order"] = 1
            data_to_duplicate["concat-order"] = 2
            for year in all_years:
                data_to_duplicate["Years"] = year
                df = pd.concat([df, data_to_duplicate], ignore_index=True)
            # Sort on concat-order and remove duplicates
            df = df.sort_values(by=['concat-order'])
            df = df.drop_duplicates(subset=dimensions, keep='first')
            del df['concat-order']
            # Interpolate values (linear) by group of dimensions
            df = df.sort_values(by=dimensions_wt_years)
            df = df.groupby(dimensions_wo_years).apply(lambda group: group.interpolate())
            # Reset index and return
            df = df.reset_index(drop=True)
            return df
        
        # -------------------------------------------------------------------------------------------------------------------
        #                                        MODEL
        # -------------------------------------------------------------------------------------------------------------------
        # --- INPUT VALUES -----
        # Copy input to output
        df_input = input_table_1.copy()
        
        # --- ADD ALL YEARS -----
        # Define constants
        VARIABLE = "land-management[ha]"
        df = add_all_years(df_input, VARIABLE, ALL_YRS)
        
        # --- GET ANNUAL DIFFERENCES -----
        # Define constants
        rx = re.compile('(.*)\[(.*)\]')
        groups = rx.match(VARIABLE)
        LAGGED_VARIABLE = f'{groups[1]}_annual-lag[{groups[2]}]'
        # Apply lag : 1 year slip to get annual differences
        df_lagged = lagged_variable(df, VARIABLE, LAGGED_VARIABLE, 1)
        
        # --- COMPUTE NEW AND REMAINING LAND -----
        df = df_lagged.copy()
        df['diff[ha]'] = df[VARIABLE] - df[LAGGED_VARIABLE]
        del df[LAGGED_VARIABLE]
        
        # 1. Annual new land and new cumulated land
        df['annual-new-land[ha]'] = 0
        df['annual-lost-land[ha]'] = 0
        mask = (df['diff[ha]'] > 0)
        df.loc[mask, 'annual-new-land[ha]'] = df.loc[mask, 'diff[ha]']
        df.loc[~mask, 'annual-lost-land[ha]'] = df.loc[~mask, 'diff[ha]']
        df.sort_values(by=['Years'], inplace=True)
        dims = list(df.select_dtypes(['object']).columns)
        df['new-cum-land[ha]'] = df.groupby(dims)['annual-new-land[ha]'].cumsum()
        
        # 2. Remaining land
        # 2.1. Lagg new culmulated lands
        # Define constants
        VARIABLE = "new-cum-land[ha]"
        rx = re.compile('(.*)\[(.*)\]')
        groups = rx.match(VARIABLE)
        LAGGED_VARIABLE = f'{groups[1]}_20-yrs-lagged[{groups[2]}]'
        # Apply lag and remove cumulated sum after 20 years (new land are only considered as new during 20 years) => EXCEPT CROPLAND / SETTLEMENT
        df_lifespan = input_table_2.copy()
        df_lagged = lagged_variable(df, VARIABLE, LAGGED_VARIABLE, df_lifespan, fill_val=0)
        df_lagged[VARIABLE] = df_lagged[VARIABLE] - df_lagged[LAGGED_VARIABLE]
        del df_lagged[LAGGED_VARIABLE]
        del df_lagged["Timestep"]
        
        # 2.2. Replace new cum land in df (considering new during only 20 years)
        del df[VARIABLE] 
        dimensions = df.select_dtypes(include=DIMENSIONS_TYPES).columns
        dimensions_wt_years = [d for d in dimensions]
        df = df.merge(df_lagged, on=dimensions_wt_years, how='inner')
        # 2.3. Remaining land = land management - new cum land
        df['remaining-land[ha]'] = df["land-management[ha]"] - df["new-cum-land[ha]"]
        
        # 3. 20 yrs lagged land management
        # Define constants
        VARIABLE = "land-management[ha]"
        rx = re.compile('(.*)\[(.*)\]')
        groups = rx.match(VARIABLE)
        LAGGED_VARIABLE = f'{groups[1]}_lifetime-lagged[{groups[2]}]'
        # Apply lag
        df_lagged = lagged_variable(df, VARIABLE, LAGGED_VARIABLE, new_duration)
        del df_lagged[VARIABLE]
        del df_lagged["Timestep"]
        df = df.merge(df_lagged, on=dimensions_wt_years, how='inner')
        
        # Reformat : Remove unused years
        mask = (df["Years"].isin(INI_YRS))
        df = df.loc[mask, :]
        
        # Create the outputs
        output_table = df
        return output_table
    # land logic DESCRIBE !!!  AFTER 20 YEARS : new land becomes remaining EXCEPT FOR CROPLAND  (including industrial-and-energy-crop) / SETTLEMENT : After 1 year  FAIRE LE MASQUE (years) sur base d'une table d'input (loop sur lifetime value et concate table)
    out_688_1 = helper_688(input_table_1=land_management_ha_2, input_table_2=agr_land_lifespan_years)
    # new-cum-land [ha]
    new_cum_land_ha = use_variable(input_table=out_688_1, selected_variable='new-cum-land[ha]')
    # land-age = new (<= 20 years)
    new_cum_land_ha['land-age'] = "new"
    # new-cum-land[ha] as land-area[ha]
    out_9552_1 = new_cum_land_ha.rename(columns={'new-cum-land[ha]': 'land-area[ha]'})
    # remaining-land [ha]
    remaining_land_ha = use_variable(input_table=out_688_1, selected_variable='remaining-land[ha]')
    # land-age = remaining (> 20 years)
    remaining_land_ha['land-age'] = "remaining"
    # remaining-land[ha] as land-area[ha]
    out_9551_1 = remaining_land_ha.rename(columns={'remaining-land[ha]': 'land-area[ha]'})
    # Get all land by type (remaining or new)
    out_1 = pd.concat([out_9551_1, out_9552_1.set_index(out_9552_1.index.astype(str) + '_dup')])
    # land-area [ha]
    land_area_ha = export_variable(input_table=out_1, selected_variable='land-area[ha]')

    # Carbon dynamics - 1) Biomass (remaining and new land)
    # 
    # Objective: Compute C stock change due to change in biomass.
    # 
    # Equation : Change in C stock (biomass) 
    # =
    # (For remaining forest) C_Gains[tC/yr] - C_Losses[tC/yr] (IPCC Guidance, eqt. 2.7)
    # (For new forest) C_Gains[tC/yr] - C_Losses[tC/yr] + C_conversion (IPCC Guidance, eqt. 2.20)
    # 
    # Where :
    # C_Gains  = Sum(A[ha] * Gw[tDM/ha/yr] * (1+R[%]) * CF[tC/tDM]) ; (IPCC Guidance, eqt. 2.9)
    # C_Losses = Losses_wood_removals[tC/yr] + Losses_fuelwoods[tC/yr] + Losses_disturbances[tC/yr] (IPCC Guidance, eqt. 2.11)
    # C_conversion = 0 (Tier 1 methodology) => Therefore, C stock biomass equation are the same between remaining and new land (only value used in equation change)
    # 
    # Note (Grassland) : The assumption for Tier 1 is that ΔCG and ΔCL equal zero. Thus, the only term that requires calculation is the 
    # ΔCCONVERSION, which is calculated with Equation 2.16. For lands converted to Grassland, Equation 2.16 is computed twice, 
    # once for the herbaceous biomass and once for the woody biomass. 
    # This is done because each of these components has a different carbon fraction.
    # 
    # Where :
    # Losses_wood_removals[tC/yr] = H[m3/yr] * BCEF_R[tDM/m3] * (1+R[%]+BF[%]) * CF[tC/tDM] (IPCC Guidance, eqt. 2.12) ;
    # Losses_fuelwoods[tC/yr] = [FG_trees[m3/yr] * BCEF_R[tDM/m3] * (1+R[%]) + FG_part[m3/yr] * D[tDM/m3]] * CF[tC/tDM] (IPCC Guidance, eqt. 2.13)
    # Losses_disturbances = A_disturbance[ha] * Bw[tDM/ha][tC/yr] * fd[%] * (1+R[%]) * CF[tC/tDM](IPCC Guidance, eqt. 2.14)
    # 
    # Assumptions :
    # R = ratio of below-ground biomass to above-ground biomass for a specific vegetation type => Set to 0 (as we use Tier 1 methodology)
    # FG_part = 0 => We only have FG_trees ; Losses_fuelswoods becomes : FG_trees[m3/yr] * BCEF_R[tDM/m3] * (1+R[%]) * CF[tC/tDM].
    # Then, we combine Eq. 2.12 and 2.13 : (H[m3/yr] + F_trees[m3/yr]) * BCEF_R[tDM/m3] * (1+R[%]+BF[%]) * CF[tC/tDM].

    # land-area [ha]
    land_area_ha = use_variable(input_table=land_area_ha, selected_variable='land-area[ha]')

    # Carbon dynamics - 2) Dead Organic Matter (new land)
    # 
    # Objective: Compute C stock change due to change in dead ormanic matter.
    # 
    # Equation : Change in C stock (dead organic matter) 
    # =
    # (For remaining) = 0 (IPCC Guidance, if Tier 1 : no change in DOM for remaining land)
    # (For new land) = Aon[ha] * (Cn[tC/ha] - Co[tC/ha])/Ton (IPCC Guidance, eqt. 2.23)
    # 
    # Where :
    # Cn and Co  = dead wood/litter stock under the old (o) and new (n) land-use category ; (IPCC Guidance, eqt. 2.9)
    # Aon = Area undergoing conversion from old to new land-use category
    # Ton = time of period of the transition from old to new category
    # 
    # Assumptions :
    # Ton = 
    # Forest : 1 (carbon losses), 20 (carbon stock increase) (Tier 1)
    # Cropland/Grassland : 1
    # Co = 0 => We don't know what are the origin land (old land-use) so we put Co to 0 (Tier 1)
    # Cn = 0 (cropland, pasture) => Except if we change management practices (not considered for the moment, but should be included in the futur !)

    # land-area [ha]
    land_area_ha_3 = use_variable(input_table=land_area_ha, selected_variable='land-area[ha]')
    # Keep forests only land-area[ha]
    land_area_ha_2 = land_area_ha_3.loc[land_area_ha_3['land-use'].isin(['forest'])].copy()
    # Keep land-age = new
    land_area_ha_2 = land_area_ha_2.loc[land_area_ha_2['land-age'].isin(['new'])].copy()

    # Add caracteristics linked to forest computation

    # all except land-age (sum)
    land_area_ha_2 = group_by_dimensions(df=land_area_ha_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'ecological-zone', 'sub-climate-type'], aggregation_method='Sum')

    # Carbon dynamics - 3) Soils carbon stock : minerals, organics and inorganics
    # 
    # Objective: Compute C stock change due to change in soils carbon stock
    # 
    # Equation : Change in C stock (minerals) => Apply only on new land ! (cfr Tier 1) 
    # =
    # A(SOC_0[tC] - SOC_(0-T)[tC]) / D[yr] (IPCC Guidance, eqt. 2.25)
    # Where SOC = sum(SOC_REF[tC/ha] * F_LU[-] * F_MG[-] * F_I[-] * A[ha]) (sum on all climate zone, soil type and management system)
    # 
    # Where :
    # SOC_0 and SOC_(T-0)  = soil organic carbon stock in the last year (0) and at the beggining (0-T) of the inventory time period
    # D = Time depend of stock change factor using the SOC equation
    # SOC_REF = the reference carbon stock
    # F_LU = stock change factor for land-use system/ sub-system
    # F_MG = stock change factor for management regime
    # F_I = stock change factor for input of organic matter
    # 
    # Equation : Losses of C stock (organics) => New and remaining land
    # =
    # A[ha] * EF[tC/ha/yr] (IPCC Guidance, eqt. 2.26)
    # 
    # Where :
    # A  = land area of drained organic soil
    # EF = emission factor (depands on climate)
    # 
    # Equation : Change in C stock (inorganics) => Not applied (Tier 1)
    # 
    # Assumptions :
    # F_MG, F_I and F_LU (forest) = 1 (cfr IPCC Guidance, Tier 1) <> cropland / pasture (except F_LU, cfr below) : depends on management, ...
    # F_LU (pasture) = 1 (cfr IPCC Guidance, Tier 1 / Table 6.2)

    # land-area [ha]
    land_area_ha_4 = use_variable(input_table=land_area_ha_3, selected_variable='land-area[ha]')

    # Global parameters

    # CP organic-soil-emission-factor (agr_soil-organic-ef_cp) [tC/ha]
    agr_soil_organic_ef_tC_per_ha = import_data(trigram='agr', variable_name='agr_soil-organic-ef', variable_type='CP')
    # Remove unused columns (source, module, data-type) (mean)
    agr_soil_organic_ef_tC_per_ha = group_by_dimensions(df=agr_soil_organic_ef_tC_per_ha, groupby_dimensions=['climate-type', 'land-use'], aggregation_method='Mean')
    # Keep forests organic-soil-emission-factor [tC/ha]
    agr_soil_organic_ef_tC_per_ha_2 = agr_soil_organic_ef_tC_per_ha.loc[agr_soil_organic_ef_tC_per_ha['land-use'].isin(['forest'])].copy()
    agr_soil_organic_ef_tC_per_ha_excluded = agr_soil_organic_ef_tC_per_ha.loc[~agr_soil_organic_ef_tC_per_ha['land-use'].isin(['forest'])].copy()
    # Keep grassland(s) organic-soil-emission-factor [tC/ha]
    agr_soil_organic_ef_tC_per_ha_excluded_2 = agr_soil_organic_ef_tC_per_ha_excluded.loc[agr_soil_organic_ef_tC_per_ha_excluded['land-use'].isin(['pasture'])].copy()
    agr_soil_organic_ef_tC_per_ha_excluded_excluded = agr_soil_organic_ef_tC_per_ha_excluded.loc[~agr_soil_organic_ef_tC_per_ha_excluded['land-use'].isin(['pasture'])].copy()

    # Carbon dynamics - 3.A) Organic soils

    # Group by climate-type (sum)
    agr_soil_organic_ef_tC_per_ha_excluded = group_by_dimensions(df=agr_soil_organic_ef_tC_per_ha_excluded_2, groupby_dimensions=['climate-type'], aggregation_method='Sum')
    # Keep cropland(s) organic-soil-emission-factor [tC/ha]
    agr_soil_organic_ef_tC_per_ha_excluded_excluded = agr_soil_organic_ef_tC_per_ha_excluded_excluded.loc[agr_soil_organic_ef_tC_per_ha_excluded_excluded['land-use'].isin(['cropland'])].copy()

    # Carbon dynamics - 3.A) Organic soils

    # Group by climate-type (sum)
    agr_soil_organic_ef_tC_per_ha_excluded_excluded = group_by_dimensions(df=agr_soil_organic_ef_tC_per_ha_excluded_excluded, groupby_dimensions=['climate-type'], aggregation_method='Sum')
    # Keep forests land-area[ha]
    land_area_ha_3 = land_area_ha_4.loc[land_area_ha_4['land-use'].isin(['forest'])].copy()
    land_area_ha_excluded = land_area_ha_4.loc[~land_area_ha_4['land-use'].isin(['forest'])].copy()
    # Keep grassland(s) land-area[ha]
    land_area_ha_excluded_2 = land_area_ha_excluded.loc[land_area_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    land_area_ha_excluded_excluded = land_area_ha_excluded.loc[~land_area_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    # Include cropland(s) land-area[ha]
    land_area_ha_excluded_excluded = land_area_ha_excluded_excluded.loc[land_area_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    # land-area [ha] cropland only
    land_area_ha_4 = use_variable(input_table=land_area_ha_excluded_excluded, selected_variable='land-area[ha]')
    # Group by  Country, Years, land-use, land-age, climate-type (sum)
    land_area_ha_4 = group_by_dimensions(df=land_area_ha_4, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'land-age'], aggregation_method='Sum')
    # land-area [ha] GRASSLAND only
    land_area_ha_5 = use_variable(input_table=land_area_ha_excluded_2, selected_variable='land-area[ha]')
    # Group by  Country, Years, land-use, land-age, climate-type (sum)
    land_area_ha_5 = group_by_dimensions(df=land_area_ha_5, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'land-age'], aggregation_method='Sum')

    # Carbon dynamics - 3.A) Organic soils

    # land-area [ha]  FOREST only
    land_area_ha_3 = use_variable(input_table=land_area_ha_3, selected_variable='land-area[ha]')
    # RCP organic-area-share [%]
    organic_area_share_percent = import_data(trigram='agr', variable_name='organic-area-share', variable_type='RCP')

    # Carbon dynamics - 3.B) Minerals soils (new only ; we only compute change of land)
    # As all FL_x parameters = 1, equation becomes : A * SOC_REF / D (where D = 20 years)

    # organic-area-share [%]
    organic_area_share_percent_4 = use_variable(input_table=organic_area_share_percent, selected_variable='organic-area-share[%]')
    # Keep pasture
    organic_area_share_percent_2 = organic_area_share_percent.loc[organic_area_share_percent['land-use'].isin(['pasture'])].copy()
    organic_area_share_percent_excluded = organic_area_share_percent.loc[~organic_area_share_percent['land-use'].isin(['pasture'])].copy()
    # Keep cropland
    organic_area_share_percent_excluded = organic_area_share_percent_excluded.loc[organic_area_share_percent_excluded['land-use'].isin(['cropland'])].copy()
    # All except land-use (mean)
    organic_area_share_percent_excluded = group_by_dimensions(df=organic_area_share_percent_excluded, groupby_dimensions=['Country'], aggregation_method='Mean')

    # Carbon dynamics - 3.B) Minerals soils (new only ; we only compute change of land)
    # By default, we use FL_x parameters = 1, equation becomes : A * SOC_REF / D (where D = 20 years)
    # In futur : add impact of land management (F_MG and F_I could change)

    # organic-area-share [%]
    organic_area_share_percent_3 = use_variable(input_table=organic_area_share_percent_excluded, selected_variable='organic-area-share[%]')
    # organic-land-area[ha] = land-area[ha] * drained-area-share[%]
    organic_land_area_ha = mcd(input_table_1=land_area_ha_4, input_table_2=organic_area_share_percent_excluded, operation_selection='x * y', output_name='organic-land-area[ha]')
    # carbon-change[tC] = organic-land-area[ha] * organic-soil-emission-factor [tC/ha]  Change = losses (here)
    carbon_change_tC = mcd(input_table_1=organic_land_area_ha, input_table_2=agr_soil_organic_ef_tC_per_ha_excluded_excluded, operation_selection='x * y', output_name='carbon-change[tC]')
    # * (-1) (we loose C-stock)
    carbon_change_tC['carbon-change[tC]'] = carbon_change_tC['carbon-change[tC]']*(-1.0)
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC = group_by_dimensions(df=carbon_change_tC, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = organic-soils
    carbon_change_tC['c-change-origin'] = "organic-soils"
    # All except land-use (mean)
    organic_area_share_percent_2 = group_by_dimensions(df=organic_area_share_percent_2, groupby_dimensions=['Country'], aggregation_method='Mean')

    # Carbon dynamics - 3.B) Minerals soils (new only ; we only compute change of land)
    # By default, we use FL_x parameters = 1, equation becomes : A * SOC_REF / D (where D = 20 years)
    # In futur : add impact of land management (F_MG and F_I could change)

    # organic-area-share [%]
    organic_area_share_percent_5 = use_variable(input_table=organic_area_share_percent_2, selected_variable='organic-area-share[%]')
    # organic-land-area[ha] = land-area[ha] * drained-area-share[%]
    organic_land_area_ha = mcd(input_table_1=land_area_ha_5, input_table_2=organic_area_share_percent_2, operation_selection='x * y', output_name='organic-land-area[ha]')
    # carbon-change[tC] = organic-land-area[ha] * organic-soil-emission-factor [tC/ha]  Change = losses (here)
    carbon_change_tC_2 = mcd(input_table_1=organic_land_area_ha, input_table_2=agr_soil_organic_ef_tC_per_ha_excluded, operation_selection='x * y', output_name='carbon-change[tC]')
    # * (-1) (we loose C-stock)
    carbon_change_tC_2['carbon-change[tC]'] = carbon_change_tC_2['carbon-change[tC]']*(-1.0)
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_2 = group_by_dimensions(df=carbon_change_tC_2, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = organic-soils
    carbon_change_tC_2['c-change-origin'] = "organic-soils"
    carbon_change_tC = pd.concat([carbon_change_tC_2, carbon_change_tC.set_index(carbon_change_tC.index.astype(str) + '_dup')])
    # Group by  Country, Years, land-use, land-age, climate-type (sum)
    land_area_ha_3 = group_by_dimensions(df=land_area_ha_3, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'land-age'], aggregation_method='Sum')
    # organic-land-area[ha] = land-area[ha] * drained-area-share[%]
    organic_land_area_ha = mcd(input_table_1=land_area_ha_3, input_table_2=organic_area_share_percent, operation_selection='x * y', output_name='organic-land-area[ha]')
    # carbon-change[tC] = organic-land-area[ha] * organic-soil-emission-factor [tC/ha]  Change = losses (here)
    carbon_change_tC_2 = mcd(input_table_1=organic_land_area_ha, input_table_2=agr_soil_organic_ef_tC_per_ha_2, operation_selection='x * y', output_name='carbon-change[tC]')
    # * (-1) (we loose C-stock)
    carbon_change_tC_2['carbon-change[tC]'] = carbon_change_tC_2['carbon-change[tC]']*(-1.0)
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_2 = group_by_dimensions(df=carbon_change_tC_2, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = organic-soils
    carbon_change_tC_2['c-change-origin'] = "organic-soils"
    carbon_change_tC = pd.concat([carbon_change_tC_2, carbon_change_tC.set_index(carbon_change_tC.index.astype(str) + '_dup')])

    # Global parameters

    # CP (agr_)above-ground-net-biomass -growth [tdm/ha/yr]
    agr_above_ground_net_biomass_growth_tdm_per_ha_per_yr = import_data(trigram='agr', variable_name='agr_above-ground-net-biomass-growth', variable_type='CP')
    # RCP forest-above-ground-biomass [tdm/ha]
    forest_above_ground_biomass_tdm_per_ha = import_data(trigram='agr', variable_name='forest-above-ground-biomass', variable_type='RCP')
    # RCP forest-above-to-below-biomass -ratio [tdm/tdm]
    forest_above_to_below_biomass_ratio_tdm_per_tdm = import_data(trigram='agr', variable_name='forest-above-to-below-biomass-ratio', variable_type='RCP')
    # biomass-ratio[%] = 1 + forest-above-to-below- biomass-ratio [tdm/tdm]
    biomass_ratio_percent = forest_above_to_below_biomass_ratio_tdm_per_tdm.assign(**{'biomass-ratio[%]': 1.0+forest_above_to_below_biomass_ratio_tdm_per_tdm['forest-above-to-below-biomass-ratio[tdm/tdm]']})
    # biomass-ratio [%]
    biomass_ratio_percent = use_variable(input_table=biomass_ratio_percent, selected_variable='biomass-ratio[%]')
    # biomass-ratio [%]
    biomass_ratio_percent_2 = use_variable(input_table=biomass_ratio_percent, selected_variable='biomass-ratio[%]')

    # Carbon dynamics - 1.C) Biomass : Losses due to wood removal and fuel wood
    # Note : We use above ground-biomass growth to avoid extracting more than the forest regeneration

    # biomass-ratio [%]
    biomass_ratio_percent_2 = use_variable(input_table=biomass_ratio_percent_2, selected_variable='biomass-ratio[%]')
    # biomass-ratio[%] = biomass-ratio[%] + 0.1 (to account for barks)
    biomass_ratio_percent_2['biomass-ratio[%]'] = biomass_ratio_percent_2['biomass-ratio[%]']+0.1
    # ground-biomass[tdm/ha] = forest-above-ground-biomass [tdm/ha] * biomass-ratio[%]
    ground_biomass_tdm_per_ha = mcd(input_table_1=forest_above_ground_biomass_tdm_per_ha, input_table_2=biomass_ratio_percent, operation_selection='x * y', output_name='ground-biomass[tdm/ha]')

    # Carbon dynamics - 1.B) Biomass : Losses due to disturbance

    # ground-biomass [tdm/ha]
    ground_biomass_tdm_per_ha = use_variable(input_table=ground_biomass_tdm_per_ha, selected_variable='ground-biomass[tdm/ha]')
    # Group by  climact-type, land-purpose, ecological-zone (sum)
    agr_above_ground_net_biomass_growth_tdm_per_ha_per_yr = group_by_dimensions(df=agr_above_ground_net_biomass_growth_tdm_per_ha_per_yr, groupby_dimensions=['climate-type', 'ecological-zone', 'land-purpose'], aggregation_method='Sum')
    # ground-net-biomass-growth[tdm/ha] = above-ground-net-biomass-growth [tdm/ha/yr] * biomass-ratio[%]
    ground_net_biomass_growth_tdm_per_ha = mcd(input_table_1=agr_above_ground_net_biomass_growth_tdm_per_ha_per_yr, input_table_2=biomass_ratio_percent, operation_selection='x * y', output_name='ground-net-biomass-growth[tdm/ha]')

    # Carbon dynamics - 1.A) Biomass : Gains

    # ground-net-biomass-growth [tdm/ha]
    ground_net_biomass_growth_tdm_per_ha = use_variable(input_table=ground_net_biomass_growth_tdm_per_ha, selected_variable='ground-net-biomass-growth[tdm/ha]')
    # above-ground-net-biomass-growth [tdm/ha/yr]
    above_ground_net_biomass_growth_tdm_per_ha_per_yr = use_variable(input_table=agr_above_ground_net_biomass_growth_tdm_per_ha_per_yr, selected_variable='above-ground-net-biomass-growth[tdm/ha/yr]')
    # above-ground-net-biomass-growth [tdm/ha/yr]
    above_ground_net_biomass_growth_tdm_per_ha_per_yr = use_variable(input_table=above_ground_net_biomass_growth_tdm_per_ha_per_yr, selected_variable='above-ground-net-biomass-growth[tdm/ha/yr]')
    # OTS/FTS wood-harvest [%]
    wood_harvest_percent = import_data(trigram='agr', variable_name='wood-harvest')
    # wood-harvest[tdm/ha] = above-ground-net-biomass-growth [tdm/ha/yr] * wood-harvest[%]
    wood_harvest_tdm_per_ha = mcd(input_table_1=above_ground_net_biomass_growth_tdm_per_ha_per_yr, input_table_2=wood_harvest_percent, operation_selection='x * y', output_name='wood-harvest[tdm/ha]')
    # Keep forests land-area[ha]
    land_area_ha_3 = land_area_ha.loc[land_area_ha['land-use'].isin(['forest'])].copy()
    land_area_ha_excluded = land_area_ha.loc[~land_area_ha['land-use'].isin(['forest'])].copy()

    # Add caracteristics linked to forest computation

    # OTS (only) cover-area-share [%]
    cover_area_share_percent = import_data(trigram='agr', variable_name='cover-area-share', variable_type='OTS (only)')
    # Same as last available year
    cover_area_share_percent = add_missing_years(df_data=cover_area_share_percent)
    # cover-area-share [%]
    cover_area_share_percent_2 = use_variable(input_table=cover_area_share_percent, selected_variable='cover-area-share[%]')
    # annual-new-land[ha] (replace) = annual-new-land[ha] * cover-area-share[%]
    annual_new_land_ha = mcd(input_table_1=land_area_ha_2, input_table_2=cover_area_share_percent_2, operation_selection='x * y', output_name='annual-new-land[ha]')

    # Carbon dynamics - 2) DOM (only on changes (new / lost land) - cfr IPCC Guidances)

    # annual-new-land [ha] FOREST only
    annual_new_land_ha = use_variable(input_table=annual_new_land_ha, selected_variable='annual-new-land[ha]')
    # / 20 (Ton = 20)
    annual_new_land_ha['annual-new-land[ha]'] = annual_new_land_ha['annual-new-land[ha]']/20.0
    # OTS (only) purpose-area-share [%]
    purpose_area_share_percent = import_data(trigram='agr', variable_name='purpose-area-share', variable_type='OTS (only)')
    # Same as last available year
    purpose_area_share_percent = add_missing_years(df_data=purpose_area_share_percent)
    # area-share[%] (replace) = purpose-area-share[%] * cover-area-share[%]
    area_share_percent = mcd(input_table_1=cover_area_share_percent, input_table_2=purpose_area_share_percent, operation_selection='x * y', output_name='area-share[%]')
    # land-area[ha] (replace) = land-area[ha] * cover-area-share[%]
    land_area_ha = mcd(input_table_1=land_area_ha_3, input_table_2=area_share_percent, operation_selection='x * y', output_name='land-area[ha]')
    # Top : temperate
    land_area_ha_2 = land_area_ha.loc[land_area_ha['climate-type'].isin(['temperate'])].copy()
    land_area_ha_excluded_2 = land_area_ha.loc[~land_area_ha['climate-type'].isin(['temperate'])].copy()
    # All except land-cover (sum)
    land_area_ha_excluded_2 = group_by_dimensions(df=land_area_ha_excluded_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'ecological-zone', 'sub-climate-type', 'land-age', 'land-purpose'], aggregation_method='Sum')
    # land-cover = undefined (we don't need information for climate other than temperate)
    land_area_ha_excluded_2['land-cover'] = "undefined"
    land_area_ha = pd.concat([land_area_ha_2, land_area_ha_excluded_2.set_index(land_area_ha_excluded_2.index.astype(str) + '_dup')])
    # land-area [ha]
    land_area_ha = use_variable(input_table=land_area_ha, selected_variable='land-area[ha]')
    # land-area [ha]
    land_area_ha_2 = use_variable(input_table=land_area_ha, selected_variable='land-area[ha]')
    # OTS/FTS disturbed-area [%]
    disturbed_area_percent = import_data(trigram='agr', variable_name='disturbed-area')
    # disturbed-land-area[ha] = land-area[ha] * disturbed-area[%]
    disturbed_land_area_ha = mcd(input_table_1=land_area_ha_2, input_table_2=disturbed_area_percent, operation_selection='x * y', output_name='disturbed-land-area[ha]')
    # disturbed-above-ground-biomass[tdm] = disturbed-land-area[ha] * ground-biomass [tdm/ha]
    disturbed_above_ground_biomass_tdm = mcd(input_table_1=disturbed_land_area_ha, input_table_2=ground_biomass_tdm_per_ha, operation_selection='x * y', output_name='disturbed-above-ground-biomass[tdm]')
    # land-area [ha]
    land_area_ha_2 = use_variable(input_table=land_area_ha_2, selected_variable='land-area[ha]')
    # removal-wood[tdm] = land-area[ha] * wood-harvest[tdm/ha]
    removal_wood_tdm = mcd(input_table_1=land_area_ha_2, input_table_2=wood_harvest_tdm_per_ha, operation_selection='x * y', output_name='removal-wood[tdm]')

    # Calibration :
    # On wood harvest [tdm]

    # removal-biomass[tdm] = removal-wood[tdm] * biomass-ratio[%]
    removal_biomass_tdm = mcd(input_table_1=removal_wood_tdm, input_table_2=biomass_ratio_percent_2, operation_selection='x * y', output_name='removal-biomass[tdm]')
    # Group by Country, Years, land-use (sum)
    removal_biomass_tdm_2 = group_by_dimensions(df=removal_biomass_tdm, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # Calibration total-removal-wood [tdm]
    total_removal_wood_tdm = import_data(trigram='agr', variable_name='total-removal-wood', variable_type='Calibration')
    # Apply Calibration on wood-harvest[tdm]
    _, out_10036_2, _ = calibration(input_table=removal_biomass_tdm_2, cal_table=total_removal_wood_tdm, data_to_be_cal='removal-biomass[tdm]', data_cal='total-removal-wood[tdm]')
    # removal-biomass[tdm] (replace) = removal-biomass[tdm] * cal-rate
    removal_biomass_tdm_2 = mcd(input_table_1=removal_biomass_tdm, input_table_2=out_10036_2, operation_selection='x * y', output_name='removal-biomass[tdm]')

    # Energy Production

    # Potential for bio-energy production
    # 
    # Objective: calculate the potention for bioenergy production linked to :
    # - Waste (other than food waste) (coming from lifestyle)
    # - Forest management
    # - Settlement management
    # - Energy crops
    # 
    # Other sources of production are computed in agriculture submodules (food-waste, dedicated double crops, crops products, crops co-product and livestock residues)

    # Wood production
    # Used as wood fuel or wood production for industry (construction, paper, ...)

    # removal-biomass [tdm] (from forest)
    removal_biomass_tdm = use_variable(input_table=removal_biomass_tdm_2, selected_variable='removal-biomass[tdm]')
    # OTS (only) total-wood-production [m3]
    total_wood_production_m3 = import_data(trigram='agr', variable_name='total-wood-production', variable_type='OTS (only)')
    # conversion-factor[tdm/m3] = removal-biomass[tdm] / total-wood-production[m3]
    conversion_factor_tdm_per_m3 = mcd(input_table_1=removal_biomass_tdm, input_table_2=total_wood_production_m3, operation_selection='x / y', output_name='conversion-factor[tdm/m3]')
    # Same as last available year (we assume we have  the same "yield" in the future)
    conversion_factor_tdm_per_m3 = add_missing_years(df_data=conversion_factor_tdm_per_m3)
    # wood-production[m3] = removal-biomass[tdm] / conversion-factor[tdm/m3]
    wood_production_m3 = mcd(input_table_1=removal_biomass_tdm, input_table_2=conversion_factor_tdm_per_m3, operation_selection='x / y', output_name='wood-production[m3]')
    # Group by Country, Years, land-cover, land-purpose
    wood_production_m3 = group_by_dimensions(df=wood_production_m3, groupby_dimensions=['Country', 'Years', 'land-cover', 'land-purpose'], aggregation_method='Sum')
    # wood-production [m3]
    wood_production_m3 = export_variable(input_table=wood_production_m3, selected_variable='wood-production[m3]')

    # Formating data for other modules + Pathway Explorer

    # For : Bionergy balance
    # 
    # - Wood production

    # wood-production [m3]
    wood_production_m3_2 = use_variable(input_table=wood_production_m3, selected_variable='wood-production[m3]')
    # Top : land-cover = resinous
    wood_production_m3 = wood_production_m3_2.loc[wood_production_m3_2['land-cover'].isin(['resinous'])].copy()
    wood_production_m3_excluded = wood_production_m3_2.loc[~wood_production_m3_2['land-cover'].isin(['resinous'])].copy()
    # Top : land-purpose = plantation
    wood_production_m3_excluded_2 = wood_production_m3_excluded.loc[wood_production_m3_excluded['land-purpose'].isin(['plantation'])].copy()
    wood_production_m3_excluded_excluded = wood_production_m3_excluded.loc[~wood_production_m3_excluded['land-purpose'].isin(['plantation'])].copy()
    # origin = natural-hardwood
    wood_production_m3_excluded_excluded['origin'] = "natural-hardwood"
    # origin = plantation-hardwood
    wood_production_m3_excluded = wood_production_m3_excluded_2.assign(**{'origin': "plantation-hardwood"})
    wood_production_m3_excluded = pd.concat([wood_production_m3_excluded, wood_production_m3_excluded_excluded.set_index(wood_production_m3_excluded_excluded.index.astype(str) + '_dup')])
    # Top : land-purpose = plantation
    wood_production_m3_2 = wood_production_m3.loc[wood_production_m3['land-purpose'].isin(['plantation'])].copy()
    wood_production_m3_excluded_2 = wood_production_m3.loc[~wood_production_m3['land-purpose'].isin(['plantation'])].copy()
    # origin = natural-resinous
    wood_production_m3_excluded_2['origin'] = "natural-resinous"
    wood_production_m3_excluded = pd.concat([wood_production_m3_excluded_2, wood_production_m3_excluded.set_index(wood_production_m3_excluded.index.astype(str) + '_dup')])
    # origin = plantation-resinous
    wood_production_m3 = wood_production_m3_2.assign(**{'origin': "plantation-resinous"})
    wood_production_m3 = pd.concat([wood_production_m3, wood_production_m3_excluded.set_index(wood_production_m3_excluded.index.astype(str) + '_dup')])
    # above-ground-biomass[tdm] = land-area[ha] * ground-net-biomass-growth [tdm/ha]
    above_ground_biomass_tdm = mcd(input_table_1=land_area_ha, input_table_2=ground_net_biomass_growth_tdm_per_ha, operation_selection='x * y', output_name='above-ground-biomass[tdm]')
    # CP (agr_)carbon-fraction [tC/tdm]
    agr_carbon_fraction_tC_per_tdm = import_data(trigram='agr', variable_name='agr_carbon-fraction', variable_type='CP')
    # Group by climate-type (mean) (Remove unused columns)
    agr_carbon_fraction_tC_per_tdm = group_by_dimensions(df=agr_carbon_fraction_tC_per_tdm, groupby_dimensions=['climate-type'], aggregation_method='Mean')
    # carbon-change[tC] = above-ground-biomass[tdm] * carbon-fraction[tC/tdm]  Change = gains (here)
    carbon_change_tC_2 = mcd(input_table_1=above_ground_biomass_tdm, input_table_2=agr_carbon_fraction_tC_per_tdm, operation_selection='x * y', output_name='carbon-change[tC]')
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_2 = group_by_dimensions(df=carbon_change_tC_2, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = biomass-gains
    carbon_change_tC_2['c-change-origin'] = "biomass-gains"
    # Group by  Country, Years, land-use, land-age (sum)  DO WE WANT TO KEEP THE ORIGIN ?
    carbon_change_tC_2 = group_by_dimensions(df=carbon_change_tC_2, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age', 'c-change-origin'], aggregation_method='Sum')
    # carbon-fraction [tC/tdm]
    carbon_fraction_tC_per_tdm = use_variable(input_table=agr_carbon_fraction_tC_per_tdm, selected_variable='carbon-fraction[tC/tdm]')
    # carbon-change[tC] = removal-biomass[tdm] * carbon-fraction[tC/tdm]  Change = losses (here)
    carbon_change_tC_3 = mcd(input_table_1=carbon_fraction_tC_per_tdm, input_table_2=removal_biomass_tdm_2, operation_selection='x * y', output_name='carbon-change[tC]')
    # * (-1) (we loose C-stock)
    carbon_change_tC_3['carbon-change[tC]'] = carbon_change_tC_3['carbon-change[tC]']*(-1.0)
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_3 = group_by_dimensions(df=carbon_change_tC_3, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')

    # Calibration :
    # On carbon change [tC]

    # Group by Country, Years, land-use (sum)
    carbon_change_tC_4 = group_by_dimensions(df=carbon_change_tC_3, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # Calibration wood-harvest-emissions [tCO2]
    wood_harvest_emissions_tCO2 = import_data(trigram='agr', variable_name='wood-harvest-emissions', variable_type='Calibration')
    # Convert Unit tCO2 to tC (*12/44)
    wood_harvest_emissions_tC = wood_harvest_emissions_tCO2.drop(columns='wood-harvest-emissions[tCO2]').assign(**{'wood-harvest-emissions[tC]': wood_harvest_emissions_tCO2['wood-harvest-emissions[tCO2]'] * 0.27})
    # Apply Calibration on carbon-changet[tC]
    _, out_10105_2, _ = calibration(input_table=carbon_change_tC_4, cal_table=wood_harvest_emissions_tC, data_to_be_cal='carbon-change[tC]', data_cal='wood-harvest-emissions[tC]')
    # carbon-change[tC] = carbon-change[tC] * cal-rate 
    carbon_change_tC_3 = mcd(input_table_1=carbon_change_tC_3, input_table_2=out_10105_2, operation_selection='x * y', output_name='carbon-change[tC]')
    # c-change-origin = wood-removals
    carbon_change_tC_3['c-change-origin'] = "wood-removals"
    # carbon-stock[tC] = above-ground-biomass[tdm] * disturbed-above-ground-biomass [tdm]
    carbon_stock_tC = mcd(input_table_1=carbon_fraction_tC_per_tdm, input_table_2=disturbed_above_ground_biomass_tdm, operation_selection='x * y', output_name='carbon-stock[tC]')
    # CP disturbance-lost-factor (agr_disturbance) [%]
    agr_disturbance_percent = import_data(trigram='agr', variable_name='agr_disturbance', variable_type='CP')
    # Group by  disturbance-type (mean) (remove unused columns)
    agr_disturbance_percent = group_by_dimensions(df=agr_disturbance_percent, groupby_dimensions=['disturbance-type'], aggregation_method='Mean')
    # carbon-change[tC] = carbon-stock[tC] * disturbance-lost-factor[%]  Change = losses (here)
    carbon_change_tC_4 = mcd(input_table_1=carbon_stock_tC, input_table_2=agr_disturbance_percent, operation_selection='x * y', output_name='carbon-change[tC]')
    # * (-1) (we loose C-stock)
    carbon_change_tC_4['carbon-change[tC]'] = carbon_change_tC_4['carbon-change[tC]']*(-1.0)
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_4 = group_by_dimensions(df=carbon_change_tC_4, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = disturbance
    carbon_change_tC_4['c-change-origin'] = "disturbance"

    # Costs
    # CAPEX 
    # 
    # Objective: the node is computing the capex for land change (here only new forest are considered).
    # Note : For forest ; we use land variation and not "reforestation" metric as this latest one only consider reforestation on remaining land (when land use < total land use for the country).
    # With land variation metric we have both : reforestation planned + reforestation on remaining land.
    # 
    # 1. Multiply nb ha of land changed by capex (cost by ha)
    # 
    # Main inputs: 
    # - New forest Land [ha]
    # - CAPEX for new forest land ; set to 5000 EUR/ha (fix amount)
    # 
    # Main outputs: 
    # - Capex for new forest

    # land-area [ha]  (forest only)
    land_area_ha = use_variable(input_table=land_area_ha_3, selected_variable='land-area[ha]')
    # Keep land-age = new
    land_area_ha = land_area_ha.loc[land_area_ha['land-age'].isin(['new'])].copy()
    # Group by Country, Years, land-use (sum)
    land_area_ha = group_by_dimensions(df=land_area_ha, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # CAPEX for new forest area allocated  capex-new-forests[MEUR] =  (Should be THIS ONE ? : reforestation[ha]) land-variation[ha] x 5000 [EUR/ha] (fix amount) / 1.000.000
    capex_new_forest_MEUR = land_area_ha.copy()
    mask = capex_new_forest_MEUR['land-area[ha]']> 0
    capex_new_forest_MEUR.loc[mask, 'capex-new-forest[MEUR]'] =  capex_new_forest_MEUR.loc[mask, 'land-area[ha]'] * 5000 / 1000000
    capex_new_forest_MEUR.loc[~mask, 'capex-new-forest[MEUR]'] =  0
    # OTS/FTS wacc [%] from TEC
    wacc_percent = import_data(trigram='tec', variable_name='wacc')
    # Keep sector = bld (should we have  a specific wacc for agr ??!!)
    wacc_percent = wacc_percent.loc[wacc_percent['sector'].isin(['bld'])].copy()
    # Group by  all except sector (sum)
    wacc_percent = group_by_dimensions(df=wacc_percent, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Assumption lifetime = 25 years
    out_10134_1 = spread_capital(output_table=capex_new_forest_MEUR, df_wacc=wacc_percent)
    # capex-new-forests [MEUR]
    capex_new_forest_MEUR = export_variable(input_table=out_10134_1, selected_variable='capex-new-forest[MEUR]')
    # Keep Years >= baseyear
    capex_new_forest_MEUR, _ = filter_dimension(df=capex_new_forest_MEUR, dimension='Years', operation_selection='≥', value_years=Globals.get().base_year)
    # Keep grassland(s) land-area[ha]
    land_area_ha_excluded_2 = land_area_ha_excluded.loc[land_area_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    land_area_ha_excluded_excluded = land_area_ha_excluded.loc[~land_area_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()

    # Add caracteristics linked to grassland computation

    # Global parameters

    # RCP grassland-above-ground-biomass [tdm/ha]
    grassland_above_ground_biomass_tdm_per_ha = import_data(trigram='agr', variable_name='grassland-above-ground-biomass', variable_type='RCP')
    # RCP grassland-above-to-below-biomass -ratio [tdm/tdm]
    grassland_above_to_below_biomass_ratio_tdm_per_tdm = import_data(trigram='agr', variable_name='grassland-above-to-below-biomass-ratio', variable_type='RCP')
    # biomass-ratio[%] = 1 + grassland-above-to- below-biomass-ratio [tdm/tdm]
    biomass_ratio_percent = grassland_above_to_below_biomass_ratio_tdm_per_tdm.assign(**{'biomass-ratio[%]': 1.0+grassland_above_to_below_biomass_ratio_tdm_per_tdm['grassland-above-to-below-biomass-ratio[tdm/tdm]']})
    # biomass-ratio [%]
    biomass_ratio_percent = use_variable(input_table=biomass_ratio_percent, selected_variable='biomass-ratio[%]')
    # ground-biomass[tdm/ha] = grassland-above-ground-biomass [tdm/ha] * biomass-ratio[%]
    ground_biomass_tdm_per_ha = mcd(input_table_1=grassland_above_ground_biomass_tdm_per_ha, input_table_2=biomass_ratio_percent, operation_selection='x * y', output_name='ground-biomass[tdm/ha]')

    # Carbon dynamics - 1.A) Biomass
    # Tier 1 approach assumes no change in biomass in Grassland Remaining Grassland.
    # In grassland where there is no change in either type or intensity of management, biomass will be in an approximate steady-state (i.e., carbon accumulation through plant growth is roughly balanced by losses through grazing, decomposition and fire)​.​
    # 
    # Equation 2.16 : Sum[(B_AFTER[tdm/ha] - B_BEFORE[tdm/ha]) * A_to_others[ha/yr]] * CF[tC/tdm]
    # Where : 
    #  * B_AFTER = 0 (cfr IPCC Guidance)
    #  * Sum = on i (where i = type of land use converted to another land-use category)
    #  
    # We only consider non-woody biomass (we don't have biomass stock (tdm/ha) for woody biomass).
    # In the futur, if we want to use woody vs non-woody biomass, we should apply a % to determine the part of biomass that is woody vs the one that is non-woody

    # ground-biomass [tdm/ha]
    ground_biomass_tdm_per_ha = use_variable(input_table=ground_biomass_tdm_per_ha, selected_variable='ground-biomass[tdm/ha]')
    # land-area [ha]
    land_area_ha = use_variable(input_table=land_area_ha_excluded_2, selected_variable='land-area[ha]')
    # Keep only new land (=cum new) 
    land_area_ha = land_area_ha.loc[land_area_ha['land-age'].isin(['new'])].copy()
    # Group by  Country, Years, land-use, climate, sub-climate (sum)
    land_area_ha = group_by_dimensions(df=land_area_ha, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type'], aggregation_method='Sum')
    # above-ground-biomass[tdm] = land-change[ha] * ground-net-biomass-growth [tdm/ha]
    above_ground_biomass_tdm_2 = mcd(input_table_1=land_area_ha, input_table_2=ground_biomass_tdm_per_ha, operation_selection='x * y', output_name='above-ground-biomass[tdm]')
    # Keep cropland(s) land-area[ha]
    land_area_ha_excluded_excluded_2 = land_area_ha_excluded_excluded.loc[land_area_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    land_area_ha_excluded_excluded_excluded = land_area_ha_excluded_excluded.loc[~land_area_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    # Keep settlement land-area[ha]
    land_area_ha_excluded_excluded_excluded = land_area_ha_excluded_excluded_excluded.loc[land_area_ha_excluded_excluded_excluded['land-use'].isin(['settlement'])].copy()

    # Add caracteristics linked to settlement computation

    # OTS only settlement-tree-cover [%]  A terme, avoir un levier sur ceci !!!
    settlement_tree_cover_percent = import_data(trigram='agr', variable_name='settlement-tree-cover', variable_type='OTS (only)')
    # Same as last available year
    settlement_tree_cover_percent = add_missing_years(df_data=settlement_tree_cover_percent)
    # land-area[ha] (replace) = land-area[ha] * settlement-tree-cover[%]
    land_area_ha = mcd(input_table_1=land_area_ha_excluded_excluded_excluded, input_table_2=settlement_tree_cover_percent, operation_selection='x * y', output_name='land-area[ha]')

    # Carbon dynamics - 1.C) Biomass : Losses due to wood removal and fuel wood
    # Note : WE SHOULD APPLY COMPUTATION FOR GREEN WASTES used in bioenergy production !!!!!!!!!!!!!

    # land-area [ha] (settlement tree cover)
    land_area_ha_2 = use_variable(input_table=land_area_ha, selected_variable='land-area[ha]')
    # OTS (only) biomass-yields [tons/ha]
    biomass_yields_tons_per_ha = import_data(trigram='agr', variable_name='biomass-yields', variable_type='OTS (only)')
    # Same as last available year
    biomass_yields_tons_per_ha = add_missing_years(df_data=biomass_yields_tons_per_ha)
    # Group by Country, Years, land-use (sum)
    land_area_ha_2 = group_by_dimensions(df=land_area_ha_2, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # potential-biomass-production[t] = land-area[ha] * biomass-yields[t/ha]
    potential_biomass_production_t = mcd(input_table_1=land_area_ha_2, input_table_2=biomass_yields_tons_per_ha, operation_selection='x * y', output_name='potential-biomass-production[t]')

    # Apply biomass supply valorisation of co-products and co levers (switch)
    # => determine % of settlement biomass which is effectively used to produce biogaz

    # OTS/FTS biomass-bioenergy-share [%]
    biomass_bioenergy_share_percent = import_data(trigram='agr', variable_name='biomass-bioenergy-share')
    # biomass-harvested[t] = potential-biomass-production[t] * biomass-bioenergy-share[%]
    biomass_harvested_t = mcd(input_table_1=potential_biomass_production_t, input_table_2=biomass_bioenergy_share_percent, operation_selection='x * y', output_name='biomass-harvested[t]')
    # biomass-harvested [t]
    biomass_harvested_t = export_variable(input_table=biomass_harvested_t, selected_variable='biomass-harvested[t]')

    # Settlement => green wastes (élagage, ...)

    # biomass-harvested [t]
    biomass_harvested_t = use_variable(input_table=biomass_harvested_t, selected_variable='biomass-harvested[t]')
    # CP land-management-bioenergy-conv-factors [TWh/ton]
    land_management_bioenergy_conv_factors_TWh_per_ton = import_data(trigram='agr', variable_name='land-management-bioenergy-conv-factors', variable_type='RCP')
    # energy-production[TWh] = biomass-harvested[t] * land-management-bioenergy-conv-factors [TWh/ton]
    energy_production_TWh = mcd(input_table_1=biomass_harvested_t, input_table_2=land_management_bioenergy_conv_factors_TWh_per_ton, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh = group_by_dimensions(df=energy_production_TWh, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = settlement
    energy_production_TWh['origin'] = "settlement"
    # Keep only new land (=cum new)
    land_area_ha = land_area_ha.loc[land_area_ha['land-age'].isin(['new'])].copy()
    # Keep only new land (=cum new)
    land_area_ha_excluded_excluded = land_area_ha_excluded_excluded_2.loc[land_area_ha_excluded_excluded_2['land-age'].isin(['new'])].copy()

    # Carbon dynamics - 1.C) Biomass : Losses due to wood removal and fuel wood
    # Note : no wood / fuel wood removal from cropland
    # Should we consider export of crops ? (all cropland are dedicated to be harvest, non ?)

    # land-area [ha] (all types of cropland)
    land_area_ha_2 = use_variable(input_table=land_area_ha_excluded_excluded, selected_variable='land-area[ha]')
    # Group by Country, Years, land-use (sum)
    land_area_ha_2 = group_by_dimensions(df=land_area_ha_2, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # OTS (only) energy-crop-yields [tons/ha]
    energy_crop_yields_tons_per_ha = import_data(trigram='agr', variable_name='energy-crop-yields', variable_type='OTS (only)')
    # Same as last available year
    energy_crop_yields_tons_per_ha = add_missing_years(df_data=energy_crop_yields_tons_per_ha)
    # potential-biomass-production[t] = land-area[ha] * energy-crops-yields[t/ha]  We consider all is harvested
    potential_biomass_production_t = mcd(input_table_1=land_area_ha_2, input_table_2=energy_crop_yields_tons_per_ha, operation_selection='x * y', output_name='potential-biomass-production[t]')
    # potential-biomass-production [t]
    potential_biomass_production_t = export_variable(input_table=potential_biomass_production_t, selected_variable='potential-biomass-production[t]')

    # Industrial and energy crops
    # These crops are grown for industrial and energy purpose.
    # Here, we estimate the amount of this kind of crops. Then we assume the % of crops that is used for energy suply (the rest = for industrial purpose)

    # potential-biomass-production [t]
    potential_biomass_production_t = use_variable(input_table=potential_biomass_production_t, selected_variable='potential-biomass-production[t]')

    # Apply import-energy-crops levers (switch)
    # => determine % of energy crops that are imported / exported

    # OTS/FTS energy-crops-share [%]
    energy_crops_share_percent = import_data(trigram='agr', variable_name='energy-crops-share')
    # potential-production-for-industry[t] = potential-biomass-production[t] * (1 - energy-crops-share[%])
    potential_production_for_industry_t = mcd(input_table_1=potential_biomass_production_t, input_table_2=energy_crops_share_percent, operation_selection='x * (1-y)', output_name='potential-production-for-industry[t]')
    # origin = industrial-crops
    potential_production_for_industry_t['origin'] = "industrial-crops"
    # potential-production-for-industry [t]
    potential_production_for_industry_t = export_variable(input_table=potential_production_for_industry_t, selected_variable='potential-production-for-industry[t]')

    # For : Bionergy balance
    # 
    # - Potential for industry

    # potential-production-for-industry [t]
    potential_production_for_industry_t = use_variable(input_table=potential_production_for_industry_t, selected_variable='potential-production-for-industry[t]')
    production = pd.concat([wood_production_m3, potential_production_for_industry_t.set_index(potential_production_for_industry_t.index.astype(str) + '_dup')])
    # potential-biomass-production[t] (replace) = potential-biomass-production[t] * energy-crops-share[%]
    potential_biomass_production_t = mcd(input_table_1=potential_biomass_production_t, input_table_2=energy_crops_share_percent, operation_selection='x * y', output_name='potential-biomass-production[t]')
    # RCP energy-crops-conversion-factor [TWh/t]
    energy_crops_conversion_factor_TWh_per_t = import_data(trigram='agr', variable_name='energy-crops-conversion-factor', variable_type='RCP')
    # energy-production[TWh] = potentiel-biomass-production[t] * energy-crops-conversion-factor [TWh/t]
    energy_production_TWh_2 = mcd(input_table_1=potential_biomass_production_t, input_table_2=energy_crops_conversion_factor_TWh_per_t, operation_selection='x * y', output_name='energy-production[TWh]')
    # Group by  Country, Years, energy-carrier (sum)
    energy_production_TWh_2 = group_by_dimensions(df=energy_production_TWh_2, groupby_dimensions=['Country', 'Years', 'energy-carrier'], aggregation_method='Sum')
    # origin = energy-crops
    energy_production_TWh_2['origin'] = "energy-crops"
    energy_production_TWh = pd.concat([energy_production_TWh_2, energy_production_TWh.set_index(energy_production_TWh.index.astype(str) + '_dup')])
    # energy-production [TWh]
    energy_production_TWh = export_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')

    # For : Bionergy balance
    # 
    # - Energy production

    # energy-production [TWh]
    energy_production_TWh = use_variable(input_table=energy_production_TWh, selected_variable='energy-production[TWh]')
    production = pd.concat([energy_production_TWh, production.set_index(production.index.astype(str) + '_dup')])
    # Module = Bioenergy
    production = column_filter(df=production, pattern='^.*$')

    # Carbon dynamics - 1.A) Biomass
    # Tier 1 approach assumes gains in C-stock only on woody perennial croplands (archards, agroforestery).
    # It is assumed that gains occurs during all the growing phase (we use cumulated new area) and loss occurs only the year of harvest / cropland area decrease (we use annual lost).
    # Assumption : new land area are never lost (lost applied only on remaining land). Therefore, we never apply losses on "cumulative new area"​

    # RCP biomass-accumulation-rate [tC/ha/yr]
    biomass_accumulation_rate_tC_per_ha_per_yr = import_data(trigram='agr', variable_name='biomass-accumulation-rate', variable_type='RCP')

    # Carbon dynamics - 1.A) Biomass
    # Assumption : woody settlement are considered as woody cropland
    # ​

    # biomass-accumulation-rate [tC/ha/yr]
    biomass_accumulation_rate_tC_per_ha_per_yr_2 = use_variable(input_table=biomass_accumulation_rate_tC_per_ha_per_yr, selected_variable='biomass-accumulation-rate[tC/ha/yr]')
    # Keep land-use = wood-cropland
    biomass_accumulation_rate_tC_per_ha_per_yr_2 = biomass_accumulation_rate_tC_per_ha_per_yr_2.loc[biomass_accumulation_rate_tC_per_ha_per_yr_2['land-use'].isin(['woody-cropland'])].copy()
    # Group by all dimensions except land-use (sum)
    biomass_accumulation_rate_tC_per_ha_per_yr_2 = group_by_dimensions(df=biomass_accumulation_rate_tC_per_ha_per_yr_2, groupby_dimensions=['Country', 'climate-type', 'sub-climate-type'], aggregation_method='Sum')
    # carbon-change[tC] = land-area[ha] (only new lands) * biomass-accumulation-rate [tC/ha/yr]  Change = gains (here)
    carbon_change_tC_7 = mcd(input_table_1=land_area_ha, input_table_2=biomass_accumulation_rate_tC_per_ha_per_yr_2, operation_selection='x * y', output_name='carbon-change[tC]')
    # carbon-change[tC] = land-area[ha] (only new lands) * biomass-accumulation-rate [tC/ha/yr]  Change = gains (here)
    carbon_change_tC_6 = mcd(input_table_1=land_area_ha_excluded_excluded, input_table_2=biomass_accumulation_rate_tC_per_ha_per_yr, operation_selection='x * y', output_name='carbon-change[tC]')
    # annual-lost-land [ha]
    annual_lost_land_ha = use_variable(input_table=out_688_1, selected_variable='annual-lost-land[ha]')
    # Keep grassland(s) annual-lost-land[ha]
    annual_lost_land_ha_2 = annual_lost_land_ha.loc[annual_lost_land_ha['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    annual_lost_land_ha_excluded = annual_lost_land_ha.loc[~annual_lost_land_ha['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    # annual-lost-land [ha]
    annual_lost_land_ha_2 = use_variable(input_table=annual_lost_land_ha_2, selected_variable='annual-lost-land[ha]')
    # Group by  Country, Years, land-use, climate, sub-climate (sum)
    annual_lost_land_ha_2 = group_by_dimensions(df=annual_lost_land_ha_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type'], aggregation_method='Sum')
    # above-ground-biomass[tdm] = annual-lost-land[ha] * ground-net-biomass-growth [tdm/ha]
    above_ground_biomass_tdm = mcd(input_table_1=annual_lost_land_ha_2, input_table_2=ground_biomass_tdm_per_ha, operation_selection='x * y', output_name='above-ground-biomass[tdm]')
    above_ground_biomass_tdm = pd.concat([above_ground_biomass_tdm_2, above_ground_biomass_tdm.set_index(above_ground_biomass_tdm.index.astype(str) + '_dup')])
    # carbon-change[tC] = above-ground-biomass[tdm] * carbon-fraction[tC/tdm] (= 0.47tC/tdm for herbaceous-grassland)
    carbon_change_tC_5 = above_ground_biomass_tdm.assign(**{'carbon-change[tC]': above_ground_biomass_tdm['above-ground-biomass[tdm]']*0.47})
    # carbon-change[tC]
    carbon_change_tC_5 = use_variable(input_table=carbon_change_tC_5, selected_variable='carbon-change[tC]')
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_5 = group_by_dimensions(df=carbon_change_tC_5, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = biomass-gains
    carbon_change_tC_5['c-change-origin'] = "biomass-gains"
    # Group by  Country, Years, land-use, land-age (sum)  DO WE WANT TO KEEP THE ORIGIN ?
    carbon_change_tC_5 = group_by_dimensions(df=carbon_change_tC_5, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age', 'c-change-origin'], aggregation_method='Sum')
    # Keep cropland(s) annual-lost-land[ha]
    annual_lost_land_ha_excluded_2 = annual_lost_land_ha_excluded.loc[annual_lost_land_ha_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    annual_lost_land_ha_excluded_excluded = annual_lost_land_ha_excluded.loc[~annual_lost_land_ha_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    # Keep settlement annual-lost-land[ha]
    annual_lost_land_ha_excluded_excluded = annual_lost_land_ha_excluded_excluded.loc[annual_lost_land_ha_excluded_excluded['land-use'].isin(['settlement'])].copy()
    # annual-lost-land[ha] (replace) = annual-lost-land[ha] * settlement-tree-cover[%]
    annual_lost_land_ha_2 = mcd(input_table_1=annual_lost_land_ha_excluded_excluded, input_table_2=settlement_tree_cover_percent, operation_selection='x * y', output_name='annual-lost-land[ha]')
    # annual-lost-land [ha]
    annual_lost_land_ha_3 = use_variable(input_table=annual_lost_land_ha_2, selected_variable='annual-lost-land[ha]')
    # annual-lost-land [ha]
    annual_lost_land_ha_2 = use_variable(input_table=annual_lost_land_ha_excluded_2, selected_variable='annual-lost-land[ha]')
    # RCP biomass-carbon-loss [tC/ha/yr]
    biomass_carbon_loss_tC_per_ha_per_yr = import_data(trigram='agr', variable_name='biomass-carbon-loss', variable_type='RCP')
    # biomass-carbon-loss [tC/ha/yr]
    biomass_carbon_loss_tC_per_ha_per_yr_2 = use_variable(input_table=biomass_carbon_loss_tC_per_ha_per_yr, selected_variable='biomass-carbon-loss[tC/ha/yr]')
    # Keep land-use = wood-cropland
    biomass_carbon_loss_tC_per_ha_per_yr_2 = biomass_carbon_loss_tC_per_ha_per_yr_2.loc[biomass_carbon_loss_tC_per_ha_per_yr_2['land-use'].isin(['woody-cropland'])].copy()
    # Group by all dimensions except land-use (sum)
    biomass_carbon_loss_tC_per_ha_per_yr_2 = group_by_dimensions(df=biomass_carbon_loss_tC_per_ha_per_yr_2, groupby_dimensions=['Country', 'climate-type', 'sub-climate-type'], aggregation_method='Sum')
    # carbon-change[tC] = annual-lost-land[ha] * biomass-carbon-loss [tC/ha/yr]  Change = losses (here) alreadynegative values as lost land are negative
    carbon_change_tC_8 = mcd(input_table_1=annual_lost_land_ha_3, input_table_2=biomass_carbon_loss_tC_per_ha_per_yr_2, operation_selection='x * y', output_name='carbon-change[tC]')
    # land-age = new (<= 20 years)
    carbon_change_tC_8['land-age'] = "new"
    carbon_change_tC_7 = pd.concat([carbon_change_tC_7, carbon_change_tC_8.set_index(carbon_change_tC_8.index.astype(str) + '_dup')])
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_7 = group_by_dimensions(df=carbon_change_tC_7, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = biomass-gains
    carbon_change_tC_7['c-change-origin'] = "biomass-gains"
    # Group by  Country, Years, land-use, land-age (sum)  DO WE WANT TO KEEP THE ORIGIN ?
    carbon_change_tC_7 = group_by_dimensions(df=carbon_change_tC_7, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age', 'c-change-origin'], aggregation_method='Sum')
    # carbon-change[tC] = annual-lost-land[ha] * biomass-carbon-loss [tC/ha/yr]  Change = losses (here) alreadynegative values as lost land are negative
    carbon_change_tC_8 = mcd(input_table_1=annual_lost_land_ha_2, input_table_2=biomass_carbon_loss_tC_per_ha_per_yr, operation_selection='x * y', output_name='carbon-change[tC]')
    # land-age = new (<= 20 years)
    carbon_change_tC_8['land-age'] = "new"
    carbon_change_tC_6 = pd.concat([carbon_change_tC_6, carbon_change_tC_8.set_index(carbon_change_tC_8.index.astype(str) + '_dup')])
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_6 = group_by_dimensions(df=carbon_change_tC_6, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = biomass-gains
    carbon_change_tC_6['c-change-origin'] = "biomass-gains"
    # Group by  Country, Years, land-use, land-age (sum)  DO WE WANT TO KEEP THE ORIGIN ?
    carbon_change_tC_6 = group_by_dimensions(df=carbon_change_tC_6, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age', 'c-change-origin'], aggregation_method='Sum')
    carbon_change_tC_6 = pd.concat([carbon_change_tC_6, carbon_change_tC_7.set_index(carbon_change_tC_7.index.astype(str) + '_dup')])
    carbon_change_tC_5 = pd.concat([carbon_change_tC_5, carbon_change_tC_6.set_index(carbon_change_tC_6.index.astype(str) + '_dup')])
    carbon_change_tC_2 = pd.concat([carbon_change_tC_2, carbon_change_tC_5.set_index(carbon_change_tC_5.index.astype(str) + '_dup')])
    carbon_change_tC_2 = pd.concat([carbon_change_tC_2, carbon_change_tC_4.set_index(carbon_change_tC_4.index.astype(str) + '_dup')])
    carbon_change_tC_2 = pd.concat([carbon_change_tC_2, carbon_change_tC_3.set_index(carbon_change_tC_3.index.astype(str) + '_dup')])
    # annual-lost-land [ha]
    annual_lost_land_ha = use_variable(input_table=annual_lost_land_ha, selected_variable='annual-lost-land[ha]')
    # Keep forests annual-lost-land[ha]
    annual_lost_land_ha = annual_lost_land_ha.loc[annual_lost_land_ha['land-use'].isin(['forest'])].copy()
    # annual-lost-land[ha] (replace) = annual-lost-land[ha] * cover-area-share[%]
    annual_lost_land_ha = mcd(input_table_1=annual_lost_land_ha, input_table_2=cover_area_share_percent_2, operation_selection='x * y', output_name='annual-lost-land[ha]')
    # annual-lost-land [ha] FOREST only
    annual_lost_land_ha = use_variable(input_table=annual_lost_land_ha, selected_variable='annual-lost-land[ha]')
    # land-change[ha] = annual-new-land[ha] + annual-lost-land[ha]
    land_change_ha = mcd(input_table_1=annual_new_land_ha, input_table_2=annual_lost_land_ha, operation_selection='x + y', output_name='land-change[ha]')
    # CP (agr_)litter-carbon-stock [tC/ha]
    agr_litter_carbon_stock_tC_per_ha = import_data(trigram='agr', variable_name='agr_litter-carbon-stock', variable_type='CP')
    # Remove unused columns (source, module, data-type) (mean)
    agr_litter_carbon_stock_tC_per_ha = group_by_dimensions(df=agr_litter_carbon_stock_tC_per_ha, groupby_dimensions=['climate-type', 'sub-climate-type', 'land-use', 'land-cover'], aggregation_method='Mean')
    # carbon-change[tC] = land-change[ha] * litter-carbon-stock[tC/ha]
    carbon_change_tC_3 = mcd(input_table_1=land_change_ha, input_table_2=agr_litter_carbon_stock_tC_per_ha, operation_selection='x * y', output_name='carbon-change[tC]')
    # land-age = new (<= 20 years)
    carbon_change_tC_3['land-age'] = "new"
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_3 = group_by_dimensions(df=carbon_change_tC_3, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = DOM
    carbon_change_tC_3['c-change-origin'] = "DOM"
    carbon_change_tC_2 = pd.concat([carbon_change_tC_2, carbon_change_tC_3.set_index(carbon_change_tC_3.index.astype(str) + '_dup')])
    carbon_change_tC = pd.concat([carbon_change_tC_2, carbon_change_tC.set_index(carbon_change_tC.index.astype(str) + '_dup')])
    # land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha = use_variable(input_table=out_688_1, selected_variable='land-management_lifetime-lagged[ha]')
    # land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha = use_variable(input_table=land_management_lifetime_lagged_ha, selected_variable='land-management_lifetime-lagged[ha]')

    # Add caracteristics linked to Mineral soil computation

    # RCP soil-area-share [%]
    soil_area_share_percent = import_data(trigram='agr', variable_name='soil-area-share', variable_type='RCP')
    # land-management- lifetime-lagged [ha] (replace) = land-management- lifetime-lagged [ha] * soil-area-share[%]
    land_management_lifetime_lagged_ha = mcd(input_table_1=land_management_lifetime_lagged_ha, input_table_2=soil_area_share_percent, operation_selection='x * y', output_name='land-management-lifetime-lagged[ha]')
    # Exclude forests land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha_2 = land_management_lifetime_lagged_ha.loc[land_management_lifetime_lagged_ha['land-use'].isin(['forest'])].copy()
    land_management_lifetime_lagged_ha_excluded = land_management_lifetime_lagged_ha.loc[~land_management_lifetime_lagged_ha['land-use'].isin(['forest'])].copy()
    # Keep grassland(s) land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha_excluded_2 = land_management_lifetime_lagged_ha_excluded.loc[land_management_lifetime_lagged_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    land_management_lifetime_lagged_ha_excluded_excluded = land_management_lifetime_lagged_ha_excluded.loc[~land_management_lifetime_lagged_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    # Group by  Country, Years, land-use, climate-type, sub-climate-type, soil-type (sum)
    land_management_lifetime_lagged_ha_excluded = group_by_dimensions(df=land_management_lifetime_lagged_ha_excluded_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # Keep cropland(s) land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha_excluded_excluded_2 = land_management_lifetime_lagged_ha_excluded_excluded.loc[land_management_lifetime_lagged_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    land_management_lifetime_lagged_ha_excluded_excluded_excluded = land_management_lifetime_lagged_ha_excluded_excluded.loc[~land_management_lifetime_lagged_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    # Group by  Country, Years, land-use, climate-type, sub-climate-type (sum)
    land_management_lifetime_lagged_ha_excluded_excluded = group_by_dimensions(df=land_management_lifetime_lagged_ha_excluded_excluded_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # Keep settlement land-management- lifetime-lagged [ha]
    land_management_lifetime_lagged_ha_excluded_excluded_excluded = land_management_lifetime_lagged_ha_excluded_excluded_excluded.loc[land_management_lifetime_lagged_ha_excluded_excluded_excluded['land-use'].isin(['settlement'])].copy()

    # Carbon dynamics - 3.B) Minerals soils (new only ; we only compute change of land)
    # By default, we use FL_x parameters = 1, equation becomes : A * SOC_REF / D (where D = 20 years)
    # For settlement, we consider 100% soil = minerals

    # Group by  Country, Years, land-use, climate-type, sub-climate-type (sum)
    land_management_lifetime_lagged_ha_excluded_excluded_excluded = group_by_dimensions(df=land_management_lifetime_lagged_ha_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # Group by  Country, Years, land-use, climate-type, sub-climate-type, soil-type (sum)
    land_management_lifetime_lagged_ha = group_by_dimensions(df=land_management_lifetime_lagged_ha_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # land-management [ha]
    land_management_ha_2 = use_variable(input_table=out_688_1, selected_variable='land-management[ha]')
    # land-management [ha]
    land_management_ha_2 = use_variable(input_table=land_management_ha_2, selected_variable='land-management[ha]')
    # land-management[ha] (replace) = land-management[ha] * soil-area-share[%]
    land_management_ha_2 = mcd(input_table_1=land_management_ha_2, input_table_2=soil_area_share_percent, operation_selection='x * y', output_name='land-management[ha]')
    # Keep forests land-management[ha]
    land_management_ha_3 = land_management_ha_2.loc[land_management_ha_2['land-use'].isin(['forest'])].copy()
    land_management_ha_excluded = land_management_ha_2.loc[~land_management_ha_2['land-use'].isin(['forest'])].copy()
    # Keep grassland(s) land-management[ha]
    land_management_ha_excluded_2 = land_management_ha_excluded.loc[land_management_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    land_management_ha_excluded_excluded = land_management_ha_excluded.loc[~land_management_ha_excluded['land-use'].isin(['natural-prairies', 'pasture'])].copy()
    # Group by  Country, Years, land-use, climate-type, sub-climate-type, soil-type (sum)
    land_management_ha_excluded = group_by_dimensions(df=land_management_ha_excluded_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # land-change[ha] (based on a 20 year lifespan) = land-management [ha] - land-management- lifetime-lagged [ha]
    land_change_ha = mcd(input_table_1=land_management_ha_excluded, input_table_2=land_management_lifetime_lagged_ha_excluded, operation_selection='x - y', output_name='land-change[ha]')
    # mineral-land-change[ha] = land-change[ha] * (1-organic-area-share[%]
    mineral_land_change_ha = mcd(input_table_1=land_change_ha, input_table_2=organic_area_share_percent_5, operation_selection='x * (1-y)', output_name='mineral-land-change[ha]')
    # Keep cropland(s) land-management[ha]
    land_management_ha_excluded_excluded_2 = land_management_ha_excluded_excluded.loc[land_management_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    land_management_ha_excluded_excluded_excluded = land_management_ha_excluded_excluded.loc[~land_management_ha_excluded_excluded['land-use'].isin(['woody-cropland', 'non-woody-cropland', 'industrial-and-energy-crop'])].copy()
    # Group by  Country, Years, land-use, climate-type, sub-climate-type (sum)
    land_management_ha_excluded_excluded = group_by_dimensions(df=land_management_ha_excluded_excluded_2, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # land-change[ha] (based on a 20 year lifespan) = land-management [ha] - land-management- lifetime-lagged [ha]
    land_change_ha = mcd(input_table_1=land_management_ha_excluded_excluded, input_table_2=land_management_lifetime_lagged_ha_excluded_excluded, operation_selection='x - y', output_name='land-change[ha]')
    # mineral-land-change[ha] = land-change[ha] * (1-organic-area-share[%]
    mineral_land_change_ha_2 = mcd(input_table_1=land_change_ha, input_table_2=organic_area_share_percent_3, operation_selection='x * (1-y)', output_name='mineral-land-change[ha]')
    # Keep settlement land-management[ha]
    land_management_ha_excluded_excluded_excluded = land_management_ha_excluded_excluded_excluded.loc[land_management_ha_excluded_excluded_excluded['land-use'].isin(['settlement'])].copy()
    # Group by  Country, Years, land-use, climate-type, sub-climate-type (sum)
    land_management_ha_excluded_excluded_excluded = group_by_dimensions(df=land_management_ha_excluded_excluded_excluded, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # land-change[ha] (based on a 20 year lifespan) = land-management [ha] - land-management- lifetime-lagged [ha]
    land_change_ha_2 = mcd(input_table_1=land_management_ha_excluded_excluded_excluded, input_table_2=land_management_lifetime_lagged_ha_excluded_excluded_excluded, operation_selection='x - y', output_name='land-change[ha]')
    # Group by  Country, Years, land-use, climate-type, sub-climate-type, soil-type (sum)
    land_management_ha_2 = group_by_dimensions(df=land_management_ha_3, groupby_dimensions=['Country', 'Years', 'land-use', 'climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Sum')
    # land-change[ha] (based on a 20 year lifespan) = land-management [ha] - land-management- lifetime-lagged [ha]
    land_change_ha = mcd(input_table_1=land_management_ha_2, input_table_2=land_management_lifetime_lagged_ha, operation_selection='x - y', output_name='land-change[ha]')
    # mineral-land-change[ha] = land-change[ha] * (1-organic-area-share[%]
    mineral_land_change_ha_3 = mcd(input_table_1=land_change_ha, input_table_2=organic_area_share_percent_4, operation_selection='x * (1-y)', output_name='mineral-land-change[ha]')
    # CP soil-organic-c-stock (agr_soil-organic-stock) [tC/ha]
    agr_soil_organic_stock_tC_per_ha = import_data(trigram='agr', variable_name='agr_soil-organic-stock', variable_type='CP')
    # Remove unused columns (data_type, source, module) (mean)
    agr_soil_organic_stock_tC_per_ha = group_by_dimensions(df=agr_soil_organic_stock_tC_per_ha, groupby_dimensions=['climate-type', 'sub-climate-type', 'soil-type'], aggregation_method='Mean')
    # carbon-change[tC] = mineral-land-change[ha] * soil-organic-c-stock[tC/ha]
    carbon_change_tC_2 = mcd(input_table_1=mineral_land_change_ha_3, input_table_2=agr_soil_organic_stock_tC_per_ha, operation_selection='x * y', output_name='carbon-change[tC]')
    # carbon-change[tC] (replace) = carbon-change[tC] / 20
    carbon_change_tC_2['carbon-change[tC]'] = carbon_change_tC_2['carbon-change[tC]']/20.0
    # land-age = new (<= 20 years)
    carbon_change_tC_2['land-age'] = "new"
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_2 = group_by_dimensions(df=carbon_change_tC_2, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = mineral-soils
    carbon_change_tC_2['c-change-origin'] = "mineral-soils"
    # soil-organic-c-stock [tC/ha]
    soil_organic_c_stock_tC_per_ha = use_variable(input_table=agr_soil_organic_stock_tC_per_ha, selected_variable='soil-organic-c-stock[tC/ha]')
    # carbon-change[tC] = mineral-land-change[ha] * soil-organic-c-stock[tC/ha]
    carbon_change_tC_3 = mcd(input_table_1=mineral_land_change_ha, input_table_2=soil_organic_c_stock_tC_per_ha, operation_selection='x * y', output_name='carbon-change[tC]')
    # carbon-change[tC] (replace) = carbon-change[tC] / 20
    carbon_change_tC_3['carbon-change[tC]'] = carbon_change_tC_3['carbon-change[tC]']/20.0
    # land-age = new (<= 20 years)
    carbon_change_tC_3['land-age'] = "new"
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_3 = group_by_dimensions(df=carbon_change_tC_3, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = mineral-soils
    carbon_change_tC_3['c-change-origin'] = "mineral-soils"
    # soil-organic-c-stock [tC/ha]
    soil_organic_c_stock_tC_per_ha = use_variable(input_table=soil_organic_c_stock_tC_per_ha, selected_variable='soil-organic-c-stock[tC/ha]')
    # carbon-change[tC] = mineral-land-change[ha] * soil-organic-c-stock[tC/ha]
    carbon_change_tC_4 = mcd(input_table_1=mineral_land_change_ha_2, input_table_2=soil_organic_c_stock_tC_per_ha, operation_selection='x * y', output_name='carbon-change[tC]')
    # carbon-change[tC] (replace) = carbon-change[tC] / 20
    carbon_change_tC_4['carbon-change[tC]'] = carbon_change_tC_4['carbon-change[tC]']/20.0
    # land-age = new (<= 20 years)
    carbon_change_tC_4['land-age'] = "new"
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_4 = group_by_dimensions(df=carbon_change_tC_4, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = mineral-soils
    carbon_change_tC_4['c-change-origin'] = "mineral-soils"
    # soil-organic-c-stock [tC/ha]
    soil_organic_c_stock_tC_per_ha = use_variable(input_table=soil_organic_c_stock_tC_per_ha, selected_variable='soil-organic-c-stock[tC/ha]')
    # carbon-change[tC] = land-change[ha] * soil-organic-c-stock[tC/ha]
    carbon_change_tC_5 = mcd(input_table_1=land_change_ha_2, input_table_2=soil_organic_c_stock_tC_per_ha, operation_selection='x * y', output_name='carbon-change[tC]')
    # carbon-change[tC] (replace) = carbon-change[tC] / 20
    carbon_change_tC_5['carbon-change[tC]'] = carbon_change_tC_5['carbon-change[tC]']/20.0
    # land-age = new (<= 20 years)
    carbon_change_tC_5['land-age'] = "new"
    # Group by  Country, Years, land-use, land-age (sum)
    carbon_change_tC_5 = group_by_dimensions(df=carbon_change_tC_5, groupby_dimensions=['Country', 'Years', 'land-use', 'land-age'], aggregation_method='Sum')
    # c-change-origin = mineral-soils
    carbon_change_tC_5['c-change-origin'] = "mineral-soils"
    carbon_change_tC_4 = pd.concat([carbon_change_tC_4, carbon_change_tC_5.set_index(carbon_change_tC_5.index.astype(str) + '_dup')])
    carbon_change_tC_3 = pd.concat([carbon_change_tC_3, carbon_change_tC_4.set_index(carbon_change_tC_4.index.astype(str) + '_dup')])
    carbon_change_tC_2 = pd.concat([carbon_change_tC_2, carbon_change_tC_3.set_index(carbon_change_tC_3.index.astype(str) + '_dup')])
    carbon_change_tC = pd.concat([carbon_change_tC, carbon_change_tC_2.set_index(carbon_change_tC_2.index.astype(str) + '_dup')])

    # Emissions linked to carbon dynamics
    # 
    # Objective: the node is computing the emissions linked to carbon dynamics given the change in land usage.
    # Note : one part of the emissions are linked to land management changes (ex. crop land coverted in forest, ...). But we also have the effect of existing forest which capture CO2 (= Forest reference level)
    # As changes are only computed for years after baseyear, we integrate historical land use emissions (as well as existing forest : historical and future) as an external and additional metric !
    # 
    # 1. Convert C to CO2, aggregate and adapt units
    # 
    # Main inputs: 
    # - Carbon stock variation [tC]
    # - Conversion factor from C to CO2
    # 
    # Main outputs: 
    # - CO2 emissions for each type of land and mechanism of emission (soil, biomass)
    # - Total CO2 emissions
    # - Total CO2 emissions for forest

    # carbon-change [tC]
    carbon_change_tC = use_variable(input_table=carbon_change_tC, selected_variable='carbon-change[tC]')
    # Group by  Country, Years, land-use (sum) 
    carbon_change_tC = group_by_dimensions(df=carbon_change_tC, groupby_dimensions=['Country', 'Years', 'land-use'], aggregation_method='Sum')
    # Convert Unit tC to MtC
    carbon_change_MtC = carbon_change_tC.drop(columns='carbon-change[tC]').assign(**{'carbon-change[MtC]': carbon_change_tC['carbon-change[tC]'] * 1e-06})
    # Convert Unit C to C02
    carbon_change_MtCO2 = carbon_change_MtC.drop(columns='carbon-change[MtC]').assign(**{'carbon-change[MtCO2]': carbon_change_MtC['carbon-change[MtC]'] * 3.7})
    # emissions[Mt] = carbon-chage[MtCO2) * -1 (losses of c-stock : emissions)
    emissions_Mt = carbon_change_MtCO2.assign(**{'emissions[Mt]': carbon_change_MtCO2['carbon-change[MtCO2]']*-1.0})
    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # Calibration :
    # On emission [Mt]

    # Group by  Country, Years, (sum)  
    emissions_Mt_2 = group_by_dimensions(df=emissions_Mt, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # Calibration emissions [Mt]
    emissions_Mt_3 = import_data(trigram='agr', variable_name='emissions', variable_type='Calibration')
    # Keep emission-type = c-stock
    emissions_Mt_3 = emissions_Mt_3.loc[emissions_Mt_3['emission-type'].isin(['c-stock'])].copy()
    # Group by  Country, Years (sum)
    emissions_Mt_3 = group_by_dimensions(df=emissions_Mt_3, groupby_dimensions=['Years', 'Country'], aggregation_method='Sum')
    # Apply Calibration on energy-demand[TWh]
    _, out_10017_2, _ = calibration(input_table=emissions_Mt_2, cal_table=emissions_Mt_3, data_to_be_cal='emissions[Mt]', data_cal='emissions[Mt]')

    # Cal_rate for emissions[Mt]

    # cal_rate for emissions[Mt]
    cal_rate_emissions_Mt = use_variable(input_table=out_10017_2, selected_variable='cal_rate_emissions[Mt]')
    # Module = CALIBRATION
    cal_rate_emissions_Mt = column_filter(df=cal_rate_emissions_Mt, pattern='^.*$')
    # emissions[Mt] (replace) = emissions[Mt] * ca-rate
    emissions_Mt = mcd(input_table_1=emissions_Mt, input_table_2=out_10017_2, operation_selection='x * y', output_name='emissions[Mt]')
    # gaes = CO2
    emissions_Mt['gaes'] = "CO2"
    # emission-type = c-stock
    emissions_Mt['emission-type'] = "c-stock"
    # emissions [Mt]
    emissions_Mt = export_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')

    # For : Climate
    # 
    # - Emissions (linked to land management)

    # emissions [Mt]
    emissions_Mt = use_variable(input_table=emissions_Mt, selected_variable='emissions[Mt]')
    # Add emissions-or-capture
    emissions_Mt['emissions-or-capture'] = "emissions"
    # Module = Climate
    emissions_Mt = column_filter(df=emissions_Mt, pattern='^.*$')

    # For : KPIs (Pathway Explorer)
    # 
    # - % of change by land-use

    # Lag  land-management[ha]
    out_9520_1, _ = lag_variable(df=land_management_ha, in_var='land-management[ha]')
    # land-change[ha] = land-management[ha] - land-management-lagged[ha]
    land_change_ha = mcd(input_table_1=land_management_ha, input_table_2=out_9520_1, operation_selection='x - y', output_name='land-change[ha]')
    # land-change[%] = land-change[ha] / land-management[ha]
    land_change_percent = mcd(input_table_1=land_management_ha, input_table_2=land_change_ha, operation_selection='y / x', output_name='land-change[%]')
    # Set 0
    land_change_percent = missing_value(df=land_change_percent, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')

    # For : Pathway Explorer
    # 
    # - Land management share [ha/ha]
    # => Detailled by type of land-use

    # Group by Country, Years, (sum)
    land_management_ha_2 = group_by_dimensions(df=land_management_ha, groupby_dimensions=['Country', 'Years'], aggregation_method='Sum')
    # land-management-share[ha/ha] = land-management[ha] (per land type) / land-management[ha] (sum over all land type)
    land_management_share_ha_per_ha = mcd(input_table_1=land_management_ha, input_table_2=land_management_ha_2, operation_selection='x / y', output_name='land-management-share[ha/ha]')
    # Set 0
    land_management_share_ha_per_ha = missing_value(df=land_management_share_ha_per_ha, DTS_DT_O=[['org.knime.core.data.def.IntCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.StringCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.DoNothingMissingCellHandlerFactory'], ['org.knime.core.data.def.DoubleCell', 'org.knime.base.node.preproc.pmml.missingval.handlers.FixedDoubleValueMissingCellHandlerFactory']], FixedValue='0.0')
    land = pd.concat([land_change_percent, land_management_share_ha_per_ha.set_index(land_management_share_ha_per_ha.index.astype(str) + '_dup')])
    land = pd.concat([land_management_ha, land.set_index(land.index.astype(str) + '_dup')])
    out_9533_1 = pd.concat([land, capex_new_forest_MEUR.set_index(capex_new_forest_MEUR.index.astype(str) + '_dup')])
    out_9187_1 = add_trigram(module_name=module_name, df=out_9533_1)
    # Module = Pathway Explorer
    out_9187_1 = column_filter(df=out_9187_1, pattern='^.*$')

    return out_9187_1, cal_rate_emissions_Mt, production, emissions_Mt, land_management_ha


