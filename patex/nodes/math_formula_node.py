# ----------------------------------------------------------------------------------------------------- #
# 2020, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    MATH FORMULA NODE
    ============================
    KNIME options NOT implemented:
        - Selection of column by wildcards
        - Use of row index and row count
        - All special mathematical function except for COL_MIN, COL_MAX, COL_MEAN, COL_MEDIAN, pi, e ,COL_SUM, COL_STDDEV, COL_VAR, ln
"""
from copy import copy

import numpy as np
import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode

SPECIFIC_EXPRESSIONS = [
    "if($vehicle-fleet-historical[number]$ >= 0,$vehicle-fleet-historical[number]$,$veh-fleet-future[number]$)", ## New = transport prototype
    "if($veh-fleet-need[number]$<0, 0, $veh-fleet-need[number]$)", ## New = transport prototype
    "if($new-veh-fleet[number]$ < 0, 0, $new-veh-fleet[number]$)",  ## New = transport prototype
    "if($new-veh-fleet[number]$ > 0, 0, $new-veh-fleet[number]$)",  ## New = transport prototype
    "if($new-capacities[kW]$ < 0, 0, $new-capacities[kW]$)", ## New = power prototype
    "if($heat-energy-production[TWh]$<0,0,$heat-energy-production[TWh]$)",  ## New = power prototype
    "if($intermitency-share[%]$<0.15,8,$transmission-capacity[GW]$)",  ## New = power prototype
    "if($energy-imported[TWh]$ < 0, 0, $energy-imported[TWh]$)",  ## New = power prototype
    "if($annual-backup-growth[GW]$ < 0, 0, $annual-backup-growth[GW]$)",  ## New = power prototype
    "if($annual-transmission-growth[GW]$ < 0, 0, $annual-transmission-growth[GW]$)",  ## New = power prototype
    "if($net-energy-production[TWh]$ * (0.2812 * $intermitency-share[%]$ - 0.0228) < 0,0,$net-energy-production[TWh]$ * (0.2812 * $intermitency-share[%]$ - 0.0228))",  ## New = power prototype
    "if($energy-imported[TWh]$ < 0, abs($energy-imported[TWh]$), 0)",  ## New = power prototype
    "if($energy-imported[TWh]$ < 0, 0, $energy-imported[TWh]$)",  ## New = power prototype
    "abs($energy-imported[GW]$)",  ## New = power prototype
    "-abs($direct-air-capture[Mt]$)",  ## New = power prototype
    "if($EE-saving-delta[TWh]$<0, 0, 15*$EE-saving-delta[TWh]$)", # New = industry prototype
    "if($disable_product_calibration$==1,0,1)", # New = industry prototype
    "min_in_args($product-import-share[%demand]$,1)", # New = industry prototype
    "min_in_args($subproduct-import-share[%demand]$,1)", # New = industry prototype
    "min_in_args($material-import-share[%demand]$,1)", # New = industry prototype
    "if($renovation-rate-overtimestep[%]$>1,1,$renovation-rate-overtimestep[%]$)", # New = buildings prototype
    "if($pipes-length-overtimestep[km]$<0, 0, $pipes-length-overtimestep[km]$)", # New = buildings prototype
    "if($heating-capacity[KW]$<0,0,$heating-capacity[KW]$)", # New = buildings prototype
    "if($land-variation[ha]$> 0, $land-variation[ha]$ * 5000 / 1000000, 0)",  # New = agriculture prototype
    "if($land-area[ha]$> 0, $land-area[ha]$ * 5000 / 1000000, 0)",
    "if($buffer-land[ha]$>0,0,abs($buffer-land[ha]$))", # New = agriculture prototype
    "if($buffer-land[ha]$<0,0,$buffer-land[ha]$)", # New = agriculture prototype
    "if($buffer[TWh]$>0,0,abs($buffer[TWh]$))", # New = agriculture prototype
    "if($buffer[TWh]$<0,0,$buffer[TWh]$)", # New = agriculture prototype
    "if($domestic-production[kcal]$<0,0,$domestic-production[kcal]$)",  # New = agriculture prototype
    "if($domestic-crop-production[kcal]$<0,0,$domestic-crop-production[kcal]$)",  # New = agriculture prototype
    "$gwp-100[-]$", # new = climate
    "if($value$==1,0,1)",  # new = industry
    "-abs($CC[Mt]$)",  # new = industry
    "-abs($emissions[Mt]$)", # new = power
    "if($emissions[MtCO2e]$>0, 0, $emissions[MtCO2e]$)", # new = climate
    "if($missing-floor-area[m2]$>=0,0,-$missing-floor-area[m2]$)", # new = buildings
    "if($missing-floor-area[m2]$<0,0,$missing-floor-area[m2]$)", # new = buildings
    "max_in_args(0,($dhg_energy-demand[TWh/year]$-$dhg_energy-demand_heat-contribution[TWh/year]$)/$dhg_energy-demand[TWh/year]$)",
    "max_in_args(0,$floor-area-increase[Mm2]$+$demolished-area[Mm2]$)",
    "max_in_args(0,$floor-area-old-stock[Mm2]$-$floor-area-old-stock-renovated[Mm2]$)",
    "$floor-area-previous[Mm2]$*((1+$demolition-rate_exi$)**($period-duration$)-1)+-min_in_args(0,$floor-area-increase[Mm2]$)",
    "min_in_args(1,$floor-area[Mm2]$/($constructed-area-acc[Mm2]$+$renovated-area-acc[Mm2]$))",
    "if($Years_number$<= 2015,$floor-area-demand[Mm2]$* $demolition-rate_exi$ ,0 )",
    "max_in_args($undemolished_area[m2]$*$renovation-rate_exi$,$undemolished_area_2015[m2]$*$renovation-rate_exi$)",
    "max_in_args($constructed_area[Mm2]$,$Limit_constructed area$)",
    "max_in_args(($floor-area-demand[Mm2]$-$previous-floor-area-demand[Mm2]$)-($floor-area-demand[Mm2]$*$demolition-rate_exi$), $floor-area-demand[Mm2]$*$demolition-rate_exi$  )",
    "if($Years_number$>=2015,$undemolished_area[m2]$,$floor-area-demand[Mm2]$)",
    "abs(0-$sim_poultry[%]$)",
    "if($Years_number$>2015, $undemolished-area-2015[m2]$*((1-$demolition-rate_exi$)**($Years_number$-2015)),$floor-area-demand[Mm2]$-$demolished-area-before2015[m2]$)",
    "max_in_args($yearly-constructed-area[Mm2]$,$Limit_constructed_area$)",
    "if($Years_number$>=2015,$undemolished-area[m2]$,$floor-area-demand[Mm2]$)",
    "max_in_args($undemolished-area[m2]$*$renovation-rate_exi$,$undemolished-area-2015[m2]$*$renovation-rate_exi$)",
    "if($Years$<=2015,$Years$-1,$Years$-5)",
    "if($Years$<2025,$constructed-area_2025[Mm2]$,$constructed-area[Mm2]$)",
    "if($Years$<2025,$renovated-area_2025[Mm2]$,$renovated-area[Mm2]$)",
    "if($Years_number$<= 2015,0,0 )",
    "max_in_args($yearly-constructed-area[Mm2]$,0)",
    "max_in_args($constructed-area-over-timestep[Mm2]$,0)",
    "if($Years_number$==2020,$constructed-area-over-timestep[Mm2]$/2,$constructed-area-over-timestep[Mm2]$/$period-duration$)",
    "if($Years$==2015,$Years$,$Years$-5)",
    "if($Years$<=2020,$constructed-area_2020[Mm2]$,$constructed-area[Mm2]$)",
    "if($Years$<=2020,$renovated-area_2020[Mm2]$,$renovated-area[Mm2]$)",
    "max_in_args($ccu_ccus_hydrogen-demand[TWh]$-$sto_hydrogen[TWh]$,0)",
    "max_in_args(0,$floor-area[Mm2]$-$constructed-area-acc[Mm2]$-$renovated-area-acc[Mm2]$)*$existing-mix$",
    "if($ref_link-refineries-to-activity[-]$==1,$net-energy-production_fossil_oil[TWh]$ ,$ref_fuel-production[TWh]$ )",
    "min_in_args($lus_land_initial-area_unfccc_cropland[ha]$,$lus_land_cropland[ha]$)",
    "min_in_args($lus_land_initial-area_unfccc_grassland[ha]$,$lus_land_grassland[ha]$)",
    "min_in_args($lus_land_matrix_demand_cropland[ha]$,$lus_land_matrix_supply_cropland[ha]$)",
    "min_in_args($lus_land_matrix_demand_grassland[ha]$,$lus_land_matrix_supply_grassland[ha]$)",
    "if($Years$<=2020,$renovated-area_2020[Mm2]$,$renovated-area[Mm2]$)",
    "$elc_demand-imported[TWh]$+if($elc_demand-after-RES[TWh]$>0,0,$elc_demand-after-RES[TWh]$)",
    "if($elc_demand-after-RES[TWh]$<0,0,$elc_demand-after-RES[TWh]$)",
    "-if($lus_energy-balance_bioenergy_solid[TWh]$<0,$lus_energy-balance_bioenergy_solid[TWh]$,0)",
    "if($lus_energy-balance_bioenergy_solid[TWh]$<0,0,$lus_energy-balance_bioenergy_solid[TWh]$)",
    "$bioenergy-import_wood_solid[TWh]$-if($bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$<0,$bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$,0)",
    "if($bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$<0,0,$bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$)",
    "-if($bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$<0,$bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$,0)",
    "if($bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$<0,0,$bioenergy-balance_wood_solid[TWh]$-($bioenergy-demand_elc_gas[TWh]$+$bioenergy-demand_agr_gas[TWh]$+$bioenergy-demand_hea_gas[TWh]$+$bioenergy-demand_ref_gas[TWh]$)/0.8)",
    "max_in_args($energy-demand[TWh]$*(1-($bld_fuel-switch_biogas[%]$+$bld_fuel-switch_e-gas[%]$+$bld_fuel-switch_hydrogen[%]$)),0 )",
    "min_in_args($bld_fuel-switch_e-gas[%]$,(1-($bld_fuel-switch_hydrogen[%]$+$bld_fuel-switch_biogas[%]$)))",
    "max_in_args($bld_fuel-switch_e-gas[%]$,0)",
    "if($constructed-area-over-timestep[Mm2]$ > 0 , $constructed-area-over-timestep[Mm2]$, $demolished-area-over-timestep[Mm2]$)",
    "min_in_args($bld_fuel-switch_e-liquids[%]$,(1-$bld_fuel-switch_bioliquids[%]$))",
    "max_in_args($bld_fuel-switch_e-liquids[%]$,0)",
    "max_in_args($energy-demand[TWh]$*(1-($bld_fuel-switch_bioliquids[%]$+$bld_fuel-switch_e-liquids[%]$)),0 )",
    "abs($elc_demand-imported[TWh]$)",
    "if($elc_demand-imported[TWh]$<0,0,$elc_demand-imported[TWh]$)",
    "if($elc_total-prod[TWh]$*(0.2812*$elc_intermitency-share[%]$-0.0228)<0,0,$elc_total-prod[TWh]$*(0.2812*$elc_intermitency-share[%]$-0.0228))",
    "if($elc_intermitency-share[%]$<0.15,8,$transmission-capacity[GW]$)",
    "$${Ddegree_integration[%]}$$*(+263.69*$elc_intermitency-share[%]$*$elc_intermitency-share[%]$-13.252*$elc_intermitency-share[%]$+4.3033)",
    "$transmission-capacity-annual-increase[GW.km]$*$${Dtransmission_cost_MEUR_per_GWkm}$$",
    "$transmission-capacity[GW.km]$*0.01*$${Dtransmission_cost_MEUR_per_GWkm}$$",
    "$backup-production[GWh]$/$${Dloadfactor_backupplant[%]}$$",
    "$backup-capacity-annual-increase[GW]$*$${Dbackup-capacity-capex[MEUR/GW]}$$",
    "$backup-capacity[GW]$*$${Dbackup-capacity-opexfixed[MEUR/GW]}$$",
    "$backup-consumption[GWh]$*$${Dbackup-capacity-opexvariable[EUR/MWh]}$$/1000",
    "$backup-production[GWh]$*$${Dloadfactor_backupplant[%]}$$",
    "if($elc_link-costs-to-activity[-]$==1,$fuel-price_elc[MEUR/TWh]$,$tec_exogenous-energy-costs_electricity[EUR/MWh]$)",
    "if($elc_link-costs-to-activity[-]$==1,$fuel-price_hyd[MEUR/TWh]$,$tec_exogenous-energy-costs_hydrogen[EUR/MWh]$)",
    "if($elc_link-costs-to-activity[-]$==1,$fuel-price_dhg[MEUR/TWh]$,$tec_exogenous-energy-costs_heat-waste[EUR/MWh]$)",
    "if($elc_link-costs-to-activity_electricity[-]$==1,$fuel-price_elc[MEUR/TWh]$,$tec_exogenous-energy-costs_electricity[EUR/MWh]$)",
    "if($elc_link-costs-to-activity_hydrogen[-]$==1,$fuel-price_hyd[MEUR/TWh]$,$tec_exogenous-energy-costs_hydrogen[EUR/MWh]$)",
    "if($elc_link-costs-to-activity_heat[-]$==1,$fuel-price_dhg[MEUR/TWh]$,$tec_exogenous-energy-costs_heat-waste[EUR/MWh]$)",
    "if($constructed-area-over-timestep_historical[Mm2]$ > 0 , $constructed-area-over-timestep_historical[Mm2]$, $demolished-area-over-timestep_historical[Mm2]$)",
    "if($Years_number$<2015,1,$renovation-mix$)",
    "max_in_args(min_in_args($bld_fuel-switch_e-gas[%]$,(1-($bld_fuel-switch_hydrogen[%]$+$bld_fuel-switch_biogas[%]$))),0)",
    "max_in_args(min_in_args($bld_fuel-switch_hydrogen[%]$,(1-($bld_fuel-switch_biogas[%]$))),0)",
    "if($lus_dyn_forest[ha]$ > 0, $lus_dyn_forest[ha]$ * 5000 / 1000000, 0)",
    "if($dhg_pipes-total-over-timestep[km]$ < 0, 0 , $dhg_pipes-total-over-timestep[km]$)",
    "if($existing-unrenovated-area-over-timestep[Mm2]$<0,0 ,$existing-unrenovated-area-over-timestep[Mm2]$ )",
    "if($chp-pth-ratio[-]$==0,0,$elc_capacity_CHP[GW]$*$capacity-factor_CHP[%]$*8.76/$chp-pth-ratio[-]$)",
    "if($elc_link-costs-to-activity_heat[-]$ == 1, $fuel-price_heat[MEUR / TWh]$, $tec_exogenous-energy-costs_heat-waste[EUR / MWh]$)",
    "if($elc_link-costs-to-activity_heat[-]$==1,$fuel-price_heat[MEUR/TWh]$,$tec_exogenous-energy-costs_heat-waste[EUR/MWh]$)",
    "if($lus_change_forest[ha]$ > 0, $lus_change_forest[ha]$ * 5000 / 1000000, 0)",
    "if($${Ioverride_electricity_calibration}$$==0, 1, $cal_rate_energy-demand[TWh]$)", # BLD / IND > calibration elec
    "$water-losses[%]$+1", # Water
    "round($buffer-land[ha]$, 3)"  # LUS
]


FUN_DICT = {
    "COL_MIN": np.min,
    "COL_MAX": np.max,
    "COL_MEAN": np.mean,
    "COL_MEDIAN": np.median,
    "pi": np.pi,
    "COL_SUM": np.sum,
    "COL_STDDEV": np.std,
    "COL_VAR": np.var,
    "ln": np.log,
}


def math_formula(
    df: pd.DataFrame,
    convert_to_int: bool,
    replaced_column: str,
    splitted: str,
    list_flow_vars: list[str] = [],
    **kwargs,
) -> pd.DataFrame:
    col_rename_dict = {}
    exp_rename_dict = {}

    if splitted not in SPECIFIC_EXPRESSIONS:
        splitted = copy(splitted)

        for var, value in kwargs.items():
            var_name = '@' + var
            splitted = [x if x != var_name else str(value) for x in splitted]
            
        col_id = 0
        for op in splitted:
            if op in df.columns:
                col_name = 'col' + str(col_id)
                col_rename_dict[op] = col_name
                exp_rename_dict[col_name] = op
                col_id += 1
        splitted = [col_rename_dict.get(item, item) for item in splitted]  # rename the column name of the splitted expression with the new col names

        expression = ''.join(splitted)
        # renaming the columns of the dataframe to avoid errors in the eval() function (see below)
        # cols = [col for col in col_rename_dict]
        # pd.to_numeric(df[cols])
        df = df.rename(columns=col_rename_dict)


        # ------------------------------------------------------
        # Evaluation of the expression using pandas.DataFrame.eval() function
        # ------------------------------------------------------
        # NOTE: pandas.DataFrame.eval() doesn't allow to access numeric column names or columns names with brackets
        # (e.g. '2011', 'col_var_[unit]'). This is the reason why we are renaming all the columns before the evaluation


        df.eval(expression, inplace=True, local_dict={**FUN_DICT, **kwargs})


        df = df.rename(columns={**exp_rename_dict, **{'NEW_COLUMN' : replaced_column}})

        if convert_to_int:
            df[replaced_column] = df[replaced_column].round(0)
    else:
        if splitted == "if($vehicle-fleet-historical[number]$ >= 0,$vehicle-fleet-historical[number]$,$veh-fleet-future[number]$)": # New : transport prototype
            df[replaced_column] = df["vehicle-fleet-historical[number]"]
            mask = (df["vehicle-fleet-historical[number]"] < 0)
            df.loc[mask, replaced_column] = df.loc[mask, "veh-fleet-future[number]"]
        elif splitted == "if($new-veh-fleet[number]$ < 0, 0, $new-veh-fleet[number]$)": # New : transport prototype
            df[replaced_column] = df["new-veh-fleet[number]"]
            mask = (df["new-veh-fleet[number]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($veh-fleet-need[number]$<0, 0, $veh-fleet-need[number]$)": # New : transport prototype
            df[replaced_column] = df["veh-fleet-need[number]"]
            mask = (df["veh-fleet-need[number]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($new-veh-fleet[number]$ > 0, 0, $new-veh-fleet[number]$)": # New : transport prototype
            df[replaced_column] = df["new-veh-fleet[number]"]
            mask = (df["new-veh-fleet[number]"] > 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($new-capacities[kW]$ < 0, 0, $new-capacities[kW]$)":  # New : power prototype
            df[replaced_column] = df["new-capacities[kW]"]
            mask = (df["new-capacities[kW]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($heat-energy-production[TWh]$<0,0,$heat-energy-production[TWh]$)":  # New : power prototype
            df[replaced_column] = df["heat-energy-production[TWh]"]
            mask = (df["heat-energy-production[TWh]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($intermitency-share[%]$<0.15,8,$transmission-capacity[GW]$)":  # New : power prototype
            df[replaced_column] = df["transmission-capacity[GW]"]
            mask = (df["intermitency-share[%]"] < 0.15)
            df.loc[mask, replaced_column] = 8
        elif splitted == "if($energy-imported[TWh]$ < 0, 0, $energy-imported[TWh]$)":  # New : power prototype
            df[replaced_column] = df["energy-imported[TWh]"]
            mask = (df["energy-imported[TWh]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($annual-backup-growth[GW]$ < 0, 0, $annual-backup-growth[GW]$)":  # New : power prototype
            df[replaced_column] = df["annual-backup-growth[GW]"]
            mask = (df["annual-backup-growth[GW]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($annual-transmission-growth[GW]$ < 0, 0, $annual-transmission-growth[GW]$)":  # New : power prototype
            df[replaced_column] = df["annual-transmission-growth[GW]"]
            mask = (df["annual-transmission-growth[GW]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($net-energy-production[TWh]$ * (0.2812 * $intermitency-share[%]$ - 0.0228) < 0,0,$net-energy-production[TWh]$ * (0.2812 * $intermitency-share[%]$ - 0.0228))":  # New : power prototype
            df[replaced_column] = df["net-energy-production[TWh]"] * (0.2812 * df["intermitency-share[%]"] - 0.0228)
            mask = (df["net-energy-production[TWh]"] * (0.2812 * df["intermitency-share[%]"] - 0.0228) < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($energy-imported[TWh]$ < 0, abs($energy-imported[TWh]$), 0)":  # New : power prototype
            df[replaced_column] = df["energy-imported[TWh]"].abs()
            mask = (df["energy-imported[TWh]"] >= 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "energy-imported[TWh]$=if($energy-imported[TWh]$ < 0, 0, $energy-imported[TWh]$)":  # New : power prototype
            df[replaced_column] = df["energy-imported[TWh]"]
            mask = (df["energy-imported[TWh]"] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "abs($energy-imported[GW]$)":  # New : power prototype
            df[replaced_column] = df["energy-imported[GW]"].abs()
        elif splitted == "-abs($direct-air-capture[Mt]$)":  # New : power prototype
            df[replaced_column] = -df["direct-air-capture[Mt]"].abs()
        elif splitted == "if($EE-saving-delta[TWh]$<0, 0, 15*$EE-saving-delta[TWh]$)": # New : industry prototype
            mask = (df["EE-saving-delta[TWh]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = 15 * df.loc[~mask, "EE-saving-delta[TWh]"]
        elif splitted == "if($disable_product_calibration$==1,0,1)": # New : industry prototype
            mask = (df["disable_product_calibration"] == 1)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = 1
        elif splitted == "min_in_args($product-import-share[%demand]$,1)":  # New : industry prototype
            mask = (df["product-import-share[%demand]"] > 1)
            df.loc[mask, replaced_column] = 1
        elif splitted == "min_in_args($subproduct-import-share[%demand]$,1)":  # New : industry prototype
            mask = (df["subproduct-import-share[%demand]"] > 1)
            df.loc[mask, replaced_column] = 1
        elif splitted == "min_in_args($material-import-share[%demand]$,1)":  # New : industry prototype
            mask = (df["material-import-share[%demand]"] > 1)
            df.loc[mask, replaced_column] = 1
        elif splitted == "if($renovation-rate-overtimestep[%]$>1,1,$renovation-rate-overtimestep[%]$)":  # New : building prototype
            mask = (df["renovation-rate-overtimestep[%]"] > 1)
            df.loc[mask, replaced_column] = 1
            df.loc[~mask, replaced_column] = df.loc[~mask, "renovation-rate-overtimestep[%]"]
        elif splitted == "if($pipes-length-overtimestep[km]$<0, 0, $pipes-length-overtimestep[km]$)":  # New : building prototype
            mask = (df["pipes-length-overtimestep[km]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "pipes-length-overtimestep[km]"]
        elif splitted == "if($missing-floor-area[m2]$ < 0, 0, $missing-floor-area[m2]$)":  # New : building prototype
            mask = (df["missing-floor-area[m2]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "missing-floor-area[m2]"]
        elif splitted == "if($missing-floor-area[m2]$>=0,0,-$missing-floor-area[m2]$)":  # New : building prototype
            mask = (df["missing-floor-area[m2]"] >= 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "missing-floor-area[m2]"]*-1
        elif splitted == "if($heating-capacity[KW]$<0,0,$heating-capacity[KW]$)":  # New : building prototype
            mask = (df["heating-capacity[KW]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "heating-capacity[KW]"]
        elif splitted == "if($land-variation[ha]$> 0, $land-variation[ha]$ * 5000 / 1000000, 0)":  # New : agriculture prototype
            mask = (df["land-variation[ha]"] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, "land-variation[ha]"] * 5000 / 1000000
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($land-area[ha]$> 0, $land-area[ha]$ * 5000 / 1000000, 0)":
            mask = (df["land-area[ha]"] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, "land-area[ha]"] * 5000 / 1000000
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($buffer-land[ha]$>0,0,abs($buffer-land[ha]$))":  # New : agriculture prototype
            mask = (df["buffer-land[ha]"] > 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "buffer-land[ha]"].abs()
        elif splitted == "if($buffer-land[ha]$<0,0,$buffer-land[ha]$)":  # New : agriculture prototype
            mask = (df["buffer-land[ha]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "buffer-land[ha]"]
        elif splitted == "if($buffer[TWh]$>0,0,abs($buffer[TWh]$))":  # New : agriculture prototype
            mask = (df["buffer[TWh]"] > 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "buffer[TWh]"].abs()
        elif splitted == "if($buffer[TWh]$<0,0,$buffer[TWh]$)":  # New : agriculture prototype
            mask = (df["buffer[TWh]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "buffer[TWh]"]
        elif splitted == "if($${Ioverride_electricity_calibration}$$==0, 1, $cal_rate_energy-demand[TWh]$)": # New : BLD / TRA electricity calibration
            if kwargs["override_electricity_calibration"] == 0:
                df["cal_rate_energy-demand[TWh]"] = 1.0
        elif splitted == "if($domestic-production[kcal]$<0,0,$domestic-production[kcal]$)":  # New : agriculture prototype
            mask = (df["domestic-production[kcal]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "domestic-production[kcal]"]
        elif splitted == "if($domestic-crop-production[kcal]$<0,0,$domestic-crop-production[kcal]$)":  # New : agriculture prototype
            mask = (df["domestic-crop-production[kcal]"] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "domestic-crop-production[kcal]"]
        elif splitted == "$gwp-100[-]$":  # New climate
            df[replaced_column] = df[replaced_column].astype(float)
        elif splitted == "if($value$==1,0,1)":  # New industry
            mask = (df["value"] == 1)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = 1
        elif splitted == "-abs($CC[Mt]$)":  # New industry
            df[replaced_column] = -np.abs(df[replaced_column])
        elif splitted == "-abs($emissions[Mt]$)":  # New Power
            df[replaced_column] = -np.abs(df[replaced_column])
        elif splitted == "if($emissions[MtCO2e]$>0, 0, $emissions[MtCO2e]$)":  # New climate
            mask = (df["emissions[MtCO2e]"] > 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "$water-losses[%]$+1":  # Water
            df.loc[:, replaced_column] = df.loc[:, "water-losses[%]"] + 1
        elif splitted == "round($buffer-land[ha]$, 3)":  # LUS
            df.loc[:, replaced_column] = df.loc[:, "buffer-land[ha]"].round(3)


        # ------------------------------------------- OLD MODULES --------------------------------------------------------------------------------- #
        elif splitted == "max_in_args(0,($dhg_energy-demand[TWh/year]$-$dhg_energy-demand_heat-contribution[TWh/year]$)/$dhg_energy-demand[TWh/year]$)":
            df[replaced_column] = (df["dhg_energy-demand[TWh/year]"]-df["dhg_energy-demand_heat-contribution[TWh/year]"])/df["dhg_energy-demand[TWh/year]"]
            df[replaced_column] = df[replaced_column].fillna(0)
            mask = (df[replaced_column]<0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "max_in_args(0,$floor-area-increase[Mm2]$+$demolished-area[Mm2]$)":
            df[replaced_column] = df["floor-area-increase[Mm2]"]+df["demolished-area[Mm2]"]
            mask = (df[replaced_column]<0)
            df.loc[mask, replaced_column] = 0
        # elif splitted == "pow($floor-area[Mm2]$/$floor-area_previousperiod[Mm2]$,1/$period-duration$)":
        #     df[replaced_column] = (df["floor-area[Mm2]"]/df["floor-area_previousperiod[Mm2]"])**(1/df["period-duration"])
        # elif splitted == "$floor-area-previous[Mm2]$*(pow(1+$demolition-rate_exi$,$period-duration$)-1)":
        #     df[replaced_column] = df["floor-area-previous[Mm2]"]*((1+df["demolition-rate_exi"]**df["period-duration"])-1)
        # elif splitted == "$floor-area[Mm2]$/pow($growth-factor$,$period-duration$)":
        #     df[replaced_column] = df["floor-area[Mm2]"]/(df["growth-factor"]**df["period-duration"])
        elif splitted == "max_in_args(0,$floor-area-old-stock[Mm2]$-$floor-area-old-stock-renovated[Mm2]$)":
            df[replaced_column] = df["floor-area-old-stock[Mm2]"] - df["floor-area-old-stock-renovated[Mm2]"]
            mask = (df[replaced_column]<0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "$floor-area-previous[Mm2]$*((1+$demolition-rate_exi$)**($period-duration$)-1)+-min_in_args(0,$floor-area-increase[Mm2]$)":
            df_min_in_args = df["floor-area-increase[Mm2]"].copy()
            df_min_in_args[df_min_in_args > 0] = 0
            df[replaced_column] = df["floor-area-previous[Mm2]"]*((1+df["demolition-rate_exi"])**df["period-duration"]-1)-df_min_in_args
        elif splitted == "min_in_args(1,$floor-area[Mm2]$/($constructed-area-acc[Mm2]$+$renovated-area-acc[Mm2]$))":
            df[replaced_column] = df["floor-area[Mm2]"]/(df["constructed-area-acc[Mm2]"]+df["renovated-area-acc[Mm2]"])
            mask = (df[replaced_column] > 1)
            df.loc[mask, replaced_column] = 1
            df[replaced_column] = df[replaced_column].replace(np.inf,1)
            df[replaced_column] = df[replaced_column].fillna(1)
        elif splitted == "if($Years_number$<= 2015,$floor-area-demand[Mm2]$* $demolition-rate_exi$ ,0 )":
            df[replaced_column] = df["floor-area-demand[Mm2]"]*df["demolition-rate_exi"]
            mask = (df['Years_number'] > 2015)
            df.loc[mask, replaced_column] = 0
        elif splitted == "max_in_args($undemolished_area[m2]$*$renovation-rate_exi$,$undemolished_area_2015[m2]$*$renovation-rate_exi$)":
            temp_1 = df["undemolished_area[m2]"]*df['renovation-rate_exi']
            temp_2 = df["undemolished_area_2015[m2]"]*df["renovation-rate_exi"]
            df_temp = pd.DataFrame({"temp_1": temp_1, "temp_2": temp_2})
            df[replaced_column] = df_temp.max(axis=1)
        elif splitted == "if($Years_number$>2015, $undemolished-area-2015[m2]$*((1-$demolition-rate_exi$)**($Years_number$-2015)),$floor-area-demand[Mm2]$-$demolished-area-before2015[m2]$)":
            df[replaced_column] = df["floor-area-demand[Mm2]"]-df["demolished-area-before2015[m2]"]
            mask = (df['Years_number'] > 2015)
            df.loc[mask, replaced_column] = df.loc[mask,"undemolished-area-2015[m2]"] * ((1-df.loc[mask,"demolition-rate_exi"])**(df.loc[mask,"Years_number"]-2015))
        elif splitted == "max_in_args($constructed_area[Mm2]$,$Limit_constructed area$)":
            df[replaced_column] = df[["constructed_area[Mm2]","Limit_constructed area"]].max(axis=1)
        elif splitted == "max_in_args(($floor-area-demand[Mm2]$-$previous-floor-area-demand[Mm2]$)-($floor-area-demand[Mm2]$*$demolition-rate_exi$), $floor-area-demand[Mm2]$*$demolition-rate_exi$  )":
            temp_1 = (df["floor-area-demand[Mm2]"]-df['previous-floor-area-demand[Mm2]'])-(df["floor-area-demand[Mm2]"]*df["demolition-rate_exi"])
            temp_2 = df["floor-area-demand[Mm2]"]*df["demolition-rate_exi"]
            df_temp = pd.DataFrame({"temp_1": temp_1, "temp_2": temp_2})
            df[replaced_column] = df_temp.max(axis=1)
        elif splitted == "if($Years_number$>=2015,$undemolished_area[m2]$,$floor-area-demand[Mm2]$)":
            df[replaced_column] = df["undemolished_area[m2]"]
            mask = (df['Years_number'] < 2015)
            df.loc[mask, replaced_column] = df.loc[mask,"floor-area-demand[Mm2]"]
        elif splitted == "if($Years_number$>=2015,$undemolished-area[m2]$,$floor-area-demand[Mm2]$)":
            df[replaced_column] = df["undemolished-area[m2]"]
            mask = (df['Years_number'] < 2015)
            df.loc[mask, replaced_column] = df.loc[mask,"floor-area-demand[Mm2]"]
        elif splitted == "abs(0-$sim_poultry[%]$)":
            df[replaced_column] = 0 - df["sim_poultry[%]"]
            df[replaced_column] = df[replaced_column].abs()
        elif splitted == "max_in_args($yearly-constructed-area[Mm2]$,$Limit_constructed_area$)":
            df[replaced_column] = df[["yearly-constructed-area[Mm2]","Limit_constructed_area"]].max(axis=1)
        elif splitted == "max_in_args($undemolished-area[m2]$*$renovation-rate_exi$,$undemolished-area-2015[m2]$*$renovation-rate_exi$)":
            temp_1 = df["undemolished-area[m2]"]*df['renovation-rate_exi']
            temp_2 = df["undemolished-area-2015[m2]"]*df["renovation-rate_exi"]
            df_temp = pd.DataFrame({"temp_1": temp_1, "temp_2": temp_2})
            df[replaced_column] = df_temp.max(axis=1)
        elif splitted == "if($Years$<=2015,$Years$-1,$Years$-5)":
            mask = (df['Years'] > 2015)
            df.loc[~mask, replaced_column] = df.loc[~mask,"Years"]-1
            df.loc[mask, replaced_column] = df.loc[mask,"Years"]-5
        elif splitted == "if($Years$<2025,$constructed-area_2025[Mm2]$,$constructed-area[Mm2]$)":
            mask = (df['Years'] < 2025)
            df.loc[~mask, replaced_column] = df.loc[~mask,"constructed-area[Mm2]"]
            df.loc[mask, replaced_column] = df.loc[mask,"constructed-area_2025[Mm2]"]
        elif splitted == "if($Years$<2025,$renovated-area_2025[Mm2]$,$renovated-area[Mm2]$)":
            mask = (df['Years'] < 2025)
            df.loc[~mask, replaced_column] = df.loc[~mask,"renovated-area[Mm2]"]
            df.loc[mask, replaced_column] = df.loc[mask,"renovated-area_2025[Mm2]"]
        elif splitted == "if($Years_number$<= 2015,0,0 )":
            mask = (df['Years_number'] > 2015)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = 0
        elif splitted == "max_in_args($yearly-constructed-area[Mm2]$,0)":
            mask = (df['yearly-constructed-area[Mm2]'] >= 0)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = df.loc[mask,"yearly-constructed-area[Mm2]"]
        elif splitted ==  "max_in_args($constructed-area-over-timestep[Mm2]$,0)":
            mask = (df['constructed-area-over-timestep[Mm2]'] >= 0)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = df.loc[mask, "constructed-area-over-timestep[Mm2]"]
        elif splitted =="if($Years_number$==2020,$constructed-area-over-timestep[Mm2]$/2,$constructed-area-over-timestep[Mm2]$/$period-duration$)":
            mask = (df['Years_number'] == 2020)
            df.loc[~mask, replaced_column] = df.loc[~mask,"constructed-area-over-timestep[Mm2]"]/df.loc[~mask,"period-duration"]
            df.loc[mask, replaced_column] = df.loc[mask,"constructed-area-over-timestep[Mm2]"]/2
        elif splitted =="if($Years$==2015,$Years$,$Years$-5)":
            mask = (df['Years'] == 2015)
            df.loc[~mask, replaced_column] = df.loc[~mask,"Years"]-5
            df.loc[mask, replaced_column] = df.loc[mask,"Years"]
        elif splitted =="if($Years$<=2020,$constructed-area_2020[Mm2]$,$constructed-area[Mm2]$)":
            mask = (df['Years'] <= 2020)
            df.loc[~mask, replaced_column] = df.loc[~mask,"constructed-area[Mm2]"]
            df.loc[mask, replaced_column] = df.loc[mask,"constructed-area_2020[Mm2]"]
        elif splitted =="if($Years$<=2020,$renovated-area_2020[Mm2]$,$renovated-area[Mm2]$)":
            mask = (df['Years'] <= 2020)
            df.loc[~mask, replaced_column] = df.loc[~mask,"renovated-area[Mm2]"]
            df.loc[mask, replaced_column] = df.loc[mask,"renovated-area_2020[Mm2]"]
        elif splitted == "max_in_args($ccu_ccus_hydrogen-demand[TWh]$-$sto_hydrogen[TWh]$,0)":
            mask = (df['ccu_ccus_hydrogen-demand[TWh]']-df['sto_hydrogen[TWh]'] >= 0)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = df.loc[mask, 'ccu_ccus_hydrogen-demand[TWh]']-df.loc[mask, 'sto_hydrogen[TWh]']
        elif splitted ==  "max_in_args(0,$floor-area[Mm2]$-$constructed-area-acc[Mm2]$-$renovated-area-acc[Mm2]$)*$existing-mix$":
            mask = (df['floor-area[Mm2]']-df['constructed-area-acc[Mm2]']-df["renovated-area-acc[Mm2]"] >= 0)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = (df.loc[mask,'floor-area[Mm2]']-df.loc[mask,'constructed-area-acc[Mm2]']-df.loc[mask,"renovated-area-acc[Mm2]"]) * df.loc[mask,"existing-mix"]
        elif splitted =="if($ref_link-refineries-to-activity[-]$==1,$net-energy-production_fossil_oil[TWh]$ ,$ref_fuel-production[TWh]$ )":
            mask = (df['ref_link-refineries-to-activity[-]'] == 1)
            df.loc[~mask, replaced_column] = df.loc[~mask,"ref_fuel-production[TWh]"]
            df.loc[mask, replaced_column] = df.loc[mask,"net-energy-production_fossil_oil[TWh]"]
        elif splitted == "min_in_args($lus_land_initial-area_unfccc_cropland[ha]$,$lus_land_cropland[ha]$)":
            mask = (df['lus_land_initial-area_unfccc_cropland[ha]'] - df["lus_land_cropland[ha]"] >= 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_land_cropland[ha]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, 'lus_land_initial-area_unfccc_cropland[ha]']
        elif splitted == "min_in_args($lus_land_initial-area_unfccc_grassland[ha]$,$lus_land_grassland[ha]$)":
            mask = (df['lus_land_initial-area_unfccc_grassland[ha]'] - df["lus_land_grassland[ha]"] >= 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_land_grassland[ha]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, 'lus_land_initial-area_unfccc_grassland[ha]']
        elif splitted == "min_in_args($lus_land_matrix_demand_cropland[ha]$,$lus_land_matrix_supply_cropland[ha]$)":
            mask = (df['lus_land_matrix_demand_cropland[ha]'] - df["lus_land_matrix_supply_cropland[ha]"] >= 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_land_matrix_supply_cropland[ha]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, 'lus_land_matrix_demand_cropland[ha]']
        elif splitted == "min_in_args($lus_land_matrix_demand_grassland[ha]$,$lus_land_matrix_supply_grassland[ha]$)":
            mask = (df['lus_land_matrix_demand_grassland[ha]'] - df["lus_land_matrix_supply_grassland[ha]"] >= 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_land_matrix_supply_grassland[ha]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, 'lus_land_matrix_demand_grassland[ha]']
        elif splitted == "$elc_demand-imported[TWh]$+if($elc_demand-after-RES[TWh]$>0,0,$elc_demand-after-RES[TWh]$)":
            mask = (df['elc_demand-after-RES[TWh]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, "elc_demand-imported[TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "elc_demand-imported[TWh]"]+df.loc[~mask, "elc_demand-after-RES[TWh]"]
        elif splitted == "if($elc_demand-after-RES[TWh]$<0,0,$elc_demand-after-RES[TWh]$)":
            mask = (df['elc_demand-after-RES[TWh]'] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "elc_demand-after-RES[TWh]"]
        elif splitted == "-if($lus_energy-balance_bioenergy_solid[TWh]$<0,$lus_energy-balance_bioenergy_solid[TWh]$,0)":
            mask = (df['lus_energy-balance_bioenergy_solid[TWh]'] > 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = - df.loc[~mask, "lus_energy-balance_bioenergy_solid[TWh]"]
        elif splitted == "if($lus_energy-balance_bioenergy_solid[TWh]$<0,0,$lus_energy-balance_bioenergy_solid[TWh]$)":
            mask = (df['lus_energy-balance_bioenergy_solid[TWh]'] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "lus_energy-balance_bioenergy_solid[TWh]"]
        elif splitted == "$bioenergy-import_wood_solid[TWh]$-if($bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$<0,$bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$,0)":
            mask = (df['bioenergy-balance_wood_solid[TWh]']-df['bioenergy-demand_elc_solid[TWh]']-df['bioenergy-demand_hea_solid[TWh]'] < 0)
            df.loc[mask, replaced_column] = df.loc[mask,'bioenergy-import_wood_solid[TWh]']-(df.loc[mask,'bioenergy-balance_wood_solid[TWh]']-df.loc[mask,'bioenergy-demand_elc_solid[TWh]']-df.loc[mask,'bioenergy-demand_hea_solid[TWh]'])
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$<0,0,$bioenergy-balance_wood_solid[TWh]$-$bioenergy-demand_elc_solid[TWh]$-$bioenergy-demand_hea_solid[TWh]$)":
            mask = (df['bioenergy-balance_wood_solid[TWh]']-df['bioenergy-demand_elc_solid[TWh]']-df['bioenergy-demand_hea_solid[TWh]'] < 0)
            df.loc[~mask, replaced_column] = (df.loc[~mask,'bioenergy-balance_wood_solid[TWh]']-df.loc[~mask,'bioenergy-demand_elc_solid[TWh]']-df.loc[~mask,'bioenergy-demand_hea_solid[TWh]'])
            df.loc[mask, replaced_column] = 0
        elif splitted == "-if($bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$<0,$bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$,0)":
            mask = (df['bioenergy-balance_wood_solid[TWh]']*0.8-df['bioenergy-demand_elc_gas[TWh]']-df['bioenergy-demand_agr_gas[TWh]']-df['bioenergy-demand_ref_gas[TWh]']-df['bioenergy-demand_hea_gas[TWh]'] < 0)
            df.loc[mask, replaced_column] = -(df.loc[mask,'bioenergy-balance_wood_solid[TWh]']*0.8-df.loc[mask,'bioenergy-demand_elc_gas[TWh]']-df.loc[mask,'bioenergy-demand_agr_gas[TWh]']-df.loc[mask,'bioenergy-demand_hea_gas[TWh]']-df.loc[mask,'bioenergy-demand_ref_gas[TWh]'])
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($bioenergy-balance_wood_solid[TWh]$*0.8-$bioenergy-demand_elc_gas[TWh]$-$bioenergy-demand_agr_gas[TWh]$-$bioenergy-demand_hea_gas[TWh]$-$bioenergy-demand_ref_gas[TWh]$<0,0,$bioenergy-balance_wood_solid[TWh]$-($bioenergy-demand_elc_gas[TWh]$+$bioenergy-demand_agr_gas[TWh]$+$bioenergy-demand_hea_gas[TWh]$+$bioenergy-demand_ref_gas[TWh]$)/0.8)":
            mask = (df['bioenergy-balance_wood_solid[TWh]']*0.8-df['bioenergy-demand_elc_gas[TWh]']-df['bioenergy-demand_agr_gas[TWh]']-df['bioenergy-demand_hea_gas[TWh]']-df['bioenergy-demand_ref_gas[TWh]'] < 0)
            df.loc[~mask, replaced_column] = (df.loc[~mask,'bioenergy-balance_wood_solid[TWh]']-(df.loc[~mask,'bioenergy-demand_elc_gas[TWh]']+df.loc[~mask,'bioenergy-demand_agr_gas[TWh]']+df.loc[~mask,'bioenergy-demand_ref_gas[TWh]']+df.loc[~mask,'bioenergy-demand_hea_gas[TWh]'])/0.8)
            df.loc[mask, replaced_column] = 0
        elif splitted ==  "max_in_args($energy-demand[TWh]$*(1-($bld_fuel-switch_biogas[%]$+$bld_fuel-switch_e-gas[%]$+$bld_fuel-switch_hydrogen[%]$)),0 )":
            mask = (df['energy-demand[TWh]']*(1-(df['bld_fuel-switch_biogas[%]']+df["bld_fuel-switch_e-gas[%]"]+df["bld_fuel-switch_hydrogen[%]"])) >= 0)
            df.loc[~mask, replaced_column] = 0
            df.loc[mask, replaced_column] = df.loc[mask,'energy-demand[TWh]']*(1-(df.loc[mask,'bld_fuel-switch_biogas[%]']+df.loc[mask,"bld_fuel-switch_e-gas[%]"]+df.loc[mask,"bld_fuel-switch_hydrogen[%]"]))
        elif splitted ==  "min_in_args($bld_fuel-switch_e-gas[%]$,(1-($bld_fuel-switch_hydrogen[%]$+$bld_fuel-switch_biogas[%]$)))":
            mask = (df['bld_fuel-switch_e-gas[%]'] < (1-(df["bld_fuel-switch_hydrogen[%]"]+df["bld_fuel-switch_biogas[%]"])))
            df.loc[mask, replaced_column] = df.loc[mask,'bld_fuel-switch_e-gas[%]']
            df.loc[~mask, replaced_column] = 1-(df.loc[~mask,"bld_fuel-switch_hydrogen[%]"]+df.loc[~mask,"bld_fuel-switch_biogas[%]"])
        elif splitted == "max_in_args($bld_fuel-switch_e-gas[%]$,0)":
            mask = (df['bld_fuel-switch_e-gas[%]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask,'bld_fuel-switch_e-gas[%]']
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($constructed-area-over-timestep[Mm2]$ > 0 , $constructed-area-over-timestep[Mm2]$, $demolished-area-over-timestep[Mm2]$)":
            mask = (df['constructed-area-over-timestep[Mm2]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, 'constructed-area-over-timestep[Mm2]']
            df.loc[~mask, replaced_column] = df.loc[~mask, 'demolished-area-over-timestep[Mm2]']
        elif splitted ==  "min_in_args($bld_fuel-switch_e-liquids[%]$,(1-$bld_fuel-switch_bioliquids[%]$))":
            mask = (df['bld_fuel-switch_e-liquids[%]'] > 1 - df['bld_fuel-switch_bioliquids[%]'])
            df.loc[mask, replaced_column] = 1 - df.loc[mask,'bld_fuel-switch_bioliquids[%]']
            df.loc[~mask, replaced_column] = df.loc[~mask,'bld_fuel-switch_e-liquids[%]']
        elif splitted == "max_in_args($bld_fuel-switch_e-liquids[%]$,0)":
            mask = (df['bld_fuel-switch_e-liquids[%]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, 'bld_fuel-switch_e-liquids[%]']
            df.loc[~mask, replaced_column] = 0
        elif splitted == "max_in_args($energy-demand[TWh]$*(1-($bld_fuel-switch_bioliquids[%]$+$bld_fuel-switch_e-liquids[%]$)),0 )":
            mask = (df['energy-demand[TWh]']*(1-(df['bld_fuel-switch_bioliquids[%]']+df['bld_fuel-switch_e-liquids[%]'])) > 0)
            df.loc[mask, replaced_column] = df.loc[mask,'energy-demand[TWh]']*(1-(df.loc[mask,'bld_fuel-switch_bioliquids[%]']+df.loc[mask,'bld_fuel-switch_e-liquids[%]']))
            df.loc[~mask, replaced_column] = 0
        elif splitted == "abs($elc_demand-imported[TWh]$)":
            df[replaced_column] = df["elc_demand-imported[TWh]"]
            df[replaced_column] = df[replaced_column].abs()
        elif splitted == "if($elc_demand-imported[TWh]$<0,0,$elc_demand-imported[TWh]$)":
            mask = (df['elc_demand-imported[TWh]'] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "elc_demand-imported[TWh]"]
        elif splitted == "if($elc_total-prod[TWh]$*(0.2812*$elc_intermitency-share[%]$-0.0228)<0,0,$elc_total-prod[TWh]$*(0.2812*$elc_intermitency-share[%]$-0.0228))":
            mask = (df['elc_total-prod[TWh]'] * (0.2812 * df['elc_intermitency-share[%]'] - 0.0228) < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "elc_total-prod[TWh]"] * (0.2812 * df.loc[~mask, "elc_intermitency-share[%]"] - 0.0228)
        elif splitted == "if($elc_intermitency-share[%]$<0.15,8,$transmission-capacity[GW]$)":
            mask = (df['elc_intermitency-share[%]'] < 0.15)
            df.loc[mask, replaced_column] = 8
            df.loc[~mask, replaced_column] = df.loc[~mask, "transmission-capacity[GW]"]
        elif splitted == "$${Ddegree_integration[%]}$$*(+263.69*$elc_intermitency-share[%]$*$elc_intermitency-share[%]$-13.252*$elc_intermitency-share[%]$+4.3033)":
            df[replaced_column] = kwargs["degree_integration_percent"] * (263.69 * df['elc_intermitency-share[%]'] * df['elc_intermitency-share[%]'] - 13.252 * df['elc_intermitency-share[%]'] + 4.3033)
        elif splitted == "$transmission-capacity-annual-increase[GW.km]$*$${Dtransmission_cost_MEUR_per_GWkm}$$":
            df[replaced_column] = df['transmission-capacity-annual-increase[GW.km]'] * kwargs["transmission_cost_MEUR_per_GWkm"]
        elif splitted == "$transmission-capacity[GW.km]$*0.01*$${Dtransmission_cost_MEUR_per_GWkm}$$":
            df[replaced_column] = df['transmission-capacity[GW.km]'] * 0.01 * kwargs["transmission_cost_MEUR_per_GWkm"]
        elif splitted == "$backup-production[GWh]$/$${Dloadfactor_backupplant[%]}$$":
            df[replaced_column] = df['backup-production[GWh]'] / kwargs["loadfactor_backupplant_percent"]
        elif splitted == "$backup-capacity-annual-increase[GW]$*$${Dbackup-capacity-capex[MEUR/GW]}$$":
            df[replaced_column] = df['backup-capacity-annual-increase[GW]'] * kwargs["backup_capacity_capex_MEUR_per_GW"]
        elif splitted == "$backup-capacity[GW]$*$${Dbackup-capacity-opexfixed[MEUR/GW]}$$":
            df[replaced_column] = df['backup-capacity[GW]'] * kwargs["backup_capacity_capex_MEUR_per_GW"]
        elif splitted == "$backup-consumption[GWh]$*$${Dbackup-capacity-opexvariable[EUR/MWh]}$$/1000":
            df[replaced_column] = df['backup-consumption[GWh]'] * kwargs["backup_capacity_opexvariable_EUR_MWh"] / 1000
        elif splitted == "$backup-production[GWh]$*$${Dloadfactor_backupplant[%]}$$":
            df[replaced_column] = df['backup-production[GWh]'] * kwargs["loadfactor_backupplant_percent"]
        elif splitted == "if($elc_link-costs-to-activity[-]$==1,$fuel-price_elc[MEUR/TWh]$,$tec_exogenous-energy-costs_electricity[EUR/MWh]$)":
            mask = (df['elc_link-costs-to-activity[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask,"fuel-price_elc[MEUR/TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_electricity[EUR/MWh]"]
        elif splitted == "if($elc_link-costs-to-activity[-]$ == 1, $fuel-price_hyd[MEUR / TWh]$, $tec_exogenous-energy-costs_hydrogen[EUR / MWh]$)":
            mask = (df['elc_link-costs-to-activity[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask,"fuel-price_hyd[MEUR / TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_hydrogen[EUR / MWh]"]
        elif splitted == "if($elc_link-costs-to-activity[-]$ == 1, $fuel-price_dhg[MEUR / TWh]$, $tec_exogenous-energy-costs_heat-waste[EUR / MWh]$)":
            mask = (df['elc_link-costs-to-activity[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask,"fuel-price_dhg[MEUR / TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_heat-waste[EUR / MWh]"]
        elif splitted == "if($elc_link-costs-to-activity_electricity[-]$==1,$fuel-price_elc[MEUR/TWh]$,$tec_exogenous-energy-costs_electricity[EUR/MWh]$)":
            mask = (df['elc_link-costs-to-activity_electricity[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask,"fuel-price_elc[MEUR/TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_electricity[EUR/MWh]"]
        elif splitted == "if($elc_link-costs-to-activity_hydrogen[-]$==1,$fuel-price_hyd[MEUR/TWh]$,$tec_exogenous-energy-costs_hydrogen[EUR/MWh]$)":
            mask = (df['elc_link-costs-to-activity_hydrogen[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask, "fuel-price_hyd[MEUR/TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_hydrogen[EUR/MWh]"]
        elif splitted == "if($elc_link-costs-to-activity_heat[-]$==1,$fuel-price_dhg[MEUR/TWh]$,$tec_exogenous-energy-costs_heat-waste[EUR/MWh]$)":
            mask = (df['elc_link-costs-to-activity_heat[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask, "fuel-price_dhg[MEUR/TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_heat-waste[EUR/MWh]"]
        elif splitted == "if($constructed-area-over-timestep_historical[Mm2]$ > 0 , $constructed-area-over-timestep_historical[Mm2]$, $demolished-area-over-timestep_historical[Mm2]$)":
            mask = (df['constructed-area-over-timestep_historical[Mm2]'] > 0)
            df.loc[~mask, replaced_column] = df.loc[~mask, "demolished-area-over-timestep_historical[Mm2]"]
        elif splitted == "if($Years_number$<2015,1,$renovation-mix$)":
            mask = (df['Years_number'] < 2015)
            df.loc[mask, replaced_column] = 1
        elif splitted == "max_in_args(min_in_args($bld_fuel-switch_hydrogen[%]$,(1-($bld_fuel-switch_biogas[%]$))),0)":
            temp_1 = df['bld_fuel-switch_hydrogen[%]']
            temp_2 = 1 - df['bld_fuel-switch_biogas[%]']
            df_temp = pd.DataFrame({"temp_1": temp_1, "temp_2": temp_2})
            df[replaced_column] = df_temp.min(axis=1)
            mask = (df[replaced_column] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "max_in_args(min_in_args($bld_fuel-switch_e-gas[%]$,(1-($bld_fuel-switch_hydrogen[%]$+$bld_fuel-switch_biogas[%]$))),0)":
            temp_1 = df['bld_fuel-switch_e-gas[%]']
            temp_2 = 1 - (df['bld_fuel-switch_hydrogen[%]'] + df['bld_fuel-switch_biogas[%]'])
            df_temp = pd.DataFrame({"temp_1": temp_1, "temp_2": temp_2})
            df[replaced_column] = df_temp.min(axis=1)
            mask = (df[replaced_column] < 0)
            df.loc[mask, replaced_column] = 0
        elif splitted == "if($lus_dyn_forest[ha]$ > 0, $lus_dyn_forest[ha]$ * 5000 / 1000000, 0)":
            mask = (df['lus_dyn_forest[ha]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_dyn_forest[ha]"] * 5000 / 1000000
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($dhg_pipes-total-over-timestep[km]$ < 0, 0 , $dhg_pipes-total-over-timestep[km]$)":
            mask = (df['dhg_pipes-total-over-timestep[km]'] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "dhg_pipes-total-over-timestep[km]"]
        elif splitted == "if($existing-unrenovated-area-over-timestep[Mm2]$<0,0 ,$existing-unrenovated-area-over-timestep[Mm2]$ )":
            mask = (df['existing-unrenovated-area-over-timestep[Mm2]'] < 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "existing-unrenovated-area-over-timestep[Mm2]"]
        elif splitted == "if($lus_change_forest[ha]$ > 0, $lus_change_forest[ha]$ * 5000 / 1000000, 0)":
            mask = (df['lus_change_forest[ha]'] > 0)
            df.loc[mask, replaced_column] = df.loc[mask, "lus_change_forest[ha]"] * 5000 / 1000000
            df.loc[~mask, replaced_column] = 0
        elif splitted == "if($elc_link-costs-to-activity_heat[-]$ == 1, $fuel-price_heat[MEUR / TWh]$, $tec_exogenous-energy-costs_heat-waste[EUR / MWh]$)":
            mask = (df['elc_link-costs-to-activity_heat[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask, "fuel-price_heat[MEUR / TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_heat-waste[EUR / MWh]"]
        elif splitted == "if($elc_link-costs-to-activity_heat[-]$==1,$fuel-price_heat[MEUR/TWh]$,$tec_exogenous-energy-costs_heat-waste[EUR/MWh]$)":
            mask = (df['elc_link-costs-to-activity_heat[-]'] == 1)
            df.loc[mask, replaced_column] = df.loc[mask, "fuel-price_heat[MEUR/TWh]"]
            df.loc[~mask, replaced_column] = df.loc[~mask, "tec_exogenous-energy-costs_heat-waste[EUR/MWh]"]
        elif splitted == "if($chp-pth-ratio[-]$==0,0,$elc_capacity_CHP[GW]$*$capacity-factor_CHP[%]$*8.76/$chp-pth-ratio[-]$)":
            mask = (df['chp-pth-ratio[-]'] == 0)
            df.loc[mask, replaced_column] = 0
            df.loc[~mask, replaced_column] = df.loc[~mask, "elc_capacity_CHP[GW]"] * df.loc[~mask, "capacity-factor_CHP[%]"] * 8.76 / df.loc[~mask, "chp-pth-ratio[-]"]

    df[replaced_column] = df[replaced_column].replace(np.inf, np.nan).astype(float)

    return df
