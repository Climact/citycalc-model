import pandas as pd

from patex.helpers.globals import Globals
from patex.helpers import *

from .agriculture import agriculture, land_use, bioenergy_balance
from .air_quality import air_quality
from .buildings import buildings
from .climate_emissions import climate_emissions
from .electricity_supply import electricity_supply
from .industry import industry
from .lifestyle import lifestyle
from .res_share import res_share
from .scope_2_3 import scope_2_3
from .transport import transport
from .water import water


def patex(
    local,
    s3_ods,
    s3_raw,
    ref_years,
    country_filter,
    levers,
    dynamic_levers,
    base_year=2021,
    max_year=2050,
):
    with Globals(
        local=local,
        s3_ods=s3_ods,
        s3_raw=s3_raw,
        ref_years=ref_years,
        base_year=base_year,
        max_year=max_year,
        country_filter=country_filter,
        levers=levers,
        dynamic_levers=dynamic_levers,
    ):
        return patex_node()


def patex_node():
    # Select parameters


    # Import data for corresponding ambition levels


    # Macro-Economy
    # DISCONNECTED FOR THE MOMENT !!
    # Deleted => should be set again
    # (but inside each modules
    # instead)


    # Import lever positions


    # Visualize outputs


    # DB
    # Il manque les DB de :
    # - Air Quality
    # - Minerals
    # - Macro-eco
    # => A rajouter quand on veut ces modules (cfr version antérieure à la release v14.1)


    # Select Countries 
    # to run


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Visualize outputs


    # Export to database and to google sheet


    # Visualize outputs




    # Lifestyle

    # Lifestyle module
    lfs_out, _, lfs_bld, lfs_tra, lfs_ind, lfs_agr, lfs_wat = lifestyle()

    # Import historical and future time series for all levers

    # Transport

    # Transport module
    tra_out, tra_ind, tra_pow, _, _, _, tra_clt, _, tra_air, _ = transport(lifestyle=lfs_tra)

    # Buildings

    # Buildings module
    bld_out, _, bld_ind, bld_pow, bld_clt, bld_air, _ = buildings(lifestyle=lfs_bld)

    # Agriculture

    # Agriculture module
    agr_out, _, agr_ind, agr_lus, agr_pow, agr_bio, agr_clt, agr_air, _, agr_sco, agr_wat = agriculture(lifestyle=lfs_agr)

    # Industry

    # Industry module
    ind_out, _, ind_bio, ind_pow, ind_clt, ind_air, _, ind_wat, ind_sco = industry(lifestyle=lfs_ind, buildings=bld_ind, transport=tra_ind, agriculture=agr_ind)

    # Power Supply

    # Power supply module
    pow_out, _, pow_clt, pow_bio, pow_air, _, pow_sco, pow_wat = electricity_supply(transport=tra_pow, buildings=bld_pow, industry=ind_pow, agriculture=agr_pow)

    # Water

    wat_out, _ = water(lifestyle=lfs_wat, industry=ind_wat, agriculture=agr_wat, power=pow_wat)

    # Energy calculation: RES share and Sankey

    # Wrond input data: it should come from the powersupply module (output to TPE) using the final-energy-demand

    res_share_out_1, res_share_out_2 = res_share(buildings=bld_pow, transport=tra_pow, agriculture=agr_pow, industry=ind_pow, power=pow_out)

    # Land use

    # Land-Use module (module-name = agriculture)
    lus_out, _, lus_bio, lus_clt, lus_air = land_use(agriculture=agr_lus)

    # Air Quality

    air_out, _ = air_quality(buildings=bld_air, transport=tra_air, agriculture=agr_air, land_use=lus_air, industry=ind_air, power=pow_air)

    # BioEnergy


    lus_bio = column_filter(df=lus_bio, columns_to_drop=[])
    # Bioenergy Balance module (module-name = agriculture)
    bio_out, _, bio_sco = bioenergy_balance(industry=ind_bio, agriculture=agr_bio, land_use=lus_bio, power=pow_bio)

    # Scope 2/3

    sco_out = scope_2_3(power=pow_sco, industry=ind_sco, agriculture=agr_sco, bioenergy_balance=bio_sco)

    # EMISSIONS

    clt_out_1, clt_out_2 = climate_emissions(buildings=bld_clt, transport=tra_clt, industry=ind_clt, agriculture=agr_clt, land_use=lus_clt, power=pow_clt)

    return {
        'node_9109_out_1': bld_out,
        'node_9076_out_1': bio_out,
        'node_9096_out_1': tra_out,
        'node_9100_out_1': pow_out,
        'node_9097_out_1': agr_out,
        'node_9099_out_1': ind_out,
        'node_9098_out_1': lus_out,
        'node_9094_out_1': lfs_out,
        'node_9053_out_1': clt_out_1,
        'node_9053_out_2': clt_out_2,
        'node_9000_out_1': res_share_out_1,
        'node_9000_out_2': res_share_out_2,
        'node_9101_out_1': sco_out,
        'node_1_out_1': wat_out,
        'node_9102_out_1': air_out,
    }


