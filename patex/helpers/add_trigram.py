# ----------------------------------------------------------------------------------------------------- #
# 2021, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

"""
    ADD TRIGRAM
    ===========
    KNIME options implemented:
        - ALL
"""

import pandas as pd


def add_trigram(df, module_name) -> pd.DataFrame:
    # Find variables
    dimensions = list(df.select_dtypes(["int", "object"]).columns)
    variables = [c for c in df.columns if c not in dimensions]
    df = df[dimensions + variables]

    # Rename variables
    if module_name == "lifestyle":
        trigram = "lfs_"
    elif module_name == "climate":
        trigram = "clm_"
    elif module_name == "technology":
        trigram = "tec_"
    elif module_name == "air_quality":
        trigram = "air_"
    elif module_name == "climate emissions":
        trigram = "clt_"
    elif module_name == "buildings":
        trigram = "bld_"
    elif module_name == "transport":
        trigram = "tra_"
    elif module_name == "industry":
        trigram = "ind_"
    elif module_name == "ammonia":
        trigram = "amm_"
    elif module_name == "power":
        trigram = "pow_"
    elif module_name == "land use":
        trigram = "lus_"
    elif module_name == "minerals":
        trigram = "min_"
    elif module_name == "agriculture":
        trigram = "agr_"
    elif module_name == "bioenergy balance":
        trigram = "bio_"
    elif module_name == "electricity_supply":
        trigram = "elc_"
    elif module_name == "scope_2_3":
        trigram = "sco_"
    elif module_name == "water":
        trigram = "wat_"
    else:
        msg = f"No trigram for following module-name: {module_name}."
        raise RuntimeError(msg)

    variables = [trigram + c for c in variables]

    # Create output
    df.columns = dimensions + variables
    return df
