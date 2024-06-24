import logging


def get_ref_years(df_years) -> dict[str, list]:
    """
    Get reference years list, including minimal historical years list, full historical years list and
    minimal timeserie

    :param df_years: Dataframe with years
    :return ref_years: Reference years lists
    """
    logging.debug("Load ref_years table")
    ref_years = {}
    df_historical = df_years[df_years["years_category"].values == "historical"]
    ref_years["historical_full"] = df_historical["Years"].tolist()
    ref_years["historical_min"] = \
        df_historical[(~df_historical["cds_optional"]) & (~df_historical["ods_optional"])]["Years"].tolist()
    ref_years["timeserie_min"] = df_years[~df_years["ods_optional"]]["Years"].tolist()
    ref_years["futur_full"] = df_years[df_years["cds_optional"]]["Years"].tolist()
    ref_years["futur_min"] = df_years[df_years["years_category"].values == "projections"]["Years"].tolist()
    return ref_years
