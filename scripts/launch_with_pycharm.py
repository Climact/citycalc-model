import time
from pathlib import Path
import pandas as pd
from patex.patex import patex
from patex.utils import get_ref_years
import logging

WORKSPACE = Path(__file__).resolve().parents[2].joinpath("dev")  # path to the parent directory of '_common'
# (default = dev)
MAX_YEAR = 2050
REGIONS = "FR0196"  # ex : BE|HU
MODE = "remote"  # "local" or "remote"
LEVERS_DEFAULT_VALUE = 1
DYN_LEVERS = {}
BUCKET = "climact-dataset"
PROJECT = "citycalc" # "patex" or "citycalc"
VERSION = "dev"
s3_path = f"s3://{BUCKET}/{PROJECT}/{VERSION}"
s3_ods = f"{s3_path}/ods"
s3_raw = f"{s3_path}/raw"


if __name__ == "__main__":

    if MODE == "local":
        logging.info(f"Running in local mode : {WORKSPACE}")
        interfaces_path = WORKSPACE.joinpath("_common", "configuration", "interfaces.xlsx")
        ref_years_path = WORKSPACE.joinpath("_common", "reference", "ref_years.csv")
    else:
        logging.info(f"Running in remote mode : {s3_path}")
        interfaces_path = f"{s3_raw}/configuration/interfaces.xlsx"
        ref_years_path = f"{s3_raw}/reference/ref_years.csv"

    logging.info("reading lever names from 'interfaces.xlsx'...")
    start = time.time()
    levers = pd.read_excel(
        io=interfaces_path,
        sheet_name="Levers",
        header=0,
        usecols=["code"],
    )
    ref_years = pd.read_csv(ref_years_path)
    ref_years = get_ref_years(ref_years)
    levers = levers["code"].tolist()
    logging.info(f'time = {time.time() - start:.2f}s')

    # Set all levers to their default value
    levers = {lever: LEVERS_DEFAULT_VALUE for lever in levers}

    outputs = patex(
        local=WORKSPACE if MODE == "local" else None,
        s3_ods=s3_ods if MODE == "remote" else None,
        s3_raw=s3_raw if MODE == "remote" else None,
        ref_years=ref_years,
        base_year=2021,
        max_year=MAX_YEAR,
        country_filter="EU28|" + REGIONS,
        levers=levers,
        dynamic_levers=DYN_LEVERS,
    )
