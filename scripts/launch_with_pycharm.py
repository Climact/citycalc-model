import time
from pathlib import Path
import pandas as pd
from patex.patex import patex
from patex.formating import get_ref_years
import logging

WORKSPACE = Path(__file__).resolve().parents[2]
MAX_YEAR = 2050
REGIONS = "FR0196"  # ex : BE|HU
MODE = "local"  # "local" or "remote"
LEVERS_DEFAULT_VALUE = 1
DYN_LEVERS = {}
BUCKET = "climact-dataset"
PROJECT = "citycalc" # "patex" or "citycalc"
VERSION = "dev"


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    if MODE == "local":
        logging.info(f"Running in local mode : {WORKSPACE}")
        ods_folder = Path(WORKSPACE,"dev","_common","_ods")
        level_data_folder = Path(WORKSPACE,"dev", '_common', "_level_data")
        interfaces_path = WORKSPACE.joinpath("api", "param", "interfaces.xlsx")
        ref_years_path = WORKSPACE.joinpath("api", "param", "ref_years.csv")
    else:
        s3_path = f"s3://{BUCKET}/{PROJECT}/{VERSION}"
        logging.info(f"Running in remote mode : {s3_path}")
        ods_folder = f"{s3_path}/ods"
        raw_folder = f"{s3_path}/raw"
        level_data_folder = None
        interfaces_path = f"{raw_folder}/configuration/interfaces.xlsx"
        ref_years_path = f"{raw_folder}/reference/ref_years.csv"

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

    # Run the model
    logging.info("executing the model...")
    start = time.time()
    outputs = patex(
        mode=MODE,
        ods_folder=ods_folder,
        level_data_folder=level_data_folder,
        ref_years=ref_years,
        base_year=2021,
        max_year=MAX_YEAR,
        country_filter="EU28|" + REGIONS,
        levers=levers,
        dynamic_levers=DYN_LEVERS,
    )
    logging.info(f"time = {time.time() - start:.2f}s")
