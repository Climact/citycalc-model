import time
import pickle
import warnings
import logging
import argparse
from pathlib import Path

import pandas as pd

from patex.patex.patex import patex

warnings.simplefilter(action="ignore", category=FutureWarning)

logging.getLogger().setLevel(logging.INFO)

# Parse CLI arguments
parser = argparse.ArgumentParser(
    prog="patex", formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument(
    "region", help='region(s) on which to run the model, like "BE" or "EU27 as sum"'
)
parser.add_argument(
    "-w",
    "--workspace",
    type=Path,
    default=Path("dev"),
    help="path to the parent directory of '_common'",
)
parser.add_argument(
    "--levers-default",
    type=int,
    default=1,
    help="the value each lever will take if not specified in `--levers-file`",
)
parser.add_argument(
    "-l",
    "--levers-file",
    type=Path,
    required=False,
    help='a JSON file with lever settings, formatted like `{"lever_name": 1.8, ...}`',
)
parser.add_argument(
    "-ldyn",
    "--levers-dynamic-file",
    type=Path,
    required=False,
    help='a JSON file with dynamic lever settings, formatted like `{"lever_name": {"start_year": 2019, "end_year": 2050'
         ', "target": 1.1, "curve_type": "s-curve"}, ...}`',
)
parser.add_argument(
    "-y",
    "--max-year",
    type=int,
    choices=[2050, 2100],
    default=2050,
    help="maximum forecasting year for the Patex",
)
parser.add_argument(
    "-o",
    "--output",
    type=Path,
    help="path to a file where the model's outputs are written as pickle",
)
args = parser.parse_args()

# Read levers from config file
logging.info("reading lever names from 'interfaces.xlsx'...")
start = time.time()
levers = pd.read_excel(
    # TODO maybe we don't want to hardcode this? This is to avoid a dependency
    #   on `api`.
    io=args.workspace / "_common/configuration/interfaces.xlsx",
    sheet_name="Levers",
    usecols=["code"],
    header=0,
)
levers = levers["code"].tolist()
logging.info(f"time = {time.time() - start:.2f}s")

# Special case if region is "EU27 as sum"
if args.region == "EU27 as sum":
    args.region = "AT|BE|BG|HR|CY|CZ|DK|EE|FI|FR|DE|EL|HU|IE|IT|LV|LT|LU|MT|NL|PL|PT|RO|SK|SI|ES|SE"

# Set all levers to their default value
levers = {lever: args.levers_default for lever in levers}

# Hydrate with the optional levers file
if args.levers_file is not None:
    import json

    with open(args.levers_file, "r") as f:
        levers.update(json.load(f))

# Set dynamic levers to their default value
dyn_levers = {}

# Hydrate with optional dynamic levers file
if args.levers_dynamic_file is not None:
    import json

    with open(args.levers_dynamic_file, "r") as f:
        dyn_levers.update(json.load(f))

# Run the model
logging.info("executing the model...")
start = time.time()
outputs = patex(
    local=args.workspace,
    base_year=2021,
    max_year=args.max_year,
    country_filter="EU28|" + args.region,
    levers=levers,
    dynamic_levers=dyn_levers,
)
logging.info(f"time = {time.time() - start:.2f}s")

# Optionally write its output to a file
if args.output:
    with open(args.output, "wb") as f:
        pickle.dump(outputs, f)
    logging.info(f"wrote output pickle to '{args.output}'")
