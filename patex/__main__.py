import time
import pickle
import warnings
import logging
import argparse

from api.src.configuration import get_basic_configuration, regions_init, levers_init
from patex.patex.patex import patex

warnings.simplefilter(action="ignore", category=FutureWarning)

logging.getLogger().setLevel(logging.INFO)

parser = argparse.ArgumentParser(prog="patex")
parser.add_argument(
    "-c", "--config-file", default="api/config/config_local_eu2050.yml"
)
parser.add_argument("country")
parser.add_argument("level", type=int)
parser.add_argument(
    "-y", "--max-year", type=int, choices=[2050, 2100], default=2050
)
parser.add_argument("-o", "--output")
args = parser.parse_args()

start = time.time()
cfg = get_basic_configuration(args.config_file)[-1]
regions = regions_init(cfg, logging.getLogger())
levers, _, _ = levers_init(cfg, logging.getLogger(), regions)
logging.info(f"loading config time = {time.time() - start:.2f}s")

if args.country == "EU27 as sum":
    args.country = "AT|BE|BG|HR|CY|CZ|DK|EE|FI|FR|DE|EL|HU|IE|IT|LV|LT|LU|MT|NL|PL|PT|RO|SK|SI|ES|SE"

levers = {lever: args.level for lever in levers}

start = time.time()
outputs = patex(
    local="dev",
    base_year=2021,
    max_year=args.max_year,
    country_filter="EU28|" + args.country,
    levers=levers,
    dynamic_levers={},
)
logging.info(f"model exec time = {time.time() - start:.2f}s")

if args.output:
    with open(args.output, "wb") as f:
        pickle.dump(outputs, f)
    logging.info(f"wrote output pickle to '{args.output}'")
