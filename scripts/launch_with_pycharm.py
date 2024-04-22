from pathlib import Path
import pandas as pd
from patex.patex import patex

WORKSPACE = Path(__file__).resolve().parents[2].joinpath("dev")  # path to the parent directory of '_common'
# (default = dev)
MAX_YEAR = 2050
REGIONS = "BE"  # ex : BE|HU
LEVERS_DEFAULT_VALUE = 1
DYN_LEVERS = {}


if __name__ == "__main__":
    levers = pd.read_excel(
        # TODO maybe we don't want to hardcode this? This is to avoid a dependency
        #   on `api`.
        io=WORKSPACE / "_common/configuration/interfaces.xlsx",
        sheet_name="Levers",
        usecols=["code"],
        header=0,
    )
    levers = levers["code"].tolist()
    levers = {lever: LEVERS_DEFAULT_VALUE for lever in levers}
    print(WORKSPACE)

    outputs = patex(
        local=WORKSPACE,
        base_year=2021,
        max_year=MAX_YEAR,
        country_filter="EU28|" + REGIONS,
        levers=levers,
        dynamic_levers=DYN_LEVERS,
    )
