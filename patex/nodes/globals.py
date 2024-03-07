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

from pathlib import Path
import typing

import pandas as pd


class Globals:
    local: str
    base_year: int
    max_year: int
    country_filter: str
    levers: dict[str, float]
    dynamic_levers: dict[str, typing.Any]
    missing_years: list[int]

    def __init__(
        self,
        local: str,
        base_year: int,
        max_year: int,
        country_filter: str,
        levers: dict[str, float],
        dynamic_levers: dict[str, typing.Any],
    ):
        self.local = local
        self.base_year = base_year
        self.max_year = max_year
        self.country_filter = country_filter
        self.levers = levers
        self.dynamic_levers = dynamic_levers

        # TODO don't hardcode this
        PATH_YEARS_LIST = Path(self.local, "_common", "reference", "ref_years.xlsx")
        ref_years = pd.read_excel(PATH_YEARS_LIST)
        self.missing_years = list(
            ref_years[ref_years["cds_optional"]]["Years"].astype(int)
        )

    @classmethod
    def get(cls):
        global _globals
        assert _globals, "Globals not set"
        return _globals[-1]
    
    def __enter__(self):
        Globals.push(self)
        return self
    
    def __exit__(self, *args):
        Globals.pop()
    
    @classmethod
    def push(cls, globals):
        global _globals
        _globals.append(globals)
    
    @classmethod
    def pop(cls):
        global _globals
        _globals.pop()


_globals = []
