import time
import pickle
import warnings
import logging
import argparse
from pathlib import Path

import pandas as pd

from patex.patex.patex import patex

logging.getLogger().setLevel(logging.INFO)

# Parse CLI arguments
parser = argparse.ArgumentParser(
    prog="patex.compare_model_outputs",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--continue",
    dest="cont",
    action="store_true",
    help="if set, continue after the first failure",
)
parser.add_argument(
    "-q",
    "--quiet",
    action="store_true",
    help="if set, only print failures",
)
parser.add_argument("left", help="first output file to compare")
parser.add_argument("right", help="second output file to compare")
args = parser.parse_args()

has_errors = False


def err(e):
    """Raise the error `e` if `args.cont` is `False`, print it otherwise."""
    global has_errors
    has_errors = True
    if args.cont:
        logging.error(e)
    else:
        raise e


# Load the files
with open(args.left, "rb") as f:
    output_1 = pickle.load(f)
with open(args.right, "rb") as f:
    output_2 = pickle.load(f)

# Ensure the keys are the same
keys_1 = set(output_1.keys())
keys_2 = set(output_2.keys())
if keys_1 != keys_2:
    err(
        AssertionError(
            f"""outputs don't share the same keys:
'{args.left}' is missing {keys_2 - keys_1}
'{args.right}' is missing {keys_1 - keys_2}"""
        )
    )

# Compare the output of each key
for key in keys_1:
    x = output_1[key]
    y = output_2[key]
    if not args.quiet:
        logging.info(f"output '{key}'")

    # We sort the columns and values beforehand, to ensure the comparison is
    # independent of order
    x = x[
        sorted(x.select_dtypes("object").columns)
        + sorted(x.select_dtypes(exclude="object").columns)
    ]
    x = x.sort_values(by=x.columns.tolist()).reset_index(drop=True)
    y = y[
        sorted(y.select_dtypes("object").columns)
        + sorted(y.select_dtypes(exclude="object").columns)
    ]
    y = y.sort_values(by=y.columns.tolist()).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(x, y)
    except AssertionError as e:
        err(e)

if has_errors:
    exit(1)
