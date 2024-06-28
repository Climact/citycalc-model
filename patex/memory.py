import logging
from typing import Any

import numpy as np
import pandas as pd


IDX = ["key_metric-name_wo-unit"]

def reduce_mem_usage(props):
    """Reduce the memory usage by converting the type of the dataframe columns

    :param props: dataframe
    :return: reduced dataframe
    """
    NAlist: list[Any] = []  # Keeps track of columns that have missing values filled in.
    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings
            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all():
                NAlist.append(col)
                IsInt = False
            else:
                # test if column can be converted to an integer
                asint = props[col].fillna(0).astype(np.int64)
                result = props[col] - asint
                result = result.sum()
                if result > -0.01 and result < 0.01:
                    IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                ###############################################################################
                # Disactivated: 487 MB => 460 MB in memory (with compression) but slowing down the converter
                ###############################################################################
                if mn >= 0:
                    pass
                    # props[col] = pd.to_numeric(props[col], downcast='signed')
                else:
                    pass
                    # props[col] = pd.to_numeric(props[col], downcast='unsigned')

            # Downcast floats
            else:
                ###############################################################################
                # THE DOWNCAST of FLOAT is causing issues in the results of Power and Buildings
                ###############################################################################

                # props[col] = pd.to_numeric(props[col], downcast='float')
                pass
                # props[col] = props[col].astype(np.float32)

            # Print new column type
            # print("dtype after: ", props[col].dtype)
            # print("******************************")

        # if object, convert to categories
        else:
            ###############################################################################
            # Disactivated: 487 MB => 400 MB in memory (with compression) but creating issues in 'missing_value_node' and 'pivoting_node' because the category dtype does not exist in np.dtypes
            ###############################################################################

            pass
            # num_unique_values = len(props[col].unique())
            # num_total_values = len(props[col])
            # if num_unique_values / num_total_values < 0.5:
            #     props.loc[:, col] = props[col].astype('category')
            # else:
            #     props.loc[:, col] = props[col]

    return props, NAlist


MEMO = {}


def read_memoized(path):
    if path in MEMO:
        return MEMO[path]
    else:
        df = pd.read_csv(path)
        MEMO[path] = df
        return df


def read_parquet_memoized(path, regions=None, modules=None, dtype=None):
    """
    Read parquet file from memoized cache or S3, and apply preprocessing for 'ods' type data.

    Args:
    - path (str): Path to the parquet file
    - regions (list, optional): List of regions to process
    - modules (list, optional): List of modules to process
    - dtype (str, optional): Data type to process

    Returns:
    - pd.DataFrame: DataFrame read from the parquet file
    """
    id_paths = generate_id_paths(path, regions, modules, dtype)

    output_df = []
    for id_path in id_paths:
        if id_path in MEMO:
            logging.debug(f"File from MEMO: {id_path}")
            output_df.append(MEMO[id_path])
        else:
            logging.debug(f"File from S3: {path}")
            df = pd.read_parquet(path)
            MEMO[path] = df

            # Pre-processing specific to 'ods' type data
            if 'ods' in path:
                df = preprocess_ods_data(df)
                keys = [path]
                regions_key = -1
                module_key = -1
                dtype_key = -1
                if 'Region' in df.columns:
                    keys = [f"{key}_{region}" for key in keys for region in df['Region'].unique()]
                if 'module' in df.columns:
                    keys = [f"{key}_{module}" for key in keys for module in df['module'].unique()]
                    regions_key -= 1
                if 'data_type' in df.columns:
                    keys = [f"{key}_{dtype}" for key in keys for dtype in df['data_type'].unique()]
                    module_key -= 1
                    regions_key -= 1
                for key in keys:
                    condition = (df['Region'].values == key.split('_')[regions_key] if 'Region' in df.columns else True) & \
                                (df['module'].values == key.split('_')[module_key] if 'module' in df.columns else True) & \
                                (df['data_type'].values == key.split('_')[dtype_key] if 'data_type' in df.columns else True)
                    # if condition is True (no Region, module or data_type in df), then the df is stored in MEMO
                    if isinstance(condition, bool) and condition:
                        MEMO[key] = df
                    else:
                        MEMO[key] = df.loc[condition]

            output_df.append(MEMO[id_path])

    return pd.concat(output_df)


def generate_id_paths(path, regions, modules, dtype):
    """ Generate id paths based on the path, regions, modules and dtype

    Args:
    - path (str): Path to the parquet file
    - regions (list, optional): List of regions to process
    - modules (list, optional): List of modules to process
    - dtype (str, optional): Data type to process

    Returns:
    - list: List of id paths
    """

    id_paths = [path]
    if regions:
        id_paths = [f"{id_path}_{region}" for id_path in id_paths for region in regions]
    if modules:
        id_paths = [f"{id_path}_{module}" for id_path in id_paths for module in modules]
    if dtype:
        id_paths = [f"{id_path}_{dtype}" for id_path in id_paths]
    return id_paths


def preprocess_ods_data(df, region=None):
    """
    Preprocess DataFrame specific to 'ods' type data.

    Args:
    - df (pd.DataFrame): DataFrame to preprocess

    Returns:
    - pd.DataFrame: Preprocessed DataFrame
    """
    if 'key_metric-name' in df.columns:
        df["key_metric-name_wo-unit"] = df["key_metric-name"].str.split("[").str[0].str.strip()
    else:
        df["key_metric-name_wo-unit"] = ""
    idx_t = [c for c in IDX if c in df.columns]
    df = df.set_index(idx_t)
    return df
