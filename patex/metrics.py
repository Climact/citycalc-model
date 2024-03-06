import re

import numpy as np
import pandas as pd


def turn_column_into_dimension(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Turn a column in a dataframe into an extra dimension.

    # Example

    >>> df = pd.DataFrame({
    ...     "dimension-names": ["area | cost", "area | cost", "building | cost", "cost"],
    ...     "dimension_1": ["10km2", "30km2", "house", "400$"],
    ...     "dimension_2": ["100$", "200$", "300$", ""],
    ...     "carrier": ["electricity", "gas", "electricity", "gas"],
    ... })
    >>> df = turn_column_into_dimension(df, "carrier")
    >>> df[sorted(df.columns)]
                 dimension-names dimension_1  dimension_2 dimension_3
    0      area | carrier | cost       10km2  electricity        100$
    1      area | carrier | cost       30km2          gas        200$
    2  building | carrier | cost       house  electricity        300$
    3             carrier | cost         gas         400$         NaN
    """
    def add_dimension_and_sort(df: pd.DataFrame) -> pd.DataFrame:
        dimensions = np.array(list(map(str.strip, df.name.split("|"))) + [column])
        sorted_indices = np.argsort(dimensions)
        df = df.assign(**{
            f"dimension_{len(dimensions)}": df[column],
            "dimension-names": " | ".join(dimensions[sorted_indices]),
        })
        del df[column]
        df = df.rename(columns={
            f"dimension_{j+1}": f"dimension_{i+1}" for (i, j) in enumerate(sorted_indices)
        })
        return df

    return df.groupby("dimension-names", sort=False, group_keys=False).apply(add_dimension_and_sort)


def sort_metric_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """Sort dimension names based on alphabetical order. DataFrame should be formatted using "dimension-names" and
    "dimension_i" columns.

    :param df: DataFrame to sort
    :return df: DataFrame sorted

    # Example

    >>> df = pd.DataFrame({
    ...     "dimension-names": ["cost | area", "cost | area", "building | cost", "cost"],
    ...     "dimension_1": ["100$", "200$", "house", "400$"],
    ...     "dimension_2": ["10km2", "30km2", "300$", ""],
    ... })
    >>> sort_metric_dimensions(df)
       dimension-names dimension_1 dimension_2
    0      area | cost       10km2        100$
    1      area | cost       30km2        200$
    2  building | cost       house        300$
    3             cost        400$            
    """
    if not "dimension-names" in df:
        return df
    
    def sort_dimensions(df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.name, float) and np.isnan(df.name):
            return df

        dimensions = np.array(list(map(str.strip, df.name.split("|"))))
        sorted_indices = np.argsort(dimensions)
        df = df.assign(**{
            f"dimension_{i+1}": df[f"dimension_{j+1}"] for (i, j) in enumerate(sorted_indices)
        })
        df = df.assign(**{"dimension-names": " | ".join(dimensions[sorted_indices])})
        return df

    return df.groupby("dimension-names", sort=False, group_keys=False, dropna=False).apply(sort_dimensions)


def create_validation_key(df: pd.DataFrame, metric_column: str, dimensions: list[str] = [], delete_dims: bool = False,
                          sort_dimensions: bool = True) -> pd.DataFrame:
    """
    Create validation key (key_metric-name-dim)

    :param df: DataFrame where key has to be added
    :param metric_column: Column name with metric names
    :param dimensions: List of dimension columns (optional - If False, look for dimension_i)
    :param delete_dims: Whether dimensions columns (dimension_i only) should be deleted or not (optional)
    :param sort_dimensions: Enable the dimension sort if needed (optional, only for dimension_i)
    :return ref_metrics: DataFrame with new column
    """
    df = df.copy()  # Copy is needed to avoid warning about copy of a slice from a DataFrame
    if not dimensions:
        if sort_dimensions:
            df = sort_metric_dimensions(df)
        rx_dim = re.compile("dimension_[0-9]+")
        dim_cols = sorted([c for c in df.columns if rx_dim.match(str(c))])
    else:
        dim_cols = list(set(dimensions).intersection(df.columns.tolist()))  # Be sure we keep only existing dimensions
        dim_cols = sorted(dim_cols)
    df["key_metric-name-dim"] = df[[metric_column] + dim_cols].agg(
        lambda x: '_'.join(c for c in x if isinstance(c, str)), axis=1)
    if not dimensions and delete_dims:
        for col in dim_cols:
            del df[col]

    return df


def exclude_years(df: pd.DataFrame, years_to_remove: list[int]) -> pd.DataFrame:
    """
    Remove excluded years from dataframe

    :param df: Dataframe where to remove the rows
    :param years_to_remove: List of years to remove
    :return: Dataframe with removed row corresponding to years_to_remove
    """
    if 'Years' in df.columns:
        df = df[~df['Years'].isin(years_to_remove)]
    return df


def create_dataframe(df_ots: pd.DataFrame, y_val: np.ndarray, dtype: str, x_val: np.ndarray = np.array([]),
                     y_column: str = "ColumnValues", unit_func = None, keep_sources: bool = False) -> pd.DataFrame:
    """
    Create a Dataframe using data provided in x_val and y_val and adding the same dimensions as OTS data.

    :param df_ots: DataFrame with OTS data as reference for dimensions
    :param y_val: y values associated to metric values (shape = (n, 1))
    :param dtype: Datatype of the created Dataframe
    :param x_val: x values associated to Years (shape = (n, 1)) (optional, no year by default)
    :param y_column: Column name to use for the value associated to y_val (optional, by default ColumnValues)
    :param unit_func: Determine function to apply on unit (optional, by default no change)
                        e.g.: "lambda x: x+'/yr'" > "[UNIT]/yr"
    :param keep_sources: Determine if sources related values have to be kept (optional, by default no)
    :return df: Dataframe with dimensions as described in metric descriptor
    """
    EXCLUSION_LIST = ["Years", "hypothesis", "completion_method_factor", "Region_source", "Region_0", "ColumnValues"]
    EXCLUSION_LIST = EXCLUSION_LIST + ["source", "source_link"] if not keep_sources else EXCLUSION_LIST
    METRIC_NAME = df_ots["key_metric-name"].iloc[0]

    # Create reference columns
    reference_columns = ["Region", "module", "data_type", "key_metric-name", "key_metric-name-dim", "Source"]
    dimension_names = [c for c in df_ots.columns if
                       c not in reference_columns + [METRIC_NAME] + EXCLUSION_LIST]
    reference_columns += dimension_names
    df = df_ots[reference_columns].iloc[[0]]

    # Fix dimension values
    df["data_type"] = dtype
    if unit_func:
        df["unit"] = df["unit"].apply(unit_func)

    # Generate data
    df["merge"] = 1
    if np.any(x_val):
        df_values = pd.DataFrame(np.concatenate([x_val, y_val, np.full((len(x_val), 1), 1)], axis=1),
                                 columns=["Years", y_column, "merge"])
        df_values["Years"] = df_values["Years"].astype("int")
    else:
        df_values = pd.DataFrame(np.concatenate([y_val, np.array([[1]])], axis=1), columns=[y_column, "merge"])
    df = df.merge(df_values, on="merge")
    del df["merge"]

    return df
