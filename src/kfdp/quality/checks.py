from __future__ import annotations

"""
Validation routines for Total Fertility Rate data.

These checks enforce basic expectations about the extracted TFR DataFrame,
ensuring that required columns are present, values are non‑negative and
unique per year, and that there are no missing years or values.
"""

import pandas as pd


def validate_tfr_long(df: pd.DataFrame) -> None:
    """
    Raise an exception if the TFR DataFrame does not meet basic quality expectations.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame produced by ``extract_tfr_long_from_excel``.

    Raises
    ------
    ValueError
        If any of the required constraints are violated.
    """
    required_cols = {"country", "indicator", "year", "value", "source_file"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    if df.empty:
        raise ValueError("DataFrame is empty after extraction.")

    if (df["indicator"] != "TFR").any():
        raise ValueError("Unexpected indicator values found (expected only 'TFR').")

    if df["year"].isna().any():
        raise ValueError("Year contains NA.")

    if df["value"].isna().any():
        raise ValueError("Value contains NA.")

    if (df["value"] < 0).any():
        raise ValueError("TFR cannot be negative.")

    # Basic sanity: years should be unique for a single indicator/country file
    if df.duplicated(subset=["country", "indicator", "year"]).any():
        raise ValueError("Duplicate (country, indicator, year) rows detected.")
