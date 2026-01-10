from __future__ import annotations

import pandas as pd


def validate_tfr_long(df: pd.DataFrame) -> None:
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
