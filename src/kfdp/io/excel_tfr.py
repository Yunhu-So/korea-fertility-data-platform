from __future__ import annotations

"""
Utilities for extracting Total Fertility Rate time series from Excel files.

The source spreadsheets for TFR are usually arranged with a row of years
followed by a row of numeric values.  This module detects the year and
value rows heuristically and returns a tidy DataFrame suitable for
downstream processing.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Tuple, List

import pandas as pd


@dataclass(frozen=True)
class TFRExtractConfig:
    """
    Configuration for extracting a Total Fertility Rate time series from an Excel file.

    Attributes
    ----------
    sheet_name: Optional[Union[str, int]]
        If provided, a sheet name or index to read.  If ``None``, the first sheet is used.
    min_year: int
        Lower bound for plausible year values.
    max_year: int
        Upper bound for plausible year values.
    min_year_cells: int
        Minimum number of year‑like cells required to detect the header row.
    """

    sheet_name: Optional[Union[str, int]] = None
    min_year: int = 1900
    max_year: int = 2100
    min_year_cells: int = 3


def _to_int_if_yearish(x: object) -> Optional[int]:
    """Attempt to coerce ``x`` into an integer year (e.g. 1970, 1970.0, '1970.0')."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, pd.Timestamp):
        return int(x.year)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x) if x.is_integer() else None
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if s.endswith(".0"):
            s = s[:-2]
        return int(s) if s.isdigit() else None
    # Fallback: try string conversion
    try:
        s = str(x).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def _is_year(x: object, min_year: int, max_year: int) -> bool:
    """Return ``True`` if ``x`` is a year within the configured range."""
    y = _to_int_if_yearish(x)
    return y is not None and (min_year <= y <= max_year)


def _as_float(x: object) -> Optional[float]:
    """Convert ``x`` to a float if possible, otherwise return ``None``."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None
    try:
        return float(x)
    except Exception:
        return None


def _read_first_sheet_as_df(excel_path: Path) -> pd.DataFrame:
    """Read the first sheet of an Excel workbook into a DataFrame with no header."""
    all_sheets = pd.read_excel(excel_path, sheet_name=None, header=None, engine="openpyxl")
    if not isinstance(all_sheets, dict) or len(all_sheets) == 0:
        raise ValueError("Failed to read sheets from Excel.")
    first_key = next(iter(all_sheets.keys()))
    return all_sheets[first_key]


def _detect_year_and_value_rows(
    raw: pd.DataFrame, min_year: int, max_year: int, min_year_cells: int
) -> Tuple[int, int, List[int]]:
    """
    Detect the header row containing years and the subsequent row containing values.

    The function scans each row looking for a set of columns that contain many
    year‑like values (according to ``_is_year``).  It then checks that the next
    row has numeric values in those same columns.  A simple scoring heuristic
    prefers rows with more matched years and more numeric values below.
    """
    best: Optional[Tuple[int, int, int, List[int]]] = None  # (score, year_row_idx, value_row_idx, columns)

    for row_idx in range(len(raw) - 1):
        row = raw.iloc[row_idx]
        year_cols = [col for col in raw.columns if _is_year(row[col], min_year, max_year)]
        if len(year_cols) < min_year_cells:
            continue

        next_row = raw.iloc[row_idx + 1]
        numeric_count = sum(1 for c in year_cols if _as_float(next_row[c]) is not None)

        score = len(year_cols) * 10 + numeric_count
        if best is None or score > best[0]:
            best = (score, row_idx, row_idx + 1, year_cols)

    if best is None:
        raise ValueError(
            "Could not detect a row with year headers.  "
            "Check that the file contains a row of years followed by numeric values."
        )

    _, year_row_idx, value_row_idx, cols = best
    return year_row_idx, value_row_idx, cols


def extract_tfr_long_from_excel(
    excel_path: Path,
    config: TFRExtractConfig = TFRExtractConfig(),
) -> pd.DataFrame:
    """
    Extract a long‑format DataFrame of total fertility rate values from an Excel workbook.

    The resulting DataFrame has columns: ``country``, ``indicator``, ``year``, ``value``
    and ``source_file``.  Any rows with missing values are dropped and the result
    is sorted by year.
    """
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    if config.sheet_name is None:
        raw = _read_first_sheet_as_df(excel_path)
    else:
        raw = pd.read_excel(
            excel_path, sheet_name=config.sheet_name, header=None, engine="openpyxl"
        )

    year_row_idx, value_row_idx, year_cols = _detect_year_and_value_rows(
        raw, config.min_year, config.max_year, config.min_year_cells
    )

    year_row = raw.iloc[year_row_idx]
    value_row = raw.iloc[value_row_idx]

    years: List[int] = []
    values: List[float] = []
    for col in year_cols:
        y = _to_int_if_yearish(year_row[col])
        v = _as_float(value_row[col])
        if y is None:
            continue
        years.append(int(y))
        # Represent missing numeric values as NaN so that pandas can handle them
        values.append(float("nan") if v is None else float(v))

    df = pd.DataFrame(
        {
            "country": "KOR",
            "indicator": "TFR",
            "year": years,
            "value": values,
            "source_file": excel_path.name,
        }
    )

    df = df.dropna(subset=["value"]).sort_values("year").reset_index(drop=True)
    return df
