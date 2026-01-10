from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Tuple, List

import pandas as pd


@dataclass(frozen=True)
class TFRExtractConfig:
    # None이면 "첫 시트" 자동 선택
    sheet_name: Optional[Union[str, int]] = None
    min_year: int = 1900
    max_year: int = 2100
    # 연도/값 최소 매칭 개수(파일마다 다를 수 있어 살짝 낮춰둠)
    min_year_cells: int = 3


def _to_int_if_yearish(x: object) -> Optional[int]:
    """Try to coerce x into an int year (handles 1970, 1970.0, '1970.0', etc)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    # pandas Timestamp -> use year
    if isinstance(x, pd.Timestamp):
        return int(x.year)

    # int
    if isinstance(x, int):
        return x

    # float like 1970.0
    if isinstance(x, float):
        if x.is_integer():
            return int(x)
        return None

    # strings like "1970", "1970.0", " 1970 "
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        # handle trailing .0
        if s.endswith(".0"):
            s = s[:-2]
        if s.isdigit():
            return int(s)
        return None

    # other numeric types
    try:
        s = str(x).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s.isdigit():
            return int(s)
    except Exception:
        return None

    return None


def _is_year(x: object, min_year: int, max_year: int) -> bool:
    y = _to_int_if_yearish(x)
    return y is not None and (min_year <= y <= max_year)


def _as_float(x: object) -> Optional[float]:
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
    all_sheets = pd.read_excel(
        excel_path, sheet_name=None, header=None, engine="openpyxl"
    )
    if not isinstance(all_sheets, dict) or len(all_sheets) == 0:
        raise ValueError("Failed to read sheets from Excel.")
    first_key = next(iter(all_sheets.keys()))
    return all_sheets[first_key]


def _detect_year_and_value_rows(
    raw: pd.DataFrame, min_year: int, max_year: int, min_year_cells: int
) -> Tuple[int, int, List[int]]:
    """
    Find (year_row_idx, value_row_idx, year_cols) by:
      - scanning rows for many year-like cells
      - ensuring the NEXT row has numeric-ish values in those same cols
    """
    best = None  # (score, year_row, value_row, cols)
    nrows = len(raw)

    for r in range(nrows - 1):
        row = raw.iloc[r]
        year_cols = [c for c in raw.columns if _is_year(row[c], min_year, max_year)]
        if len(year_cols) < min_year_cells:
            continue

        next_row = raw.iloc[r + 1]
        numeric_count = 0
        for c in year_cols:
            if _as_float(next_row[c]) is not None:
                numeric_count += 1

        # score: prioritize more matched years and more numeric values below
        score = len(year_cols) * 10 + numeric_count

        if best is None or score > best[0]:
            best = (score, r, r + 1, year_cols)

    if best is None:
        raise ValueError(
            "Could not detect a 'year header row'. "
            "Likely the years are stored in an unexpected format or the table layout differs."
        )

    _, year_row_idx, value_row_idx, cols = best
    return year_row_idx, value_row_idx, cols


def extract_tfr_long_from_excel(
    excel_path: Path,
    config: TFRExtractConfig = TFRExtractConfig(),
) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel not found: {excel_path}")

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

    years = []
    values = []
    for c in year_cols:
        y = _to_int_if_yearish(year_row[c])
        v = _as_float(value_row[c])
        if y is None:
            continue
        years.append(int(y))
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
