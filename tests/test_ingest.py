from pathlib import Path

import pandas as pd

from kfdp.io.excel_tfr import extract_tfr_long_from_excel
from kfdp.quality.checks import validate_tfr_long


def test_extract_and_validate():
    p = Path("data/bronze/tfr_source.xlsx")
    df = extract_tfr_long_from_excel(p)
    validate_tfr_long(df)

    assert not df.empty
    assert set(["country", "indicator", "year", "value", "source_file"]).issubset(
        df.columns
    )
    assert df["year"].is_monotonic_increasing
