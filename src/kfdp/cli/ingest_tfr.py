from __future__ import annotations

"""
Command‑line interface for ingesting a Total Fertility Rate Excel file.

This script reads an Excel workbook containing fertility rates, extracts a
normalized time series using the functions in :mod:`kfdp.io.excel_tfr`,
validates the resulting DataFrame, and writes it out as a parquet file.
"""

import argparse
from pathlib import Path

from rich import print

from kfdp.io.excel_tfr import extract_tfr_long_from_excel
from kfdp.quality.checks import validate_tfr_long


def main() -> None:
    """Parse arguments, ingest the Excel file and write a parquet file."""
    parser = argparse.ArgumentParser(
        description=(
            "Ingest a Total Fertility Rate Excel file into a silver parquet (long format)."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the source Excel file",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to write the output parquet file",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = extract_tfr_long_from_excel(input_path)
    validate_tfr_long(df)

    df.to_parquet(output_path, index=False)
    print(f"[green]OK[/green] Wrote {len(df)} rows -> {output_path}")


if __name__ == "__main__":
    main()
