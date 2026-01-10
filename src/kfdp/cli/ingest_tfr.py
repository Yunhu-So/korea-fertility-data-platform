from __future__ import annotations

import argparse
from pathlib import Path

from rich import print

from kfdp.io.excel_tfr import extract_tfr_long_from_excel
from kfdp.quality.checks import validate_tfr_long


def main() -> None:
    p = argparse.ArgumentParser(
        description="Ingest TFR Excel into Silver Parquet (long format)."
    )
    p.add_argument("--input", type=str, required=True, help="Path to source Excel file")
    p.add_argument(
        "--output", type=str, required=True, help="Output Parquet path (silver)"
    )
    args = p.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = extract_tfr_long_from_excel(in_path)
    validate_tfr_long(df)

    df.to_parquet(out_path, index=False)
    print(f"[green]OK[/green] Wrote {len(df)} rows -> {out_path}")


if __name__ == "__main__":
    main()
