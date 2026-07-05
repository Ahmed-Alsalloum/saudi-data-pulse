"""Bulk-backfill Tadawul history: one yfinance call for a whole date range.

The daily asset fetches one partition per invocation, which is right for the
nightly schedule but slow for seeding months of history. This script downloads
the full range in a single request and lands the exact same per-day Parquet
partitions (`raw/tadawul/date=YYYY-MM-DD/part.parquet`), so the dbt sources
can't tell the difference. Non-trading days simply produce no rows.

Usage (writes to DATA_LAKE_PATH, same env contract as the assets):

    python scripts/backfill_tadawul.py --start 2026-01-01 --end 2026-07-02
"""

import argparse
from datetime import date, timedelta

import yfinance as yf

from orchestration.assets.ingestion.tadawul import TADAWUL_TICKERS, tidy_ohlcv
from orchestration.resources import DataLakeResource


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first trade date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last trade date (inclusive), YYYY-MM-DD")
    args = parser.parse_args()

    end_exclusive = date.fromisoformat(args.end) + timedelta(days=1)
    raw = yf.download(
        tickers=list(TADAWUL_TICKERS),
        start=args.start,
        end=end_exclusive.isoformat(),
        interval="1d",
        group_by="column",
        auto_adjust=True,
        progress=False,
    )
    tidy = tidy_ohlcv(raw, TADAWUL_TICKERS)
    if tidy.empty:
        raise SystemExit(f"no rows returned for {args.start}..{args.end}")

    lake = DataLakeResource()
    for day, chunk in tidy.groupby("trade_date"):
        path = lake.write_parquet(
            chunk.reset_index(drop=True), zone="raw", dataset="tadawul", partition=f"date={day}"
        )
        print(f"{day}: {len(chunk):3d} rows -> {path}")
    print(f"total: {len(tidy)} rows across {tidy['trade_date'].nunique()} trading days")


if __name__ == "__main__":
    main()
