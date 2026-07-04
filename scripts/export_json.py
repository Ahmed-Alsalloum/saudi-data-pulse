"""Export small JSON summaries from the warehouse for the static dashboard.

The GitHub Pages dashboard reads these files from the `data` branch via
raw.githubusercontent.com, so it needs no backend at all. Keep exports small
(days, not history) — the API serves anything deeper.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb

WAREHOUSE_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    result = con.execute(sql)
    columns = [d[0] for d in result.description]
    return [
        {
            col: (str(v) if hasattr(v, "isoformat") else v)
            for col, v in zip(columns, row, strict=True)
        }
        for row in result.fetchall()
    ]


def main() -> None:
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    exports = {
        "market_summary.json": rows_as_dicts(
            con,
            """
            select * from daily_market_summary
            where trade_date >= (select max(trade_date) from daily_market_summary) - interval 14 day
            order by trade_date desc, sector
            """,
        ),
        "weather_daily.json": rows_as_dicts(
            con,
            """
            select * from weather_daily_summary
            where obs_date >= (select max(obs_date) from weather_daily_summary) - interval 7 day
            order by obs_date, city
            """,
        ),
        "econ.json": rows_as_dicts(
            con,
            """
            select indicator_code, indicator_name, year, value
            from stg_econ_indicators
            where year >= 2000
            order by indicator_code, year
            """,
        ),
    }

    counts = {}
    for filename, rows in exports.items():
        (EXPORT_DIR / filename).write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        counts[filename] = len(rows)
        print(f"wrote {filename}: {len(rows)} rows")

    meta = {
        "updated_at": datetime.now(UTC).isoformat(),
        "row_counts": counts,
        "source": "https://github.com/akaD1D/saudi-data-pulse",
    }
    (EXPORT_DIR / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    print(f"wrote meta.json: {meta['updated_at']}")


if __name__ == "__main__":
    main()
