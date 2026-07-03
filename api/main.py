"""Public read API over the analytics warehouse.

Opens DuckDB read-only per request — the warehouse file is written by dbt, and
read-only connections let the API and pipeline coexist safely.
"""

import os

import duckdb
from fastapi import FastAPI, HTTPException

WAREHOUSE_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")

app = FastAPI(
    title="Saudi Data Pulse API",
    description="Analytics over Tadawul market data and Saudi open datasets.",
    version="0.1.0",
)


def query(sql: str, params: list | None = None) -> list[dict]:
    if not os.path.exists(WAREHOUSE_PATH):
        raise HTTPException(status_code=503, detail="Warehouse not built yet — run the pipeline")
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    try:
        result = con.execute(sql, params or [])
        columns = [d[0] for d in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    finally:
        con.close()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/v1/market/summary")
def market_summary(limit: int = 50) -> list[dict]:
    """Latest per-sector daily performance from the daily_market_summary mart."""
    return query(
        "select * from daily_market_summary order by trade_date desc, sector limit ?",
        [min(limit, 500)],
    )


@app.get("/api/v1/market/prices/{ticker}")
def ticker_prices(ticker: str, limit: int = 30) -> list[dict]:
    """Recent daily OHLCV for one ticker (e.g. 2222.SR)."""
    rows = query(
        "select * from stg_tadawul_prices where ticker = ? order by trade_date desc limit ?",
        [ticker.upper(), min(limit, 365)],
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for ticker {ticker}")
    return rows
