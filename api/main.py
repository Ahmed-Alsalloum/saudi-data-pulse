"""Public read API over the analytics warehouse.

Opens DuckDB read-only per request — the warehouse file is written by dbt, and
read-only connections let the API and pipeline coexist safely.

Remote mode (for free hosting, e.g. a Hugging Face Space): set WAREHOUSE_URL
to the raw data-branch URL of warehouse.duckdb and the API downloads a fresh
copy on demand, at most every 15 minutes.
"""

import os
import tempfile
import threading
import time

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

WAREHOUSE_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
REFRESH_SECONDS = int(os.getenv("WAREHOUSE_REFRESH_SECONDS", "900"))

app = FastAPI(
    title="Saudi Data Pulse API",
    description="Analytics over Tadawul market data and Saudi open datasets.",
    version="0.1.0",
)
# The static dashboard (GitHub Pages) is on a different origin.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

_refresh_lock = threading.Lock()
_last_refresh = 0.0


def _ensure_warehouse() -> None:
    """In remote mode, (re)download the warehouse if missing or stale."""
    global _last_refresh
    if not WAREHOUSE_URL:
        return
    with _refresh_lock:
        fresh = os.path.exists(WAREHOUSE_PATH) and time.time() - _last_refresh < REFRESH_SECONDS
        if fresh:
            return
        import requests

        response = requests.get(WAREHOUSE_URL, timeout=60)
        if response.status_code != 200:
            if os.path.exists(WAREHOUSE_PATH):
                return  # keep serving the previous copy
            raise HTTPException(status_code=503, detail="Warehouse download failed")
        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(WAREHOUSE_PATH) or ".")
        with os.fdopen(fd, "wb") as f:
            f.write(response.content)
        os.replace(tmp_path, WAREHOUSE_PATH)
        _last_refresh = time.time()


def query(sql: str, params: list | None = None) -> list[dict]:
    _ensure_warehouse()
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


@app.get("/api/v1/weather/{city}")
def city_weather(city: str, hours: int = 24) -> list[dict]:
    """Recent hourly observations for one city (riyadh, jeddah, dammam, makkah, madinah, abha)."""
    rows = query(
        "select * from stg_weather where city = ? order by observed_at desc limit ?",
        [city.lower(), min(hours, 168)],
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No weather data for city {city}")
    return rows


@app.get("/api/v1/econ")
def econ_indicators() -> list[dict]:
    """Latest value per Saudi macro indicator (CPI, GDP growth, population, unemployment)."""
    return query(
        """
        select indicator_code, indicator_name, year, value
        from stg_econ_indicators
        qualify row_number() over (partition by indicator_code order by year desc) = 1
        order by indicator_code
        """
    )
