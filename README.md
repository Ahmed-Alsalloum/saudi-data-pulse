# Saudi Data Pulse

A real-time analytics platform for Saudi open data. It continuously ingests
**Tadawul stock market data** (with GASTAT statistics and city weather coming
next), runs it through a lakehouse-style pipeline, and serves live dashboards
and a public JSON API.

Built to demonstrate production data-engineering practices end to end:
asset-based orchestration, ELT with tested transformations, data-quality
gates that block bad data, and infrastructure as code.

## Architecture

```
yfinance (.SR tickers)          GASTAT open data*        OpenWeatherMap*
        │                              │                        │
        └──────────────┬───────────────┴────────────────────────┘
                       ▼
              Dagster ingestion assets          * Phase 2
                       │
                       ▼
        Data lake — raw zone (Parquet, partitioned by date)
          local folder / MinIO (Docker) / S3 (prod)
                       │
                       ▼
              dbt on DuckDB  ──  staging views + data-quality tests
                       │            (failing tests block the marts)
                       ▼
              analytics marts (daily_market_summary, …)
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
     Metabase dashboards    FastAPI read API
```

Orchestration is **Dagster** (asset-based; the concepts map 1:1 to Airflow
DAGs). Transformations are **dbt** with the DuckDB adapter. Everything is
addressed through two env vars (`DATA_LAKE_PATH`, `DUCKDB_PATH`) so the same
code runs on a laptop, in Docker Compose, or on a cloud VM.

## Quickstart (native, no Docker)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1          # source .venv/bin/activate on Linux/macOS
pip install -e ".[dev]"

# Run the full pipeline once: ingest Tadawul prices -> build marts + tests
# (explicit selection because "*" gets glob-expanded by the CLI on Windows)
dagster asset materialize --select "lake/tadawul_prices+,stg_tadawul_prices+" -m orchestration.definitions

# Explore the asset graph / schedules in the Dagster UI
dagster dev                          # http://localhost:3000

# Serve the API
uvicorn api.main:app                 # http://localhost:8000/docs
```

## Quickstart (Docker)

```bash
docker compose up --build
```

- Dagster UI → http://localhost:3000 (materialize all assets from the UI)
- API → http://localhost:8000/docs
- Metabase → http://localhost:3001 (add the [DuckDB community driver](https://github.com/motherduckdb/metabase_duckdb_driver) jar to `./metabase-plugins/` first, then connect it to `/data/warehouse.duckdb`)

## API

| Endpoint | Description |
| --- | --- |
| `GET /api/v1/market/summary` | Per-sector daily performance (volume, avg move, top gainer/loser) |
| `GET /api/v1/market/prices/{ticker}` | Recent daily OHLCV for one ticker, e.g. `2222.SR` |

## Project layout

```
orchestration/   Dagster assets, resources, schedules
transform/       dbt project (staging + marts + data-quality tests)
api/             FastAPI read API over the DuckDB warehouse
infra/           Terraform (Phase 3)
```

## Roadmap

- [x] Phase 1 — Tadawul ingestion → dbt marts → dashboard/API, daily schedule
- [ ] Phase 2 — GASTAT + weather sources, incremental partitions, dbt-expectations quality gates with alerting, MinIO/S3 lake
- [ ] Phase 3 — Terraform deploy to a VM, HTTPS, uptime monitoring, live public URL
- [ ] Phase 4 — hosted dbt docs, architecture write-up, cost/latency numbers

## Development

```bash
ruff check .          # lint
pytest                # unit tests
cd transform && dbt build --profiles-dir .   # models + data tests
```

CI (GitHub Actions) runs lint, unit tests, and a dbt compile check on every push.
