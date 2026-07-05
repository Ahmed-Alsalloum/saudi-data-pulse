# Saudi Data Pulse

[![CI](https://github.com/Ahmed-Alsalloum/saudi-data-pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/Ahmed-Alsalloum/saudi-data-pulse/actions/workflows/ci.yml)

A real-time analytics platform for Saudi open data. It continuously ingests
**Tadawul stock market data**, **hourly weather for 6 Saudi cities**, and
**Saudi macroeconomic indicators**, runs them through a lakehouse-style
pipeline, and serves live dashboards and a public JSON API.

## 🔴 Live

- **Dashboard** (EN/عربي): https://ahmed-alsalloum.github.io/saudi-data-pulse/
- **API**: https://akad1d-saudi-data-pulse.hf.space/docs
- **dbt docs**: https://ahmed-alsalloum.github.io/saudi-data-pulse/dbt/

[![Live dashboard](docs/assets/dashboard.png)](https://ahmed-alsalloum.github.io/saudi-data-pulse/)

## Built with

**Dagster** (asset-based orchestration) → **Parquet** lake (local folder /
MinIO / S3) → **dbt on DuckDB** with data-quality gates that block bad data →
**FastAPI**, **Metabase**, and a static **Chart.js** dashboard. One set of
asset definitions runs in two runtimes — Docker Compose locally, GitHub
Actions cron in the cloud at $0 — switched by env vars alone. Design
decisions, trade-offs, and measured performance numbers live in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Run it

**Docker:**

```bash
docker compose up --build
```

Dagster UI → http://localhost:3000 · API → http://localhost:8000/docs ·
Metabase → http://localhost:3001 · MinIO console → http://localhost:9001

**Native (no Docker):**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1          # source .venv/bin/activate on Linux/macOS
pip install -e ".[dev]"
cd transform && dbt deps --profiles-dir . && cd ..

# Ingest one trading day, then build marts + quality gates
dagster asset materialize --select "lake/tadawul_prices" --partition 2026-07-02 -m orchestration.definitions
dagster asset materialize --select "lake/weather_hourly,lake/econ_indicators,stg_tadawul_prices+,stg_weather+,stg_econ_indicators+" -m orchestration.definitions

dagster dev                          # Dagster UI on http://localhost:3000
uvicorn api.main:app                 # API on http://localhost:8000/docs
```

**Develop:**

```bash
ruff check . && pytest                        # lint + unit tests
cd transform && dbt build --profiles-dir .    # models + data tests
```
