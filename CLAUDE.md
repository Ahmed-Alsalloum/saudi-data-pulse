# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pip install -e ".[dev]"                          # once per venv
cd transform && dbt deps --profiles-dir . && dbt parse --profiles-dir . && cd ..
                                                 # required before anything imports
                                                 # orchestration.definitions (manifest must exist)

ruff check .                                     # lint (line length 100)
pytest                                           # unit tests (network-free, fixture-based)
pytest tests/test_weather.py -k tidy             # single test

# Run the pipeline locally (any partition since 2026-01-01 is backfillable)
dagster asset materialize --select "lake/tadawul_prices" --partition 2026-07-02 -m orchestration.definitions
dagster asset materialize --select "lake/weather_hourly,lake/econ_indicators,stg_tadawul_prices+,stg_weather+,stg_econ_indicators+" -m orchestration.definitions

dagster dev                                      # Dagster UI on :3000
cd transform && dbt build --profiles-dir .       # models + data tests directly
docker compose up --build -d                     # full local stack (Dagster :3000, API :8000, Metabase :3001, MinIO :9001)
```

**Windows caveat:** never use `--select "*"` — Dagster's CLI (Click) glob-expands `*` into filenames on Windows. Always use explicit asset selections as above.

## Architecture

One set of Dagster asset definitions runs in **two runtimes**:

- **Local**: Docker Compose (or `dagster dev`) with the daemon running four schedules and a run-failure webhook sensor ([orchestration/schedules.py](orchestration/schedules.py), [orchestration/sensors.py](orchestration/sensors.py)).
- **Cloud ($0)**: [.github/workflows/pipeline.yml](.github/workflows/pipeline.yml) invokes the identical assets via `dagster asset materialize` on cron, then pushes the lake, warehouse, and JSON exports (from [scripts/export_json.py](scripts/export_json.py)) to the **`data` branch** — which is pipeline-owned; never edit it manually. The GitHub Pages dashboard ([docs/index.html](docs/index.html)) reads the exports from raw.githubusercontent URLs; [deploy-api.yml](.github/workflows/deploy-api.yml) pushes `api/` to a Hugging Face Space (skips unless the `HF_TOKEN` secret and `HF_SPACE` variable are set).

Data flow: ingestion assets write date-partitioned Parquet to the lake (`DATA_LAKE_PATH`, default `data/lake`) → dbt-duckdb reads the Parquet globs → tables in the DuckDB warehouse (`DUCKDB_PATH`, default `data/warehouse.duckdb`) → FastAPI ([api/main.py](api/main.py)), Metabase, and the JSON exports. Those two env vars are the only thing that changes between laptop, Docker, and CI.

### Wiring rules (cross-file, easy to break)

- **Dagster↔dbt lineage** hangs on a naming contract: an ingestion asset's key `["lake", "<table>"]` must match a dbt source table `lake.<table>` in [transform/models/sources.yml](transform/models/sources.yml), whose `meta.external_location` glob must match the `dataset=` the asset passes to `DataLakeResource.write_parquet`. Adding a source means touching all three plus a staging model.
- **The warehouse file must stay self-contained**: every dbt model is materialized as a `table` (see [transform/dbt_project.yml](transform/dbt_project.yml)). Views would bake build-time-relative Parquet paths into the file and break the API/Metabase, which open it read-only from other working directories.
- **`orchestration/definitions.py` is deliberately not the package `__init__`**: importing it validates the dbt manifest, so unit tests import asset modules directly and stay manifest-free. Anything that loads definitions needs `dbt parse` run first (the Dockerfile and both workflows do this).
- **Tadawul is daily-partitioned** (`Asia/Riyadh`, completed days only): schedules and the nightly cron materialize *yesterday's* partition; non-trading days (Fri/Sat, holidays) intentionally return `rows: 0` instead of failing. Ingestion assets carry `RetryPolicy` for transient network errors.
- **Weather has a dual-provider fallback**: Open-Meteo (rich 48h window) throttles shared datacenter IPs, so on `RequestException` the asset switches to MET Norway (current hours only, requires the identifying User-Agent, UTC→Riyadh conversion, m/s→km/h). Both land in the same lake schema; staging dedupes on (city, observed_at).
- **dbt-expectations recency tests require the `dbt_date:time_zone` var** (set in dbt_project.yml). Known gap: freshness gates pass vacuously when a staging table is completely empty.
- **Metabase must build from [metabase.Dockerfile](metabase.Dockerfile)** (Metabase jar on a Debian JRE): the DuckDB driver's native library segfaults on the official Alpine image. The driver jar lives in `metabase-plugins/` (gitignored except `.gitkeep`).

## Data sources

yfinance with `.SR` ticker suffixes (Tadawul), Open-Meteo + met.no (weather), World Bank API (Saudi macro). All keyless. The Saudi Open Data portal (open.data.gov.sa) rejects programmatic clients at its WAF — don't reattempt it without a browser-impersonation strategy.
