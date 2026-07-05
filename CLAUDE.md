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

# Bulk-backfill Tadawul history — ALWAYS via CI dispatch, never a local push to the data branch
gh workflow run pipeline.yml -f target=all -f backfill_range="2026-01-01:2026-07-02"

# After changing dbt models/tests: regenerate the hosted dbt docs and keep ARCHITECTURE.md's
# test/model counts in sync (both are checked in)
cd transform && dbt docs generate --profiles-dir . --static && cd .. \
  && cp transform/target/static_index.html docs/dbt/index.html
```

**Windows caveat:** never use `--select "*"` — Dagster's CLI (Click) glob-expands `*` into filenames on Windows. Always use explicit asset selections as above.

## Architecture

One set of Dagster asset definitions runs in **two runtimes**:

- **Local**: Docker Compose (or `dagster dev`) with the daemon running four schedules and a run-failure webhook sensor ([orchestration/schedules.py](orchestration/schedules.py), [orchestration/sensors.py](orchestration/sensors.py)).
- **Cloud ($0)**: [.github/workflows/pipeline.yml](.github/workflows/pipeline.yml) invokes the identical assets via `dagster asset materialize` on cron, then pushes the lake, warehouse, and JSON exports (from [scripts/export_json.py](scripts/export_json.py)) to the **`data` branch** — which is pipeline-owned; never edit or push it manually (backfills go through the `backfill_range`/`backfill_partition` dispatch inputs, which run [scripts/backfill_tadawul.py](scripts/backfill_tadawul.py) inside CI). The GitHub Pages dashboard ([docs/index.html](docs/index.html)) reads the exports from raw.githubusercontent URLs; [deploy-api.yml](.github/workflows/deploy-api.yml) pushes a generated Space repo (Dockerfile with `WAREHOUSE_URL` baked in) to the Hugging Face Space on changes to `api/**` — the `HF_TOKEN` secret and `HF_SPACE` variable are configured and the Space is live.

**Live URLs / naming trap**: the GitHub account is `Ahmed-Alsalloum` (renamed from `akaD1D` on 2026-07-05 — Pages URLs did NOT redirect, hence `https://ahmed-alsalloum.github.io/saudi-data-pulse/`), but the Hugging Face account is still `akaD1D`, so the API URL `https://akad1d-saudi-data-pulse.hf.space` is correct — do not "fix" the apparent mismatch. Hosted dbt docs are a committed static file at [docs/dbt/index.html](docs/dbt/index.html) (from `dbt docs generate --static`), served by Pages at `/dbt/`. The README is deliberately minimal (live URLs, stack, run commands — owner's preference); depth belongs in [ARCHITECTURE.md](ARCHITECTURE.md), which also holds measured performance numbers.

Data flow: ingestion assets write date-partitioned Parquet to the lake (`DATA_LAKE_PATH`, default `data/lake`) → dbt-duckdb reads the Parquet globs → tables in the DuckDB warehouse (`DUCKDB_PATH`, default `data/warehouse.duckdb`) → FastAPI ([api/main.py](api/main.py)), Metabase, and the JSON exports. Only env vars change between laptop, Docker, and CI — on laptop and CI the lake is a local folder; in Docker it is MinIO object storage (see the S3 wiring rule below).

### Wiring rules (cross-file, easy to break)

- **Dagster↔dbt lineage** hangs on a naming contract: an ingestion asset's key `["lake", "<table>"]` must match a dbt source table `lake.<table>` in [transform/models/sources.yml](transform/models/sources.yml), whose `meta.external_location` glob must match the `dataset=` the asset passes to `DataLakeResource.write_parquet`. Adding a source means touching all three plus a staging model.
- **The dashboard is bilingual and its dictionaries mirror ingestion catalogs**: [docs/index.html](docs/index.html) translates data values via `SECTOR_AR`, `COMPANY_AR`, and `CITY_NAMES` maps. Adding a ticker to `TADAWUL_TICKERS` (tadawul.py) or a city to `CITIES` (weather.py) without updating those maps silently falls back to English in the Arabic view. Language selection: `?lang=ar` param > localStorage > `en`; percent cells carry `dir="ltr"` so signs don't flip in RTL.
- **The warehouse file must stay self-contained**: every dbt model is materialized as a `table` (see [transform/dbt_project.yml](transform/dbt_project.yml)). Views would bake build-time-relative Parquet paths into the file and break the API/Metabase, which open it read-only from other working directories.
- **DuckDB allows one writer XOR many readers**: Metabase's driver keeps a pooled read-only connection that blocks dbt's write lock — restart the metabase container before manual in-container dbt builds. The API opens per-request, so it doesn't hold locks. Local Dagster schedule state lives in the container layer (`DAGSTER_HOME`), so re-enable schedules after an image rebuild (`dagster schedule start --start-all -m orchestration.definitions` inside the container).
- **`orchestration/definitions.py` is deliberately not the package `__init__`**: importing it validates the dbt manifest, so unit tests import asset modules directly and stay manifest-free. Anything that loads definitions needs `dbt parse` run first (the Dockerfile and both workflows do this).
- **Tadawul is daily-partitioned** (`Asia/Riyadh`, completed days only): schedules and the nightly cron materialize *yesterday's* partition; non-trading days (Fri/Sat, holidays) intentionally return `rows: 0` instead of failing. Ingestion assets carry `RetryPolicy` for transient network errors.
- **Weather has a dual-provider fallback**: Open-Meteo (rich 48h window) throttles shared datacenter IPs, so on `RequestException` the asset switches to MET Norway (current hours only, requires the identifying User-Agent, UTC→Riyadh conversion, m/s→km/h). Both land in the same lake schema; staging dedupes on (city, observed_at).
- **The S3 lake mode needs two clients configured in lockstep**: with `DATA_LAKE_PATH=s3://…`, ingestion writes via s3fs (`S3_ENDPOINT_URL` + standard AWS creds) while dbt/DuckDB reads via httpfs (`DBT_TARGET=s3` selects the target in [transform/profiles.yml](transform/profiles.yml), whose `DUCKDB_S3_ENDPOINT` is host:port with no scheme). docker-compose sets all of these for MinIO; native dev and CI leave them unset and use local folders. s3fs is an optional extra (`pip install -e ".[s3]"`; the Docker image installs it).
- **dbt-expectations recency tests require the `dbt_date:time_zone` var** (set in dbt_project.yml). Every staging model also carries a row-count floor (`expect_table_row_count_to_be_between`, min 1) because recency/not-null gates pass vacuously on empty tables — don't remove the floors, they're the only thing that catches a silent ingestion outage. The tadawul mart (`daily_market_summary`) needs ≥2 trading days in the lake or `lag()` yields an empty (but test-passing) table.
- **Metabase must build from [metabase.Dockerfile](metabase.Dockerfile)** (Metabase jar on a Debian JRE): the DuckDB driver's native library segfaults on the official Alpine image. The driver jar lives in `metabase-plugins/` (gitignored except `.gitkeep`).

## Data sources

yfinance with `.SR` ticker suffixes (Tadawul), Open-Meteo + met.no (weather), World Bank API (Saudi macro). All keyless. The Saudi Open Data portal (open.data.gov.sa) rejects programmatic clients at its WAF — don't reattempt it without a browser-impersonation strategy.
