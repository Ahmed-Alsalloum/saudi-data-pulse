# Architecture

Saudi Data Pulse is a small but complete lakehouse: three public data sources,
asset-based orchestration, tested SQL transformations, and three serving
surfaces (dashboard, API, BI), running in two runtimes from one codebase at a
total infrastructure cost of $0.

This document explains the design decisions and gives real operational
numbers. The [README](README.md) covers setup; the
[hosted dbt docs](https://akad1d.github.io/saudi-data-pulse/dbt/) cover every
model, column, and test.

## The two-runtime design

The core constraint: a public, always-fresh deployment with **no cloud spend
and no credit card**. Free VMs (Oracle, GCP trials) require card verification,
so instead of one always-on server the platform splits into:

```
                ┌── local (Docker Compose) ──────────────────────────┐
                │  Dagster daemon: 4 schedules + failure sensor       │
                │  lake = MinIO (s3://lake, real object storage)      │
                │  Metabase + FastAPI read the DuckDB warehouse       │
                └─────────────────────────────────────────────────────┘
one set of
Dagster assets ─┤
                ┌── cloud, $0 (GitHub Actions) ───────────────────────┐
                │  cron invokes `dagster asset materialize`           │
                │    hourly weather · nightly Tadawul · weekly econ   │
                │  lake + warehouse + JSON exports → `data` branch    │
                │  GitHub Pages dashboard reads exports (CORS *)      │
                │  Hugging Face Space serves the FastAPI              │
                └─────────────────────────────────────────────────────┘
```

The asset definitions are identical in both; only environment variables
change (`DATA_LAKE_PATH`, `DUCKDB_PATH`, and the S3 settings below). GitHub
Actions is the "scheduler" in the cloud: each cron tick materializes the same
assets the Dagster daemon would, then commits the lake, the rebuilt DuckDB
warehouse, and small JSON exports to a pipeline-owned `data` branch.
`raw.githubusercontent.com` serves those exports with `CORS *`, which makes a
zero-backend live dashboard possible on GitHub Pages.

Accepted trade-offs: no always-on Dagster UI or Metabase in the cloud, data
cadence bounded by cron (~hourly), and the free API may cold-start after
idle. For a portfolio-scale dataset those are the right trades against ~$5+/mo
for a VM.

## Data flow

1. **Ingestion (Dagster assets)** — yfinance for Tadawul OHLCV (`.SR`
   tickers), Open-Meteo for hourly weather in 6 cities, World Bank API for
   Saudi macro indicators. Each asset writes date-partitioned Parquet to the
   raw zone of the lake and carries a `RetryPolicy` for transient network
   failures.
   - Tadawul is daily-partitioned in `Asia/Riyadh` over *completed* trading
     days; Fri/Sat and holidays legitimately produce `rows: 0` instead of a
     red run. Any partition since 2026-01-01 is backfillable from the CLI.
   - Weather has a dual-provider fallback: Open-Meteo throttles shared
     datacenter IPs (like Actions runners), so on failure the asset switches
     to MET Norway and normalizes units/timezones into the same schema.
     Staging dedupes on (city, observed_at), so overlapping windows are safe.
2. **Lake** — plain Parquet under `<root>/raw/<dataset>/<partition>/`. The
   root is one env var: a local folder on a laptop and in CI, MinIO object
   storage in Docker (`s3://lake` — written via s3fs, read back by DuckDB's
   httpfs extension). Pointing at real AWS S3 is a config change, not a code
   change.
3. **Transform (dbt on DuckDB)** — staging models read the Parquet globs as
   external sources; marts aggregate them into serving tables. 23 data tests
   (dbt-generic + dbt-expectations, including recency gates) run in the same
   `dbt build` as the models, so **bad data blocks the marts** instead of
   landing in them. Every model is materialized as a `table`: the warehouse
   file must stay self-contained because the API and Metabase open it
   read-only from other working directories, and views would bake in
   build-time-relative Parquet paths.
4. **Serving** — FastAPI opens the warehouse read-only per request; Metabase
   connects via the community DuckDB driver; the static dashboard renders
   pre-computed JSON exports.

## Operational numbers (measured 2026-07-04)

| Metric | Value |
| --- | --- |
| Scheduled pipeline, end to end | avg 94s over 6 runs (min 74s, max 170s) |
| — pip install (cached) | 43s |
| — Tadawul ingestion (one session, 14 tickers) | 11s |
| — dbt build: 5 models + 23 tests | 19s |
| — JSON export + data-branch push | ~2s |
| API latency, `/api/v1/market/summary` (warm, local Docker) | p50 19 ms · p95 21 ms |
| DuckDB warehouse file | 1.8 MB |
| Lake (raw Parquet) | ~5–7 KB per partition |
| Dashboard payload | 4 JSON files, a few KB each, no backend |
| Infrastructure cost | $0/month (Actions + Pages free for public repos, HF free CPU tier) |

The absolute numbers are small — this is a portfolio dataset — but the shape
is what matters: the pipeline spends half its wall time on `pip install`,
which is the classic serverless-cron overhead, and the query path is
milliseconds because the marts are pre-aggregated tables in a columnar file.

## Failure handling

- GitHub emails on any failed scheduled run; an optional `ALERT_WEBHOOK_URL`
  secret posts failures to a webhook from CI, mirroring the local Dagster
  run-failure sensor.
- Ingestion retries transient network errors (`RetryPolicy`); the weather
  asset degrades to a second provider before failing.
- Data-quality failures are *upstream* of serving: a failing dbt test stops
  the mart rebuild, so the API and dashboard keep serving the last good data.

## Decisions worth defending in review

- **Dagster over Airflow**: the unit of orchestration here is a *dataset*,
  not a task. Software-defined assets give lineage, partitions, and backfills
  natively; the concepts map 1:1 to Airflow DAGs when needed.
- **DuckDB over a client-server warehouse**: single-file, zero-ops, in-process
  columnar SQL. The whole warehouse ships to the API as an artifact. The
  trade-off — one writer at a time — is real: a pooled read-only connection
  (e.g. Metabase's) blocks dbt's write lock, which is why serving containers
  open read-only and the local stack may need Metabase restarted before a
  manual rebuild.
- **A git branch as cloud storage**: the `data` branch is rewritten by CI and
  never edited by hand. It is versioned, free, served over CDN with CORS —
  and would be the first thing replaced (by S3) if the data volume grew.
- **Tables, not views** (see Data flow §3) and **the lake/dbt naming
  contract**: an ingestion asset key `lake/<table>` must match a dbt source
  `lake.<table>` whose `external_location` glob matches the written path —
  Dagster↔dbt lineage hangs on that contract.

## Known gaps and next steps

- dbt-expectations recency gates pass vacuously when a staging table is
  completely empty (confirmed in practice: an empty mart passed all its
  not-null tests). A row-count floor test would close this.
- The GitHub-hosted lake keeps whole-file history; Parquet partitions are
  append-only, so the branch grows unbounded — fine for years at current
  volume, but S3 + lifecycle rules is the real answer.
- Local schedule state lives in the container layer (`DAGSTER_HOME`), so
  schedules need re-enabling after an image rebuild; a mounted volume would
  persist it.
