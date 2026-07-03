from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from orchestration.assets.dbt_assets import transform_models
from orchestration.assets.ingestion.tadawul import daily_partitions

# Tadawul trades Sunday-Thursday and closes at 15:00 Asia/Riyadh; the
# partitioned schedule fires daily after close (non-trading partitions land
# empty and are logged, which keeps backfills uniform).
tadawul_job = define_asset_job(
    name="tadawul_ingestion",
    selection=AssetSelection.assets(["lake", "tadawul_prices"]),
    partitions_def=daily_partitions,
)
tadawul_schedule = build_schedule_from_partitioned_job(
    tadawul_job, hour_of_day=15, minute_of_hour=30
)

weather_job = define_asset_job(
    name="weather_ingestion",
    selection=AssetSelection.assets(["lake", "weather_hourly"]),
)
weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="5 * * * *",
    execution_timezone="Asia/Riyadh",
)

econ_job = define_asset_job(
    name="econ_ingestion",
    selection=AssetSelection.assets(["lake", "econ_indicators"]),
)
econ_schedule = ScheduleDefinition(
    job=econ_job,
    cron_schedule="0 12 * * 0",  # weekly Sunday noon; annual data changes rarely
    execution_timezone="Asia/Riyadh",
)

# dbt staging + marts + quality gates, after the market ingest lands.
transform_job = define_asset_job(
    name="transform_and_serve",
    selection=AssetSelection.assets(transform_models),
)
transform_schedule = ScheduleDefinition(
    job=transform_job,
    cron_schedule="0 16 * * *",
    execution_timezone="Asia/Riyadh",
)

all_schedules = [
    tadawul_schedule,
    weather_schedule,
    econ_schedule,
    transform_schedule,
]
