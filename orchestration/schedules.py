from dagster import AssetSelection, ScheduleDefinition, define_asset_job

# Tadawul trades Sunday-Thursday and closes at 15:00 Asia/Riyadh; run after
# close so the day's candle is final.
daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection=AssetSelection.all(),
)

daily_pipeline_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="30 15 * * 0-4",
    execution_timezone="Asia/Riyadh",
)
