from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt_assets import dbt_project, transform_models
from orchestration.assets.ingestion.econ import econ_indicators
from orchestration.assets.ingestion.tadawul import tadawul_prices
from orchestration.assets.ingestion.weather import weather_hourly
from orchestration.resources import DataLakeResource
from orchestration.schedules import all_schedules
from orchestration.sensors import pipeline_failure_webhook

defs = Definitions(
    assets=[tadawul_prices, weather_hourly, econ_indicators, transform_models],
    schedules=all_schedules,
    sensors=[pipeline_failure_webhook],
    resources={
        "data_lake": DataLakeResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
