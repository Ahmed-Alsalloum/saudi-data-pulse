from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestration.assets.dbt_assets import dbt_project, transform_models
from orchestration.assets.ingestion.tadawul import tadawul_prices
from orchestration.resources import DataLakeResource
from orchestration.schedules import daily_pipeline_schedule

defs = Definitions(
    assets=[tadawul_prices, transform_models],
    schedules=[daily_pipeline_schedule],
    resources={
        "data_lake": DataLakeResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
