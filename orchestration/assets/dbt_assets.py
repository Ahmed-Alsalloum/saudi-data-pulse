"""Expose the dbt project's models as Dagster assets.

The dbt source `lake.tadawul_prices` maps to the ingestion asset's key
["lake", "tadawul_prices"], so Dagster wires ingestion -> staging -> marts
into one lineage graph automatically.
"""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "transform").resolve()

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def transform_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` runs models and their tests together, so a failing data-quality
    # test blocks every downstream mart in the same run.
    yield from dbt.cli(["build"], context=context).stream()
