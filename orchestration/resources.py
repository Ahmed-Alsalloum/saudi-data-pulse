"""Shared Dagster resources.

The data lake is addressed by a single env-driven root so the same code runs
against a local folder (native dev on Windows), MinIO (Docker Compose), or S3
(production VM) — only DATA_LAKE_PATH changes.
"""

import os
from pathlib import Path

import pandas as pd
from dagster import ConfigurableResource

DEFAULT_LAKE_PATH = "data/lake"
DEFAULT_WAREHOUSE_PATH = "data/warehouse.duckdb"


class DataLakeResource(ConfigurableResource):
    """Writes Parquet files into `<root>/<zone>/<dataset>/<partition>/part.parquet`."""

    root: str = os.getenv("DATA_LAKE_PATH", DEFAULT_LAKE_PATH)

    def write_parquet(self, df: pd.DataFrame, zone: str, dataset: str, partition: str) -> str:
        target = f"{self.root}/{zone}/{dataset}/{partition}/part.parquet"
        if not target.startswith("s3://"):
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        # pandas + pyarrow handle both local paths and s3:// URIs (via s3fs)
        df.to_parquet(target, index=False)
        return target
