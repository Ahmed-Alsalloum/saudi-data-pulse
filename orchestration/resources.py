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

    def storage_options(self) -> dict | None:
        """fsspec options for s3:// roots; None for local paths.

        S3_ENDPOINT_URL points s3fs at MinIO (or any S3-compatible store);
        without it s3fs falls back to real AWS. Credentials come from the
        standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
        """
        if not self.root.startswith("s3://"):
            return None
        endpoint = os.getenv("S3_ENDPOINT_URL")
        return {"client_kwargs": {"endpoint_url": endpoint}} if endpoint else {}

    def write_parquet(self, df: pd.DataFrame, zone: str, dataset: str, partition: str) -> str:
        target = f"{self.root}/{zone}/{dataset}/{partition}/part.parquet"
        options = self.storage_options()
        if options is None:
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        # pandas + pyarrow handle both local paths and s3:// URIs (via s3fs)
        df.to_parquet(target, index=False, storage_options=options or None)
        return target
