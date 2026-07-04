import pandas as pd

from orchestration.resources import DataLakeResource

DF = pd.DataFrame({"a": [1, 2]})


def test_local_write_creates_partition_dirs(tmp_path):
    lake = DataLakeResource(root=str(tmp_path))

    target = lake.write_parquet(DF, zone="raw", dataset="tadawul", partition="2026-07-02")

    assert target == f"{tmp_path}/raw/tadawul/2026-07-02/part.parquet"
    assert pd.read_parquet(target).equals(DF)


def test_local_root_has_no_storage_options(tmp_path):
    assert DataLakeResource(root=str(tmp_path)).storage_options() is None


def test_s3_root_targets_custom_endpoint(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    lake = DataLakeResource(root="s3://lake")

    assert lake.storage_options() == {"client_kwargs": {"endpoint_url": "http://minio:9000"}}


def test_s3_root_without_endpoint_uses_s3fs_defaults(monkeypatch):
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    assert DataLakeResource(root="s3://lake").storage_options() == {}


def test_s3_write_passes_options_and_skips_mkdir(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    captured = {}

    def fake_to_parquet(self, path, **kwargs):
        captured["path"] = path
        captured["kwargs"] = kwargs

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    lake = DataLakeResource(root="s3://lake")

    target = lake.write_parquet(DF, zone="raw", dataset="weather", partition="2026-07-04")

    assert target == "s3://lake/raw/weather/2026-07-04/part.parquet"
    assert captured["path"] == target
    assert captured["kwargs"]["storage_options"] == {
        "client_kwargs": {"endpoint_url": "http://minio:9000"}
    }
