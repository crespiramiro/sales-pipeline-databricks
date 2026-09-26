import sys
import io
import pandas as pd
import pytest
from unittest.mock import patch
from pathlib import Path

# Add project root and ingestion to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from etl.load import _parquet_to_bytes, cargar_a_bronze, VOLUME_BRONZE_PATH


def test_parquet_to_bytes():
    df = pd.DataFrame({"id": [1, 2], "name": ["item1", "item2"]})
    parquet_bytes = _parquet_to_bytes(df)

    assert isinstance(parquet_bytes, bytes)
    assert len(parquet_bytes) > 0

    # Read back parquet bytes into DataFrame to verify content integrity
    df_read = pd.read_parquet(io.BytesIO(parquet_bytes))
    assert len(df_read) == 2
    assert list(df_read.columns) == ["id", "name"]


def test_cargar_a_bronze_missing_env(monkeypatch):
    monkeypatch.setattr("etl.load.DATABRICKS_HOST", None)
    monkeypatch.setattr("etl.load.DATABRICKS_TOKEN", None)

    with pytest.raises(ValueError, match="DATABRICKS_HOST y DATABRICKS_TOKEN"):
        cargar_a_bronze(pd.DataFrame())


@patch("etl.load._volume_upload")
def test_cargar_a_bronze_success(mock_upload, monkeypatch):
    monkeypatch.setattr("etl.load.DATABRICKS_HOST", "https://test.cloud.databricks.com")
    monkeypatch.setattr("etl.load.DATABRICKS_TOKEN", "dapitest123")

    df = pd.DataFrame({"sale_id": [101, 102]})
    result_path = cargar_a_bronze(df)

    assert result_path.startswith(VOLUME_BRONZE_PATH)
    assert result_path.endswith(".parquet")
    assert mock_upload.called
