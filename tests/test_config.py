"""Unit tests for environment-driven configuration; no cloud access required."""

import pytest

from data_quality.config import Settings, env_bool, load_settings, quote_identifier


def test_load_settings_uses_environment(monkeypatch):
    monkeypatch.setenv("APP_NAME", "test-platform")
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "test-profile")
    monkeypatch.setenv("DATABRICKS_CATALOG", "test_catalog")
    monkeypatch.setenv("DATABRICKS_BRONZE_SCHEMA", "raw_bronze")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_RAW_PREFIX", "test/raw/")
    monkeypatch.setenv("QUALITY_FAIL_ON_DUPLICATES", "false")

    cfg = load_settings()
    assert cfg.app_name == "test-platform"
    assert cfg.databricks_profile == "test-profile"
    assert cfg.catalog == "test_catalog"
    assert cfg.bronze_schema == "raw_bronze"
    assert cfg.s3_raw_path == "s3://test-bucket/test/raw"
    assert cfg.quality_fail_on_duplicates is False


def test_boolean_values(monkeypatch):
    monkeypatch.setenv("FLAG", "yes")
    assert env_bool("FLAG") is True
    monkeypatch.setenv("FLAG", "0")
    assert env_bool("FLAG") is False


def test_invalid_boolean_fails(monkeypatch):
    monkeypatch.setenv("FLAG", "sometimes")
    with pytest.raises(ValueError, match="must be a boolean"):
        env_bool("FLAG")


def test_quote_identifier_escapes_backticks():
    assert quote_identifier("my`table") == "`my``table`"


def test_empty_identifier_fails():
    with pytest.raises(ValueError):
        quote_identifier("  ")
