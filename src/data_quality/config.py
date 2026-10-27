"""Centralized environment-driven configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Keep already-exported environment variables higher priority than .env values.
load_dotenv(PROJECT_ROOT / ".env", override=False)


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable with clear validation."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value (true/false, yes/no, 1/0).")


def required_env(name: str) -> str:
    """Read a required environment variable."""
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}. Check your local .env configuration.")
    return value


def quote_identifier(identifier: str) -> str:
    """Safely quote a single Spark SQL identifier."""
    value = identifier.strip()
    if not value or "\x00" in value:
        raise ValueError("SQL identifier must be non-empty and cannot contain NUL.")
    return "`" + value.replace("`", "``") + "`"


@dataclass(frozen=True)
class Settings:
    app_name: str = "data-quality-platform"
    environment: str = "development"
    log_level: str = "INFO"
    databricks_profile: str = ""
    catalog: str = "olist_data_quality"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    aws_region: str = ""
    s3_bucket: str = "olist-data-quality-s3"
    s3_raw_prefix: str = "raw/olist/initial/"
    s3_manifest_prefix: str = "metadata/ingestion-manifests/"
    quality_fail_on_duplicates: bool = True
    quality_fail_on_null_keys: bool = True
    orders_source_table: str = "olist_orders_dataset"
    silver_orders_table: str = "orders"
    quarantine_orders_table: str = "orders_quarantine"

    @property
    def s3_raw_path(self) -> str:
        prefix = self.s3_raw_prefix.strip("/")
        return f"s3://{self.s3_bucket}/{prefix}" if prefix else f"s3://{self.s3_bucket}"

    @property
    def bronze_orders_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.orders_source_table}"

    @property
    def silver_orders_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_orders_table}"

    @property
    def quarantine_orders_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.quarantine_orders_table}"


def load_settings() -> Settings:
    """Build settings from environment variables; safe to call in tests."""
    return Settings(
        app_name=os.getenv("APP_NAME", "data-quality-platform").strip() or "data-quality-platform",
        environment=os.getenv("APP_ENV", "development").strip() or "development",
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        databricks_profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "").strip(),
        catalog=os.getenv("DATABRICKS_CATALOG", "olist_data_quality").strip(),
        bronze_schema=os.getenv("DATABRICKS_BRONZE_SCHEMA", "bronze").strip(),
        silver_schema=os.getenv("DATABRICKS_SILVER_SCHEMA", "silver").strip(),
        gold_schema=os.getenv("DATABRICKS_GOLD_SCHEMA", "gold").strip(),
        aws_region=os.getenv("AWS_REGION", "").strip(),
        s3_bucket=os.getenv("S3_BUCKET", "olist-data-quality-s3").strip(),
        s3_raw_prefix=os.getenv("S3_RAW_PREFIX", "raw/olist/initial/").strip(),
        s3_manifest_prefix=os.getenv("S3_MANIFEST_PREFIX", "metadata/ingestion-manifests/").strip(),
        quality_fail_on_duplicates=env_bool("QUALITY_FAIL_ON_DUPLICATES", True),
        quality_fail_on_null_keys=env_bool("QUALITY_FAIL_ON_NULL_KEYS", True),
        orders_source_table=os.getenv("BRONZE_ORDERS_TABLE", "olist_orders_dataset").strip(),
        silver_orders_table=os.getenv("SILVER_ORDERS_TABLE", "orders").strip(),
        quarantine_orders_table=os.getenv("QUARANTINE_ORDERS_TABLE", "orders_quarantine").strip(),
    )


settings = load_settings()
