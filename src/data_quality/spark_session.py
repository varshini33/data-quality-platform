"""Databricks Spark session factory."""

from databricks.connect import DatabricksSession

from data_quality.config import settings


def get_spark():
    """Create a Spark session using the configured Databricks CLI profile."""
    builder = DatabricksSession.builder.serverless()
    if settings.databricks_profile:
        builder = builder.profile(settings.databricks_profile)
    return builder.getOrCreate()
