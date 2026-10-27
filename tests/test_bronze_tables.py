"""Optional live checks for expected Bronze tables."""

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABRICKS_INTEGRATION", "false").lower() not in {"1", "true", "yes"},
    reason="Set RUN_DATABRICKS_INTEGRATION=true to query live Databricks tables.",
)


def test_bronze_orders_table_exists_and_has_rows():
    from data_quality.config import settings
    from data_quality.spark_session import get_spark

    spark = get_spark()
    table = settings.bronze_orders_table
    assert spark.catalog.tableExists(table), f"Expected Bronze table not found: {table}"
    assert spark.table(table).count() > 0
