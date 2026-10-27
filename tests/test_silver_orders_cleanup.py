"""Optional live checks for Silver outputs."""

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABRICKS_INTEGRATION", "false").lower() not in {"1", "true", "yes"},
    reason="Set RUN_DATABRICKS_INTEGRATION=true to query live Databricks tables.",
)


def test_silver_and_quarantine_tables_exist_and_reconcile():
    from data_quality.config import settings
    from data_quality.spark_session import get_spark

    spark = get_spark()
    assert spark.catalog.tableExists(settings.silver_orders_fqn)
    assert spark.catalog.tableExists(settings.quarantine_orders_fqn)
    silver_count = spark.table(settings.silver_orders_fqn).count()
    quarantine_count = spark.table(settings.quarantine_orders_fqn).count()
    bronze_count = spark.table(settings.bronze_orders_table).count()
    assert silver_count + quarantine_count == bronze_count
