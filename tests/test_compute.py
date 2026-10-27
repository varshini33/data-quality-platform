"""Optional live Databricks connectivity smoke test.

Run explicitly with RUN_DATABRICKS_INTEGRATION=true after configuring .env and CLI auth.
"""

import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABRICKS_INTEGRATION", "false").lower() not in {"1", "true", "yes"},
    reason="Set RUN_DATABRICKS_INTEGRATION=true to run against live Databricks compute.",
)


def test_serverless_compute():
    from data_quality.spark_session import get_spark

    spark = get_spark()
    assert spark.range(10).count() == 10
