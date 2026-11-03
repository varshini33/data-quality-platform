
"""Run the Olist pipeline and persist stage-level execution logs."""

import subprocess
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_quality.config import quote_identifier, settings
from data_quality.spark_session import get_spark


PIPELINE_STAGES = [
    ("Bronze ingestion", "data_quality.bronze.bronze_ingestion"),
    ("Silver transformation", "data_quality.silver.orders_cleanup"),
    ("Data quality checks", "data_quality.quality.run_quality_checks"),
    ("Gold business metrics", "data_quality.gold.business_metrics"),
]

LOG_TABLE = "pipeline_run_log"

LOG_SCHEMA = StructType(
    [
        StructField("pipeline_run_id", StringType(), False),
        StructField("stage_name", StringType(), False),
        StructField("start_time_utc", TimestampType(), False),
        StructField("end_time_utc", TimestampType(), False),
        StructField("duration_seconds", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)


def qualified_table(schema_name: str, table_name: str) -> str:
    """Return a safely quoted catalog.schema.table name."""
    return ".".join(
        [
            quote_identifier(settings.catalog),
            quote_identifier(schema_name),
            quote_identifier(table_name),
        ]
    )


def utc_now_naive() -> datetime:
    """Return UTC datetime without timezone info for Spark TimestampType."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_log_table(spark, log_table_name: str) -> None:
    """Create the Gold schema and Delta log table if they do not exist."""
    catalog_name = settings.catalog
    schema_name = settings.gold_schema

    catalog = quote_identifier(catalog_name)
    gold_schema = quote_identifier(schema_name)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {log_table_name} (
            pipeline_run_id STRING,
            stage_name STRING,
            start_time_utc TIMESTAMP,
            end_time_utc TIMESTAMP,
            duration_seconds DOUBLE,
            status STRING,
            error_message STRING
        )
        USING DELTA
        """
    )

    # Verify the table is visible to this Spark session
    spark.sql(f"DESCRIBE TABLE {log_table_name}").show(truncate=False)

    print(f"Pipeline log table ready: {log_table_name}", flush=True)


def write_stage_log(
    spark,
    log_table_name: str,
    pipeline_run_id: str,
    stage_name: str,
    start_time: datetime,
    end_time: datetime,
    duration_seconds: float,
    status: str,
    error_message: str | None,
) -> None:
    """Append one stage execution record to the Delta log table."""
    record = [
        (
            pipeline_run_id,
            stage_name,
            start_time,
            end_time,
            float(duration_seconds),
            status,
            error_message,
        )
    ]

    log_df = spark.createDataFrame(record, schema=LOG_SCHEMA)

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(log_table_name)
    )


def run_stage(
    spark,
    log_table_name: str,
    pipeline_run_id: str,
    stage_name: str,
    module_name: str,
) -> bool:
    """Run one stage and persist its result."""
    print(f"\n{'=' * 70}")
    print(f"STARTING: {stage_name}")
    print(f"Module: {module_name}")
    print("=" * 70, flush=True)

    start_time_utc = utc_now_naive()
    start_perf = time.perf_counter()

    result = subprocess.run(
        [sys.executable, "-u", "-m", module_name],
        check=False,
    )

    duration_seconds = time.perf_counter() - start_perf
    end_time_utc = utc_now_naive()

    if result.returncode == 0:
        status = "SUCCESS"
        error_message = None
    else:
        status = "FAILED"
        error_message = (
            f"Process exited with return code {result.returncode}"
        )

    write_stage_log(
        spark=spark,
        log_table_name=log_table_name,
        pipeline_run_id=pipeline_run_id,
        stage_name=stage_name,
        start_time=start_time_utc,
        end_time=end_time_utc,
        duration_seconds=duration_seconds,
        status=status,
        error_message=error_message,
    )

    print(
        f"\n[{status}] {stage_name} "
        f"completed in {duration_seconds:.2f} seconds."
    )

    if status == "FAILED":
        print(f"Error: {error_message}")
        return False

    return True


def main() -> None:
    """Run all pipeline stages and log each stage to Delta."""
    spark = get_spark()

    pipeline_run_id = str(uuid4())
    log_table_name = qualified_table(settings.gold_schema, LOG_TABLE)

    print(f"Pipeline run ID: {pipeline_run_id}")

    ensure_log_table(spark, log_table_name)

    pipeline_start = time.perf_counter()

    for stage_name, module_name in PIPELINE_STAGES:
        success = run_stage(
            spark=spark,
            log_table_name=log_table_name,
            pipeline_run_id=pipeline_run_id,
            stage_name=stage_name,
            module_name=module_name,
        )

        if not success:
            print(
                "\nPipeline stopped because a stage failed. "
                "Downstream stages were not executed."
            )
            raise SystemExit(1)

    total_duration = time.perf_counter() - pipeline_start

    print(f"\n{'=' * 70}")
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Total duration: {total_duration:.2f} seconds")
    print(f"Stage logs saved to: {log_table_name}")
    print("=" * 70)


if __name__ == "__main__":
    main()
