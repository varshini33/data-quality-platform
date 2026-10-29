
"""Run data-quality checks and persist results to a Delta audit table."""

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_quality.config import quote_identifier, settings
from data_quality.quality.checks import (
    check_allowed_values,
    check_non_negative,
    check_not_null,
    check_positive,
    check_timestamp_order,
    check_unique,
)
from data_quality.spark_session import get_spark


ALLOWED_ORDER_STATUSES = {
    "approved",
    "canceled",
    "created",
    "delivered",
    "invoiced",
    "processing",
    "shipped",
    "unavailable",
}

AUDIT_TABLE = "data_quality_audit"

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp_utc", TimestampType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("failed_records", LongType(), False),
    StructField("status", StringType(), False),
])


def qualified_table(schema: str, name: str) -> str:
    """Return a quoted Unity Catalog table identifier."""
    return ".".join(
        quote_identifier(part)
        for part in (settings.catalog, schema, name)
    )


def run_check(
    results: list[dict],
    run_id: str,
    source_table: str,
    check_name: str,
    failed_records: int,
) -> None:
    """Record a single check result."""
    failed_records = int(failed_records)

    results.append({
        "run_id": run_id,
        "run_timestamp_utc": datetime.now(timezone.utc).replace(tzinfo=None),
        "table_name": source_table,
        "check_name": check_name,
        "failed_records": failed_records,
        "status": "PASS" if failed_records == 0 else "FAIL",
    })

    print(
        f"[{'PASS' if failed_records == 0 else 'FAIL'}] "
        f"{source_table} | {check_name} | "
        f"violations={failed_records}",
        flush=True,
    )


def require_columns(dataframe, table: str, columns: set[str]) -> None:
    """Fail clearly if the source schema doesn't match expectations."""
    missing = columns - set(dataframe.columns)
    if missing:
        raise ValueError(
            f"{table} is missing required columns: {sorted(missing)}"
        )


def main() -> None:
    spark = get_spark()
    run_id = str(uuid4())

    orders_name = qualified_table(
        settings.silver_schema,
        settings.silver_orders_table,
    )
    items_name = qualified_table(
        settings.bronze_schema,
        "olist_order_items_dataset",
    )
    payments_name = qualified_table(
        settings.bronze_schema,
        "olist_order_payments_dataset",
    )
    audit_name = qualified_table(settings.gold_schema, AUDIT_TABLE)

    orders = spark.table(orders_name)
    items = spark.table(items_name)
    payments = spark.table(payments_name)

    require_columns(
        orders,
        orders_name,
        {
            "order_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
        },
    )
    require_columns(
        items, items_name, {"order_id", "price", "freight_value"}
    )
    require_columns(
        payments, payments_name, {"order_id", "payment_value"}
    )

    results: list[dict] = []

    # Silver orders checks.
    run_check(
        results, run_id, orders_name, "order_id_not_null",
        check_not_null(orders, "order_id"),
    )
    run_check(
        results, run_id, orders_name, "order_id_unique",
        check_unique(orders, "order_id"),
    )
    run_check(
        results, run_id, orders_name, "order_status_allowed",
        check_allowed_values(
            orders, "order_status", ALLOWED_ORDER_STATUSES
        ),
    )
    run_check(
        results, run_id, orders_name, "purchase_timestamp_not_null",
        check_not_null(orders, "order_purchase_timestamp"),
    )
    run_check(
        results, run_id, orders_name, "delivery_not_before_purchase",
        check_timestamp_order(
            orders,
            "order_purchase_timestamp",
            "order_delivered_customer_date",
        ),
    )

    # Bronze order items checks.
    run_check(
        results, run_id, items_name, "item_order_id_not_null",
        check_not_null(items, "order_id"),
    )
    run_check(
        results, run_id, items_name, "item_price_positive",
        check_positive(items, "price"),
    )
    run_check(
        results, run_id, items_name, "freight_value_non_negative",
        # Freight is expected to be non-negative; zero is allowed.
        check_non_negative(items, "freight_value"),
    )

    # Bronze payments checks.
    run_check(
        results, run_id, payments_name, "payment_order_id_not_null",
        check_not_null(payments, "order_id"),
    )
    run_check(
        results, run_id, payments_name, "payment_value_positive",
        check_positive(payments, "payment_value"),
    )

    # Persist results before returning a failure exit code.
    catalog = quote_identifier(settings.catalog)
    gold_schema = quote_identifier(settings.gold_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

    audit_df = spark.createDataFrame(results, schema=AUDIT_SCHEMA)
    (
        audit_df.write.format("delta")
        .mode("append")
        .saveAsTable(audit_name)
    )

    failed_checks = sum(row["status"] == "FAIL" for row in results)

    print(f"\nQuality run ID: {run_id}")
    print(f"Checks executed: {len(results)}")
    print(f"Failed checks: {failed_checks}")
    print(f"Audit table: {audit_name}")

    spark.table(audit_name).filter(
        F.col("run_id") == run_id
    ).orderBy("table_name", "check_name").show(truncate=False)

    if failed_checks:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
