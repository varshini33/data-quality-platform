
"""Build business-ready Gold analytics tables from Bronze and Silver data."""

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from data_quality.config import quote_identifier, settings
from data_quality.spark_session import get_spark

MONEY_TYPE = DecimalType(18, 2)
PERCENT_TYPE = DecimalType(7, 2)


def table_name(schema: str, name: str) -> str:
    """Build a safely quoted three-part Unity Catalog table name."""
    return ".".join(
        quote_identifier(part)
        for part in (settings.catalog, schema, name)
    )


def money_sum(column_name: str):
    """Sum a monetary column using fixed-precision decimal arithmetic."""
    return F.sum(F.col(column_name).cast(MONEY_TYPE)).cast(MONEY_TYPE)


def build_monthly_sales(
    orders: DataFrame,
    items: DataFrame,
) -> DataFrame:
    """Build monthly sales metrics from orders and order items."""
    item_sales = (
        items.groupBy("order_id")
        .agg(
            money_sum("price").alias("item_revenue"),
            money_sum("freight_value").alias("freight_revenue"),
            F.count("*").alias("item_count"),
        )
    )

    order_metrics = (
        orders.join(item_sales, on="order_id", how="left")
        .withColumn(
            "order_month",
            F.date_trunc("month", F.col("order_purchase_timestamp")),
        )
    )

    return (
        order_metrics.groupBy("order_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            money_sum("item_revenue").alias("item_revenue"),
            money_sum("freight_revenue").alias("freight_revenue"),
            F.sum("item_count").alias("items_sold"),
        )
        .withColumn(
            "average_item_revenue_per_order",
            (
                F.col("item_revenue")
                / F.col("total_orders")
            ).cast(MONEY_TYPE),
        )
    )


def build_order_status_summary(orders: DataFrame) -> DataFrame:
    """Build order-status counts and percentages without a global window."""
    counts = orders.groupBy("order_status").agg(
        F.countDistinct("order_id").alias("total_orders")
    )

    totals = counts.agg(
        F.sum("total_orders").alias("all_orders")
    )

    return (
        counts.crossJoin(totals)
        .withColumn(
            "order_percentage",
            F.when(
                F.col("all_orders") > 0,
                F.round(
                    F.col("total_orders") * F.lit(100.0)
                    / F.col("all_orders"),
                    2,
                ),
            ).otherwise(F.lit(0.0))
            .cast(PERCENT_TYPE),
        )
        .drop("all_orders")
    )


def build_payment_type_summary(payments: DataFrame) -> DataFrame:
    """Build payment metrics grouped by payment type."""
    return payments.groupBy("payment_type").agg(
        F.count("*").alias("payment_records"),
        money_sum("payment_value").alias("total_payment_value"),
        F.avg(F.col("payment_value").cast(MONEY_TYPE))
        .cast(MONEY_TYPE)
        .alias("average_payment_value"),
    )


def build_delivery_performance(orders: DataFrame) -> DataFrame:
    """Build overall delivery-time and lateness metrics."""
    eligible = (
        orders.filter(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull()
        )
        .withColumn(
            "delivery_days",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_estimated_delivery_date"),
            ),
        )
    )

    return eligible.agg(
        F.countDistinct("order_id").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 2).alias("average_delivery_days"),
        F.sum(
            F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
        ).alias("orders_delivered_late"),
        F.sum(
            F.when(F.col("delivery_delay_days") <= 0, 1).otherwise(0)
        ).alias("orders_delivered_on_time"),
    )


def main() -> None:
    """Build and publish all Gold analytics tables."""
    spark = get_spark()

    orders_table = table_name(
        settings.silver_schema, settings.silver_orders_table
    )
    items_table = table_name(
        settings.bronze_schema, "olist_order_items_dataset"
    )
    payments_table = table_name(
        settings.bronze_schema, "olist_order_payments_dataset"
    )

    catalog = quote_identifier(settings.catalog)
    gold_schema = quote_identifier(settings.gold_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

    orders = spark.table(orders_table)
    items = spark.table(items_table)
    payments = spark.table(payments_table)

    outputs = {
        "monthly_sales": build_monthly_sales(orders, items),
        "order_status_summary": build_order_status_summary(orders),
        "payment_type_summary": build_payment_type_summary(payments),
        "delivery_performance": build_delivery_performance(orders),
    }

    for name, dataframe in outputs.items():
        destination = table_name(settings.gold_schema, name)
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(destination)
        )
        row_count = spark.table(destination).count()
        print(f"Created {destination}: {row_count:,} rows", flush=True)

    print("\nGold analytics build completed.", flush=True)
    print("\nMonthly sales preview:")
    spark.table(
        table_name(settings.gold_schema, "monthly_sales")
    ).orderBy("order_month").show(12, truncate=False)

    print("\nDelivery performance:")
    spark.table(
        table_name(settings.gold_schema, "delivery_performance")
    ).show(truncate=False)


if __name__ == "__main__":
    main()
