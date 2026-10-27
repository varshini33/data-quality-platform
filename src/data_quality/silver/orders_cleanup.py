"""Clean Bronze orders and write valid and quarantined Silver snapshots.

WARNING: each run overwrites the configured Silver and quarantine tables.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_quality.config import quote_identifier, settings
from data_quality.spark_session import get_spark

TIMESTAMP_COLUMNS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)


def main() -> None:
    spark = get_spark()
    bronze_table = settings.bronze_orders_table
    silver_table = settings.silver_orders_fqn
    quarantine_table = settings.quarantine_orders_fqn
    catalog_sql = quote_identifier(settings.catalog)
    silver_schema_sql = quote_identifier(settings.silver_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{silver_schema_sql}")
    print(f"Reading Bronze table: {bronze_table}")
    df = spark.table(bronze_table)

    for column_name in TIMESTAMP_COLUMNS:
        df = df.withColumn(column_name, F.to_timestamp(F.col(column_name)))

    df = df.withColumn("_quality_issue", F.lit(None).cast("string"))
    if settings.quality_fail_on_null_keys:
        df = df.withColumn(
            "_quality_issue",
            F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID"))
             .when(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), F.lit("MISSING_CUSTOMER_ID"))
             .otherwise(F.col("_quality_issue")),
        )

    if settings.quality_fail_on_duplicates:
        dedup_window = Window.partitionBy("order_id").orderBy(
            F.col("customer_id").asc_nulls_last(),
            F.col("order_status").asc_nulls_last(),
        )
        df = df.withColumn("_duplicate_rank", F.row_number().over(dedup_window))
        df = df.withColumn(
            "_quality_issue",
            F.when(F.col("_quality_issue").isNotNull(), F.col("_quality_issue"))
             .when(F.col("_duplicate_rank") > 1, F.lit("DUPLICATE_ORDER_ID"))
             .otherwise(F.col("_quality_issue")),
        )
    else:
        df = df.withColumn("_duplicate_rank", F.lit(1))

    df = df.withColumn("_quality_checked_at", F.current_timestamp())
    valid_df = df.filter(F.col("_quality_issue").isNull()).drop("_quality_issue", "_duplicate_rank")
    quarantine_df = (df.filter(F.col("_quality_issue").isNotNull())
                     .withColumnRenamed("_quality_issue", "_quarantine_reason")
                     .drop("_duplicate_rank"))

    (valid_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(silver_table))
    (quarantine_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(quarantine_table))

    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    quarantine_count = spark.table(quarantine_table).count()
    print("\n========== SILVER QUALITY SUMMARY ==========")
    print(f"Bronze rows:     {bronze_count:,}")
    print(f"Silver rows:     {silver_count:,}")
    print(f"Quarantined:     {quarantine_count:,}")
    print(f"Reconciled:      {silver_count + quarantine_count:,}")
    print("\nQuarantine reasons:")
    spark.table(quarantine_table).groupBy("_quarantine_reason").count().orderBy(F.desc("count")).show(truncate=False)
    print("\nSilver orders sample:")
    spark.table(silver_table).show(5, truncate=False)

    if bronze_count != silver_count + quarantine_count:
        raise RuntimeError("Row reconciliation failed: Bronze count does not equal Silver plus quarantined rows.")
    print("Silver order cleaning completed successfully.")


if __name__ == "__main__":
    main()
