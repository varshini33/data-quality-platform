"""Load the Olist CSV source files from S3 into Bronze Delta tables.

WARNING: this is a full-refresh job. Existing target tables are overwritten.
"""

from pyspark.sql import functions as F

from data_quality.config import quote_identifier, settings
from data_quality.spark_session import get_spark

DATASETS = (
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
)


def main() -> None:
    spark = get_spark()
    catalog = quote_identifier(settings.catalog)
    schema = quote_identifier(settings.bronze_schema)

    print(f"Connected to Databricks. Target: {settings.catalog}.{settings.bronze_schema}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    audit_results = []
    for file_name in DATASETS:
        source_path = f"{settings.s3_raw_path.rstrip('/')}/{file_name}"
        table_name = file_name.removesuffix(".csv")
        target_table = (
            f"{catalog}.{schema}.{quote_identifier(table_name)}"
        )
        print(f"\nReading source: {source_path}")
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(source_path)
        )
        df = (
            df.withColumn("_source_file", F.lit(file_name))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingestion_date", F.current_date())
        )
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(target_table))
        row_count = spark.table(target_table).count()
        audit_results.append((file_name, f"{settings.catalog}.{settings.bronze_schema}.{table_name}", row_count))
        print(f"Created/updated {target_table}: {row_count:,} rows")

    print("\n========== BRONZE INGESTION SUMMARY ==========")
    for file_name, table_name, row_count in audit_results:
        print(f"{file_name} | {table_name} | {row_count:,} rows | SUCCESS")
    print(f"Files processed: {len(audit_results)}")


if __name__ == "__main__":
    main()
