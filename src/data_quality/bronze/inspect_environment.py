"""Inspect Databricks catalogs and test access to the configured Olist S3 path."""

from data_quality.config import settings
from data_quality.spark_session import get_spark


def main() -> None:
    spark = get_spark()
    print("=== Available catalogs ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    orders_path = f"{settings.s3_raw_path.rstrip('/')}/olist_orders_dataset.csv"
    print(f"\n=== Testing Olist source access: {orders_path} ===")
    orders_df = (spark.read.option("header", "true")
                 .option("inferSchema", "false").csv(orders_path))
    orders_df.printSchema()
    orders_df.show(5, truncate=False)
    print("Orders row count:", orders_df.count())


if __name__ == "__main__":
    main()
