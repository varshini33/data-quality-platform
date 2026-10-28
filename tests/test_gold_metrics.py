
"""Tests for Gold metric transformations and published tables."""

import unittest
from datetime import datetime

from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_quality.config import settings
from data_quality.gold.business_metrics import (
    build_delivery_performance,
    build_monthly_sales,
    build_order_status_summary,
    build_payment_type_summary,
    table_name,
)
from data_quality.spark_session import get_spark


class TestGoldMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark()

        cls.orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
        ])

        cls.orders = cls.spark.createDataFrame(
            [
                (
                    "o1", "delivered",
                    datetime(2024, 1, 2, 10),
                    datetime(2024, 1, 5, 10),
                    datetime(2024, 1, 6, 10),
                ),
                (
                    "o2", "delivered",
                    datetime(2024, 1, 10, 10),
                    datetime(2024, 1, 15, 10),
                    datetime(2024, 1, 12, 10),
                ),
                (
                    "o3", "canceled",
                    datetime(2024, 2, 1, 10),
                    None,
                    datetime(2024, 2, 8, 10),
                ),
            ],
            schema=cls.orders_schema,
        )

        cls.items = cls.spark.createDataFrame(
            [
                ("o1", "10.10", "2.50"),
                ("o1", "5.25", "1.25"),
                ("o2", "20.00", "3.00"),
            ],
            ["order_id", "price", "freight_value"],
        )

        cls.payments = cls.spark.createDataFrame(
            [
                ("o1", "credit_card", "15.35"),
                ("o2", "credit_card", "20.00"),
                ("o3", "voucher", "5.00"),
            ],
            ["order_id", "payment_type", "payment_value"],
        )

    def test_monthly_sales_uses_decimal_money(self):
        result = build_monthly_sales(self.orders, self.items)
        january = result.filter("order_month = TIMESTAMP '2024-01-01 00:00:00'").first()

        self.assertIsNotNone(january)
        self.assertEqual(january.total_orders, 2)
        self.assertEqual(str(january.item_revenue), "35.35")
        self.assertEqual(str(january.freight_revenue), "6.75")
        self.assertEqual(january.items_sold, 3)
        self.assertIsInstance(
            result.schema["item_revenue"].dataType, DecimalType
        )

    def test_order_status_percentages_sum_to_100(self):
        result = build_order_status_summary(self.orders)
        rows = {row.order_status: row for row in result.collect()}

        self.assertEqual(rows["delivered"].total_orders, 2)
        self.assertEqual(rows["canceled"].total_orders, 1)
        self.assertEqual(
            sum(float(row.order_percentage) for row in rows.values()),
            100.0,
        )

    def test_payment_summary_uses_decimal_money(self):
        result = build_payment_type_summary(self.payments)
        rows = {row.payment_type: row for row in result.collect()}

        self.assertEqual(rows["credit_card"].payment_records, 2)
        self.assertEqual(str(rows["credit_card"].total_payment_value), "35.35")
        self.assertEqual(str(rows["voucher"].total_payment_value), "5.00")
        self.assertIsInstance(
            result.schema["total_payment_value"].dataType, DecimalType
        )

    def test_delivery_performance_counts_late_and_on_time(self):
        result = build_delivery_performance(self.orders).first()

        self.assertEqual(result.delivered_orders, 2)
        self.assertEqual(result.orders_delivered_late, 1)
        self.assertEqual(result.orders_delivered_on_time, 1)
        self.assertEqual(float(result.average_delivery_days), 4.0)

    def test_published_gold_tables_exist(self):
        expected = {
            "monthly_sales": {
                "order_month", "total_orders", "item_revenue",
                "freight_revenue", "items_sold",
            },
            "order_status_summary": {
                "order_status", "total_orders", "order_percentage",
            },
            "payment_type_summary": {
                "payment_type", "payment_records", "total_payment_value",
            },
            "delivery_performance": {
                "delivered_orders", "average_delivery_days",
                "orders_delivered_late", "orders_delivered_on_time",
            },
        }

        for table, required_columns in expected.items():
            with self.subTest(table=table):
                dataframe = self.spark.table(
                    table_name(settings.gold_schema, table)
                )
                self.assertTrue(
                    required_columns.issubset(set(dataframe.columns))
                )
                self.assertGreater(dataframe.count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
