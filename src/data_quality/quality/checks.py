
"""Reusable data-quality checks for Spark DataFrames."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def count_violations(dataframe: DataFrame, condition) -> int:
    """Count rows that violate a condition."""
    return dataframe.filter(condition).count()


def check_not_null(dataframe: DataFrame, column: str) -> int:
    """Count rows where a required column is null."""
    return count_violations(dataframe, F.col(column).isNull())



def check_unique(dataframe: DataFrame, column: str) -> int:
    """Count all rows belonging to duplicate-key groups."""
    duplicates = (
        dataframe.filter(F.col(column).isNotNull())
        .groupBy(column)
        .count()
        .filter(F.col("count") > 1)
    )

    result = duplicates.agg(
        F.sum("count").alias("duplicate_records")
    ).first()["duplicate_records"]

    return int(result or 0)


def check_allowed_values(
    dataframe: DataFrame,
    column: str,
    allowed_values: set[str],
) -> int:
    """Count non-null values outside an allowed set."""
    return count_violations(
        dataframe,
        F.col(column).isNotNull()
        & ~F.col(column).isin(sorted(allowed_values)),
    )


def check_timestamp_order(
    dataframe: DataFrame,
    earlier_column: str,
    later_column: str,
) -> int:
    """Count rows where a later timestamp precedes an earlier one."""
    return count_violations(
        dataframe,
        F.col(earlier_column).isNotNull()
        & F.col(later_column).isNotNull()
        & (F.col(later_column) < F.col(earlier_column)),
    )



def check_positive(dataframe: DataFrame, column: str) -> int:
    """Count non-null values that are zero, negative, or not valid numbers."""
    numeric_value = F.expr(f"try_cast(`{column}` AS DOUBLE)")

    invalid_condition = (
        F.col(column).isNotNull()
        & (
            numeric_value.isNull()
            | (numeric_value <= 0)
        )
    )

    return count_violations(dataframe, invalid_condition)


def check_non_negative(dataframe: DataFrame, column: str) -> int:
    """Count non-null values that are negative or not valid numbers."""
    numeric_value = F.expr(f"try_cast(`{column}` AS DOUBLE)")

    invalid_condition = (
        F.col(column).isNotNull()
        & (numeric_value.isNull() | (numeric_value < 0))
    )

    return count_violations(dataframe, invalid_condition)

