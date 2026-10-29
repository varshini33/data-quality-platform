# Databricks Data Quality & Lakehouse Management Platform

## Overview

A data engineering portfolio project that uses Databricks, Apache Spark, Delta tables, and AWS S3 to build a data pipeline with data quality validation and business reporting.

The project uses the Brazilian E-Commerce Public Dataset by Olist.

## Architecture

**AWS S3 → Bronze → Silver → Gold → Data Quality Audit**

- **AWS S3:** Stores the raw Olist dataset.
- **Bronze:** Stores ingested source tables.
- **Silver:** Cleans and validates order data and separates quarantined records.
- **Gold:** Produces business-level sales, payment, order-status, and delivery metrics.
- **Data Quality Audit:** Records validation results, failed-record counts, and run timestamps.

## Technology Stack

- Python
- PySpark
- Databricks
- Delta Lake
- AWS S3
- AWS CLI
- Databricks CLI
- pytest
- Git and GitHub

## Data Quality Checks

The quality framework checks:

- Null and duplicate order IDs
- Allowed order statuses
- Missing purchase timestamps
- Delivery timestamps occurring before purchase timestamps
- Missing order IDs in item and payment data
- Positive prices and payment values
- Non-negative freight values

Each run records its results in:

`olist_data_quality.gold.data_quality_audit`

Failed checks are recorded in the audit table, and the runner exits with a non-zero status if failures are detected.

## Running the Project

Activate the project environment if needed and run commands from the repository root.

Run the quality checks:

```powershell
uv run python -u -m data_quality.quality.run_quality_checks
```

Run automated tests:

```powershell
uv run pytest -q
```

Check Python syntax:

```powershell
uv run python -m compileall src
```

## Business Outputs

The Gold layer includes:

- `monthly_sales`
- `order_status_summary`
- `payment_type_summary`
- `delivery_performance`

## Security

Credentials are configured locally and should not be committed to Git. Keep `.env` files, access tokens, and other secrets out of the repository.

## Future Improvements

- Schedule pipeline execution
- Add more data quality rules and alerts
- Add automated pipeline monitoring
- Build dashboards from Gold tables
- Add incremental ingestion and recovery capabilities