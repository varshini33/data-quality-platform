# Data Quality & Lakehouse Management Platform

A learning/portfolio project that loads the Olist Brazilian e-commerce dataset into Databricks Bronze Delta tables, applies order-level quality checks, and writes clean Silver and quarantine tables.

## Configuration

1. Copy `.env.example` to `.env`.
2. Set `AWS_REGION` to the real region of your S3 bucket.
3. Review the Databricks profile, catalog, schema, bucket, and data-quality settings.
4. Keep `.env` and all credentials out of Git. Authentication is provided by the AWS CLI and Databricks CLI profiles; do not put access keys or Databricks tokens in `.env`.

## Setup (Windows PowerShell)

```powershell
cd "C:\Users\Hamsaa Varshini.B\git_projects\data-quality-platform"
Copy-Item .env.example .env
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

Edit `.env` before running the pipeline. Ensure the virtual environment uses Python 3.12.

## Run tests

Unit tests do not require cloud access:

```powershell
python -m pytest -q
```

Live Databricks integration tests are skipped by default. Enable them for the current PowerShell session only when you want to query your workspace:

```powershell
$env:RUN_DATABRICKS_INTEGRATION = "true"
python -m pytest -q
```

## Run the pipeline

Run Bronze ingestion first, then Silver cleaning:

```powershell
python -m data_quality.bronze.inspect_environment
python -m data_quality.bronze.bronze_ingestion
python -m data_quality.silver.orders_cleanup
```

**Important:** Bronze ingestion is a full refresh and overwrites the configured Bronze tables. Silver cleaning also overwrites the configured Silver and quarantine tables on each run.

## Upload/refresh the source data

The upload script reads `S3_BUCKET`, `S3_RAW_PREFIX`, `S3_MANIFEST_PREFIX`, and optionally `KAGGLE_DATASET_ID` from `.env`:

```powershell
.\scripts\upload_to_s3.ps1
```

Requires the AWS CLI and Kaggle CLI to be installed and authenticated. The Olist Kaggle dataset is a static snapshot, not a live feed.
