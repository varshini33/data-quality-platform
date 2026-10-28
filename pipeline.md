## Data Pipeline

This project implements a Bronze–Silver–Gold lakehouse workflow using
Databricks, PySpark, Delta tables, and Unity Catalog.

### Architecture

1. **Raw ingestion:** Olist Brazilian e-commerce CSV files are stored in
   Amazon S3 and loaded into Bronze Delta tables.
2. **Silver transformation:** Orders are cleaned and validated, with
   invalid records handled by the existing quarantine logic.
3. **Gold analytics:** Business-ready tables are built from Silver orders
   and Bronze order-item and payment data.

### Gold Tables

| Table | Description |
|---|---|
| `monthly_sales` | Monthly order counts, item revenue, freight revenue, and items sold |
| `order_status_summary` | Order counts and percentage by status |
| `payment_type_summary` | Payment records and payment values by type |
| `delivery_performance` | Average delivery duration and on-time/late delivery counts |

Gold financial measures use Spark decimal types to avoid binary
floating-point rounding artifacts. Order-status percentages use aggregate
counts rather than an unpartitioned window.

### Run the Gold Pipeline

From the repository root, with the project environment and Databricks
profile configured:

```powershell
uv run python -u -m data_quality.gold.business_metrics
```

The pipeline overwrites the four Gold tables with the latest calculations.
Run it only when you intend to refresh those tables.

### Run Tests

```powershell
uv run python -m compileall src tests
uv run python -m unittest discover -s tests -p "test_*.py" -v
```

The Gold tests use small sample DataFrames for metric validation and check
the published Gold table schemas. A working Databricks connection and
existing Gold tables are required.

### Configuration and Security

- Configure environment-specific settings in `.env`.
- Keep `.env` out of Git.
- Commit `.env.example` with placeholder values only.
- Do not commit access tokens, passwords, or cloud credentials.
- Keep `pyproject.toml` and `uv.lock` in version control for reproducible
  Python dependency installation.
