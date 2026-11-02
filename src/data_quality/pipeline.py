
"""Run the complete Olist data pipeline in dependency order."""

import subprocess
import sys
import time


PIPELINE_STAGES = [
    ("Bronze ingestion", "data_quality.bronze.bronze_ingestion"),
    ("Silver transformation", "data_quality.silver.orders_cleanup"),
    ("Data quality checks", "data_quality.quality.run_quality_checks"),
    ("Gold business metrics", "data_quality.gold.business_metrics"),
]


def run_stage(stage_name: str, module_name: str) -> bool:
    """Run one pipeline module and report its duration and result."""
    print(f"\n{'=' * 70}")
    print(f"STARTING: {stage_name}")
    print(f"Module: {module_name}")
    print("=" * 70, flush=True)

    start_time = time.perf_counter()

    result = subprocess.run(
        [sys.executable, "-u", "-m", module_name],
        check=False,
    )

    duration = time.perf_counter() - start_time

    if result.returncode == 0:
        print(
            f"\n[SUCCESS] {stage_name} completed "
            f"in {duration:.2f} seconds."
        )
        return True

    print(
        f"\n[FAILED] {stage_name} failed "
        f"after {duration:.2f} seconds "
        f"(exit code: {result.returncode})."
    )
    return False


def main() -> None:
    """Run all pipeline stages sequentially."""
    pipeline_start = time.perf_counter()

    print("Starting Olist Data Quality & Lakehouse Pipeline")

    for stage_name, module_name in PIPELINE_STAGES:
        if not run_stage(stage_name, module_name):
            print(
                "\nPipeline stopped because a stage failed. "
                "Downstream stages were not executed."
            )
            raise SystemExit(1)

    total_duration = time.perf_counter() - pipeline_start

    print(f"\n{'=' * 70}")
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total duration: {total_duration:.2f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    main()
