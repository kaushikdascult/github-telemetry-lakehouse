"""
End-to-End Pipeline Runner: Ingestion → dbt Run → dbt Test.
Can be invoked standalone via CLI, cron, or Airflow BashOperator.
Uses my_python312_env with zero disk bloat.
"""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("run_pipeline")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = PROJECT_ROOT / "dbt"
PYTHON_BIN = sys.executable


def run_command(cmd: list, cwd: Path = PROJECT_ROOT) -> None:
    """Execute shell command with stream logging and fail-fast non-zero check."""
    log.info(f"Executing: {' '.join(str(c) for c in cmd)}")
    t0 = time.time()
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=False)
    elapsed = time.time() - t0
    if result.returncode != 0:
        log.error(f"Command failed with exit code {result.returncode} in {elapsed:.1f}s")
        sys.exit(result.returncode)
    log.info(f"Command succeeded in {elapsed:.1f}s")


def main():
    log.info("Starting GitHub Telemetry Lakehouse batch execution...")
    t_start = time.time()

    # 1. Ingestion: Fetch from GitHub API and stream to BigQuery Bronze
    ingest_script = PROJECT_ROOT / "ingestion" / "fetch_and_push_bq.py"
    run_command([PYTHON_BIN, str(ingest_script)])

    # 2. dbt Transformations: Silver & Gold layers
    dbt_bin = Path(PYTHON_BIN).parent / "dbt"
    run_command([str(dbt_bin), "run", "--profiles-dir", "."], cwd=DBT_DIR)

    # 3. dbt Data Quality Assertions
    run_command([str(dbt_bin), "test", "--profiles-dir", "."], cwd=DBT_DIR)

    total_time = time.time() - t_start
    log.info(f"Pipeline executed successfully end-to-end in {total_time:.1f}s.")


if __name__ == "__main__":
    main()
