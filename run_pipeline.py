
from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
DBT = ROOT / ".venv" / "Scripts" / "dbt.exe"
DBT_PROJECT = ROOT / "music_analytics"
DBT_PROFILES = Path.home() / ".dbt"
LOG_DIR = ROOT / "logs"


def configure_logging() -> logging.Logger:
    """Write logs to both the terminal and a timestamped file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = LOG_DIR / f"pipeline_{run_id}.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("music_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info("Pipeline run started")
    logger.info("Log file: %s", log_file)

    return logger


def run_step(
    logger: logging.Logger,
    name: str,
    command: list[str],
    cwd: Path = ROOT,
) -> float:
    """Run a stage, log its duration, and raise if it fails."""
    logger.info("START | %s", name)
    start = time.perf_counter()

    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError:
        duration = time.perf_counter() - start
        logger.exception(
            "FAILED | %s | duration=%.2f seconds",
            name,
            duration,
        )
        raise

    duration = time.perf_counter() - start
    logger.info(
        "SUCCESS | %s | duration=%.2f seconds",
        name,
        duration,
    )
    return duration


def main() -> int:
    logger = configure_logging()
    pipeline_start = time.perf_counter()
    completed_steps: list[tuple[str, float]] = []

    try:
        if not DBT.is_file():
            raise FileNotFoundError(f"dbt executable not found: {DBT}")

        if not (DBT_PROJECT / "dbt_project.yml").is_file():
            raise FileNotFoundError(
                f"dbt project not found: {DBT_PROJECT}"
            )

        stages = [
            (
                "Extract HDF5 to Parquet",
                [
                    PYTHON,
                    "-m",
                    "src.processing.extract_to_parquet",
                ],
            ),
            (
                "Build dbt models",
                [
                    str(DBT),
                    "run",
                    "--project-dir",
                    str(DBT_PROJECT),
                    "--profiles-dir",
                    str(DBT_PROFILES),
                ],
            ),
            (
                "Run dbt data-quality tests",
                [
                    str(DBT),
                    "test",
                    "--project-dir",
                    str(DBT_PROJECT),
                    "--profiles-dir",
                    str(DBT_PROFILES),
                ],
            ),
        ]

        for name, command in stages:
            duration = run_step(logger, name, command)
            completed_steps.append((name, duration))

        total_duration = time.perf_counter() - pipeline_start

        logger.info("========== PIPELINE SUMMARY ==========")
        for name, duration in completed_steps:
            logger.info("PASS | %s | %.2f seconds", name, duration)

        logger.info("Total duration: %.2f seconds", total_duration)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        return 0

    except Exception:
        logger.exception("PIPELINE FAILED")
        logger.info(
            "Completed stages before failure: %d/%d",
            len(completed_steps),
            3,
        )
        return 1

    finally:
        logging.getLogger("music_pipeline").info("Pipeline run ended")


if __name__ == "__main__":
    raise SystemExit(main())
