
from __future__ import annotations

import json
import logging
import os
import queue
import re
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow.parquet as pq


# ============================================================
# Configuration
# ============================================================

ROOT = Path(__file__).resolve().parent

PYTHON = sys.executable
DBT = ROOT / ".venv" / "Scripts" / "dbt.exe"
DBT_PROJECT = ROOT / "music_analytics"
DBT_PROFILES = ROOT / "music_analytics"

LOG_DIR = ROOT / "logs"
REPORT_DIR = ROOT / "reports"

PARQUET_FILE = ROOT / "data" / "processed" / "songs.parquet"
WAREHOUSE_FILE = ROOT / "data" / "warehouse" / "music_analytics.duckdb"
EXTRACTION_FAILURE_FILE = (
    ROOT / "data" / "processed" / "extraction_errors.csv"
)


# ============================================================
# Logging
# ============================================================

def configure_logging(run_id: str) -> tuple[logging.Logger, Path]:
    """Write pipeline messages to the console and a timestamped log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / f"pipeline_{run_id}.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("music_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Pipeline run started")
    logger.info("Run ID: %s", run_id)
    logger.info("Log file: %s", log_file)

    return logger, log_file


# ============================================================
# JSON reporting
# ============================================================

def write_json_report(report: dict[str, Any]) -> Path:
    """Write a JSON report using an atomic file replacement."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    report_path = REPORT_DIR / f"pipeline_{report['run_id']}.json"
    temporary_path = report_path.with_suffix(".json.tmp")

    temporary_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temporary_path.replace(report_path)

    return report_path


# ============================================================
# Extraction metrics
# ============================================================

def collect_extraction_metrics() -> dict[str, Any]:
    """Collect row count, file size, and extraction failure count."""
    if not PARQUET_FILE.is_file():
        raise FileNotFoundError(
            f"Parquet output not found: {PARQUET_FILE}"
        )

    table = pq.read_table(PARQUET_FILE)
    metadata = table.schema
    columns = metadata.names
    required_columns = [
        "track_id",
        "song_id",
        "title",
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
        "release",
        "year",
        "genre",
        "duration_seconds",
        "tempo",
        "loudness",
        "danceability",
        "energy",
        "key",
        "mode",
        "time_signature",
        "song_hotttnesss",
        "artist_hotttnesss",
        "artist_familiarity",
        "source_file",
        "ingested_at_utc",
    ]
    missing_columns = sorted(set(required_columns) - set(columns))
    if missing_columns:
        raise ValueError(f"Parquet missing required columns: {missing_columns}")

    df = table.to_pandas()
    extracted_rows = len(df)
    duplicate_track_ids = int(df["track_id"].duplicated(keep=False).sum())
    duplicate_source_files = int(df["source_file"].duplicated(keep=False).sum())
    null_count = int(df[["track_id", "source_file"]].isna().sum().sum())

    failure_count = 0
    if EXTRACTION_FAILURE_FILE.is_file():
        failure_count = len(pd.read_csv(EXTRACTION_FAILURE_FILE))

    return {
        "output_file": str(PARQUET_FILE),
        "extracted_rows": extracted_rows,
        "failed_files": failure_count,
        "parquet_size_bytes": PARQUET_FILE.stat().st_size,
        "unique_track_ids": int(df["track_id"].nunique(dropna=True)),
        "unique_source_files": int(df["source_file"].nunique(dropna=True)),
        "duplicate_track_ids": duplicate_track_ids,
        "duplicate_source_files": duplicate_source_files,
        "null_required_fields": null_count,
    }


# ============================================================
# dbt result parsing
# ============================================================

def parse_dbt_output(stage: dict[str, Any]) -> dict[str, Any]:
    """Extract available model and test totals from captured output."""
    lines = stage.pop("_output_lines", [])

    metrics: dict[str, Any] = {}

    for line in lines:
        # Example: PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8
        if "Done." in line and "TOTAL=" in line:
            matches = re.findall(r"([A-Z-]+)=(\d+)", line)
            if matches:
                metrics["results"] = {
                    key.lower().replace("-", "_"): int(value)
                    for key, value in matches
                }

        # Example: Found 4 models, 8 data tests, 500 macros
        found = re.search(
            r"Found\s+(\d+)\s+models?,\s+(\d+)\s+data tests?",
            line,
            flags=re.IGNORECASE,
        )
        if found:
            metrics["models_discovered"] = int(found.group(1))
            metrics["data_tests_discovered"] = int(found.group(2))

    return metrics


# ============================================================
# Warehouse metrics
# ============================================================

def collect_warehouse_metrics() -> dict[str, Any]:
    """Query row counts from the existing DuckDB analytics tables."""
    if not WAREHOUSE_FILE.is_file():
        raise FileNotFoundError(
            f"DuckDB warehouse not found: {WAREHOUSE_FILE}"
        )

    table_names = [
        "stg_songs",
        "dim_artists",
        "fct_song_audio",
        "song_yearly_summary",
    ]

    counts: dict[str, int] = {}

    with duckdb.connect(str(WAREHOUSE_FILE), read_only=True) as conn:
        for table_name in table_names:
            query = f'SELECT COUNT(*) FROM analytics."{table_name}"'
            counts[table_name] = conn.execute(query).fetchone()[0]

    return {
        "database_file": str(WAREHOUSE_FILE),
        "table_row_counts": counts,
    }


def validate_parquet_dataset(path: Path) -> dict[str, Any]:
    """Validate an extracted dataset before pipeline approval."""
    if not path.is_file():
        raise FileNotFoundError(f"Parquet output not found: {path}")

    try:
        table = pq.read_table(path)
    except Exception as exc:
        raise ValueError(f"Parquet file unreadable: {path}") from exc

    columns = table.schema.names
    required = [
        "track_id",
        "song_id",
        "title",
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
        "release",
        "year",
        "genre",
        "duration_seconds",
        "tempo",
        "loudness",
        "danceability",
        "energy",
        "key",
        "mode",
        "time_signature",
        "song_hotttnesss",
        "artist_hotttnesss",
        "artist_familiarity",
        "source_file",
        "ingested_at_utc",
    ]
    missing = sorted(set(required) - set(columns))
    if missing:
        raise ValueError(f"Missing required parquet columns: {missing}")

    df = table.to_pandas()
    if df.empty:
        raise ValueError(f"Parquet file is empty: {path}")
    if df["track_id"].isna().any():
        raise ValueError("Parquet contains missing track_id values")
    if df["source_file"].isna().any():
        raise ValueError("Parquet contains missing source_file values")
    if df["track_id"].duplicated(keep=False).any():
        raise ValueError("Parquet contains duplicate track_id values")
    if df["source_file"].duplicated(keep=False).any():
        raise ValueError("Parquet contains duplicate source_file values")

    return {
        "row_count": int(len(df)),
        "columns": columns,
        "unique_track_ids": int(df["track_id"].nunique(dropna=True)),
        "unique_source_files": int(df["source_file"].nunique(dropna=True)),
        "parquet_size_bytes": path.stat().st_size,
    }


# ============================================================
# Stage execution
# ============================================================

def run_step(
    logger: logging.Logger,
    name: str,
    command: list[str],
    cwd: Path = ROOT,
) -> dict[str, Any]:
    """Run a stage, stream output to logs, and return its result."""
    logger.info("START | %s", name)
    logger.info("COMMAND | %s", subprocess.list2cmdline(command))

    start = time.perf_counter()
    output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()
    output_lines: list[str] = []

    process: subprocess.Popen[str] | None = None

    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        def read_output() -> None:
            assert process is not None
            assert process.stdout is not None

            try:
                for line in process.stdout:
                    output_queue.put(("line", line.rstrip("\r\n")))
            finally:
                output_queue.put(("done", None))

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()

        while True:
            kind, message = output_queue.get()

            if kind == "done":
                break

            if message is not None:
                output_lines.append(message)
                logger.info("[%s] %s", name, message)

        reader.join()
        exit_code = process.wait()
        duration = round(time.perf_counter() - start, 2)

        if exit_code != 0:
            logger.error(
                "FAILED | %s | exit_code=%d | duration=%.2f seconds",
                name,
                exit_code,
                duration,
            )

            error = subprocess.CalledProcessError(exit_code, command)
            error.output_lines = output_lines
            raise error

        logger.info(
            "SUCCESS | %s | duration=%.2f seconds",
            name,
            duration,
        )

        return {
            "status": "success",
            "exit_code": exit_code,
            "duration_seconds": duration,
            "_output_lines": output_lines,
        }

    except OSError:
        logger.exception("FAILED TO START | %s", name)
        raise

    finally:
        if process is not None and process.stdout is not None:
            process.stdout.close()


# ============================================================
# Pipeline definition
# ============================================================

def get_stages() -> list[tuple[str, list[str]]]:
    """Return pipeline stages in execution order."""
    return [
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


# ============================================================
# Main
# ============================================================

def main() -> int:
    run_start = datetime.now(timezone.utc)
    run_id = run_start.strftime("%Y%m%dT%H%M%SZ")

    logger, log_file = configure_logging(run_id)
    pipeline_timer = time.perf_counter()

    report: dict[str, Any] = {
        "project": "Million Song Dataset Analytics Pipeline",
        "run_id": run_id,
        "started_at_utc": run_start.isoformat(),
        "finished_at_utc": None,
        "status": "running",
        "total_duration_seconds": None,
        "log_file": str(log_file),
        "extraction_metrics": None,
        "dbt_run_metrics": None,
        "dbt_test_metrics": None,
        "warehouse_metrics": None,
        "stages": [],
        "error": None,
    }

    return_code = 0

    try:
        if not DBT.is_file():
            raise FileNotFoundError(
                f"dbt executable not found: {DBT}"
            )

        if not (DBT_PROJECT / "dbt_project.yml").is_file():
            raise FileNotFoundError(
                f"dbt project not found: {DBT_PROJECT}"
            )

        if not DBT_PROFILES.is_dir():
            raise FileNotFoundError(
                f"dbt profiles directory not found: {DBT_PROFILES}"
            )

        for name, command in get_stages():
            stage_start = datetime.now(timezone.utc)
            stage_timer = time.perf_counter()

            stage_report: dict[str, Any] = {
                "name": name,
                "status": "running",
                "started_at_utc": stage_start.isoformat(),
                "finished_at_utc": None,
                "duration_seconds": None,
                "exit_code": None,
                "metrics": None,
                "error": None,
            }

            report["stages"].append(stage_report)

            try:
                result = run_step(logger, name, command)

                stage_report["status"] = result["status"]
                stage_report["exit_code"] = result["exit_code"]
                stage_report["duration_seconds"] = (
                    result["duration_seconds"]
                )

                output_lines = result["_output_lines"]

                if name == "Extract HDF5 to Parquet":
                    report["extraction_metrics"] = collect_extraction_metrics()
                    stage_report["metrics"] = report["extraction_metrics"]
                    logger.info(
                        "Extraction metrics: %s",
                        json.dumps(stage_report["metrics"]),
                    )
                    validation = validate_parquet_dataset(PARQUET_FILE)
                    report["extraction_metrics"].update({
                        "validated": validation,
                    })
                    stage_report["metrics"] = report["extraction_metrics"]

                elif name == "Build dbt models":
                    stage_report["metrics"] = parse_dbt_output(
                        {"_output_lines": output_lines}
                    )
                    report["dbt_run_metrics"] = stage_report["metrics"]

                elif name == "Run dbt data-quality tests":
                    stage_report["metrics"] = parse_dbt_output(
                        {"_output_lines": output_lines}
                    )
                    report["dbt_test_metrics"] = stage_report["metrics"]

            except subprocess.CalledProcessError as exc:
                stage_report["status"] = "failed"
                stage_report["exit_code"] = exc.returncode
                stage_report["error"] = str(exc)
                raise

            except Exception as exc:
                stage_report["status"] = "failed"
                stage_report["error"] = (
                    f"{type(exc).__name__}: {exc}"
                )
                raise

            finally:
                stage_report["finished_at_utc"] = (
                    datetime.now(timezone.utc).isoformat()
                )

                if stage_report["duration_seconds"] is None:
                    stage_report["duration_seconds"] = round(
                        time.perf_counter() - stage_timer,
                        2,
                    )

        report["warehouse_metrics"] = collect_warehouse_metrics()

        logger.info(
            "Warehouse metrics: %s",
            json.dumps(report["warehouse_metrics"]),
        )

        report["status"] = "success"

        logger.info("========== PIPELINE SUMMARY ==========")

        for stage in report["stages"]:
            logger.info(
                "%s | %s | %.2f seconds",
                stage["status"].upper(),
                stage["name"],
                stage["duration_seconds"],
            )

        logger.info("PIPELINE COMPLETED SUCCESSFULLY")

        if report["extraction_metrics"] is None:
            raise ValueError("Extraction metrics missing from successful pipeline report")
        if report["warehouse_metrics"] is None:
            raise ValueError("Warehouse metrics missing from successful pipeline report")

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }

        logger.exception("PIPELINE FAILED")

        successful_stages = sum(
            stage["status"] == "success"
            for stage in report["stages"]
        )

        logger.info(
            "Completed stages before failure: %d/%d",
            successful_stages,
            len(get_stages()),
        )

        return_code = 1

    finally:
        report["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()

        report["total_duration_seconds"] = round(
            time.perf_counter() - pipeline_timer,
            2,
        )

        logger.info(
            "Total duration: %.2f seconds",
            report["total_duration_seconds"],
        )
        logger.info("Pipeline run ended")

        try:
            report_path = write_json_report(report)
            logger.info("JSON report: %s", report_path)

        except Exception:
            logger.exception("Could not write JSON pipeline report")
            return_code = 1

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
