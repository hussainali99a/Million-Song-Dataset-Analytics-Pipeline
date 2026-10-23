
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
DBT = ROOT / ".venv" / "Scripts" / "dbt.exe"
DBT_PROJECT = ROOT / "music_analytics"
DBT_PROFILES = Path.home() / ".dbt"


def run_step(name: str, command: list[str], cwd: Path = ROOT) -> None:
    """Run one pipeline stage and stop if it fails."""
    print(f"\n{'=' * 60}\nSTART: {name}\n{'=' * 60}", flush=True)

    subprocess.run(command, cwd=cwd, check=True)

    print(f"\nSUCCESS: {name}", flush=True)


def main() -> int:
    try:
        if not DBT.exists():
            raise FileNotFoundError(f"dbt executable not found: {DBT}")

        run_step(
            "Extract HDF5 dataset to Parquet",
            [PYTHON, "-m", "src.processing.extract_to_parquet"],
        )

        run_step(
            "Build dbt models",
            [
                str(DBT),
                "run",
                "--project-dir",
                str(DBT_PROJECT),
                "--profiles-dir",
                str(DBT_PROFILES),
            ],
        )

        run_step(
            "Validate dbt data quality",
            [
                str(DBT),
                "test",
                "--project-dir",
                str(DBT_PROJECT),
                "--profiles-dir",
                str(DBT_PROFILES),
            ],
        )

        print("\nPIPELINE COMPLETED SUCCESSFULLY", flush=True)
        return 0

    except subprocess.CalledProcessError as exc:
        print(
            f"\nPIPELINE FAILED: command exited with code {exc.returncode}",
            file=sys.stderr,
            flush=True,
        )
        return exc.returncode or 1

    except Exception as exc:
        print(f"\nPIPELINE FAILED: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
