
from __future__ import annotations

import hashlib
import json
import os
import posixpath
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import h5py
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "MillionSongSubset"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "songs.parquet"

MANIFEST_FILE = (
    PROJECT_ROOT / "data" / "processed" / "ingestion_manifest.json"
)
FAILURE_FILE = (
    PROJECT_ROOT / "data" / "processed" / "extraction_errors.csv"
)

STRING_COLUMNS = [
    "track_id",
    "song_id",
    "title",
    "artist_id",
    "artist_name",
    "artist_location",
    "release",
    "genre",
    "source_file",
    "ingested_at_utc",
]

INTEGER_COLUMNS = ["year", "key", "mode", "time_signature"]

EXPECTED_COLUMNS = [
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


def normalize_source_path(
    path_value: Any,
    project_root: Path | str | None = None,
) -> str:
    """Normalize paths under the project root without rewriting external paths."""
    if path_value is None or (isinstance(path_value, float) and pd.isna(path_value)):
        return ""

    value = str(path_value).removeprefix("file://")
    root_value = str(project_root if project_root is not None else PROJECT_ROOT)
    windows_path = PureWindowsPath(value)
    windows_root = PureWindowsPath(root_value)

    if windows_path.is_absolute():
        path_parts = windows_path.parts
        root_parts = windows_root.parts
        if (
            windows_root.is_absolute()
            and len(path_parts) >= len(root_parts)
            and all(
                path_part.casefold() == root_part.casefold()
                for path_part, root_part in zip(path_parts, root_parts)
            )
        ):
            return PurePosixPath(*path_parts[len(root_parts):]).as_posix()
        return windows_path.as_posix()

    posix_path = PurePosixPath(value.replace("\\", "/"))
    posix_root = PurePosixPath(root_value.replace("\\", "/"))
    if posix_path.is_absolute():
        normalized_path = PurePosixPath(posixpath.normpath(posix_path.as_posix()))
        if posix_root.is_absolute():
            normalized_root = PurePosixPath(posixpath.normpath(posix_root.as_posix()))
            try:
                return normalized_path.relative_to(normalized_root).as_posix()
            except ValueError:
                pass
        return normalized_path.as_posix()

    normalized = PurePosixPath(posixpath.normpath(posix_path.as_posix())).as_posix()
    return "" if normalized == "." else normalized


def decode_value(value: Any) -> Any:
    """Convert HDF5 byte strings into readable Python strings."""
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8", errors="replace").rstrip("\x00").strip()
    return value


def clean_number(value: Any) -> float | None:
    """Convert non-finite numeric values to None."""
    number = float(value)
    return number if np.isfinite(number) else None


def read_scalar(record: Any, field: str) -> Any:
    """Safely read a field from a single-row structured HDF5 dataset."""
    if field not in record.dtype.names:
        return None

    value = record[field][0]

    if isinstance(value, (bytes, np.bytes_)):
        return decode_value(value)

    if isinstance(value, (np.integer, int)):
        return int(value)

    if isinstance(value, (np.floating, float)):
        return clean_number(value)

    return value


def relative_source_path(file_path: Path, root_dir: Path | None = None) -> str:
    """Return a stable source path relative to a known project root."""
    base_dir = (root_dir or PROJECT_ROOT).resolve()
    try:
        relative = file_path.resolve().relative_to(base_dir)
        return normalize_source_path(relative.as_posix())
    except ValueError:
        if file_path.is_absolute():
            try:
                relative = file_path.resolve().relative_to(PROJECT_ROOT.resolve())
                return normalize_source_path(relative.as_posix())
            except ValueError:
                pass
        return normalize_source_path(file_path.as_posix())


def file_signature(file_path: Path) -> dict[str, int | str]:
    """Build a content-aware signature used to detect source changes."""
    stat = file_path.stat()
    digest = hashlib.sha256()
    with file_path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)

    return {
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": digest.hexdigest(),
    }


def extract_song(file_path: Path, ingested_at: str) -> dict[str, Any]:
    """Extract one song's descriptive metadata and audio features."""
    with h5py.File(file_path, "r") as h5:
        analysis = h5["analysis/songs"][()]
        metadata = h5["metadata/songs"][()]
        musicbrainz = h5["musicbrainz/songs"][()]

        return {
            "track_id": read_scalar(analysis, "track_id"),
            "song_id": read_scalar(metadata, "song_id"),
            "title": read_scalar(metadata, "title"),
            "artist_id": read_scalar(metadata, "artist_id"),
            "artist_name": read_scalar(metadata, "artist_name"),
            "artist_location": read_scalar(metadata, "artist_location"),
            "artist_latitude": read_scalar(metadata, "artist_latitude"),
            "artist_longitude": read_scalar(metadata, "artist_longitude"),
            "release": read_scalar(metadata, "release"),
            "year": read_scalar(musicbrainz, "year"),
            "genre": read_scalar(musicbrainz, "genre"),
            "duration_seconds": read_scalar(analysis, "duration"),
            "tempo": read_scalar(analysis, "tempo"),
            "loudness": read_scalar(analysis, "loudness"),
            "danceability": read_scalar(analysis, "danceability"),
            "energy": read_scalar(analysis, "energy"),
            "key": read_scalar(analysis, "key"),
            "mode": read_scalar(analysis, "mode"),
            "time_signature": read_scalar(analysis, "time_signature"),
            "song_hotttnesss": read_scalar(metadata, "song_hotttnesss"),
            "artist_hotttnesss": read_scalar(metadata, "artist_hotttnesss"),
            "artist_familiarity": read_scalar(metadata, "artist_familiarity"),
            "source_file": relative_source_path(file_path),
            "ingested_at_utc": ingested_at,
        }


def validate_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Validate dataset integrity before writing or using it."""
    if df.empty:
        return df.copy()

    missing = [column for column in EXPECTED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Dataset missing expected columns: {missing}")

    normalized = df.copy()

    if "source_file" in normalized.columns:
        normalized["source_file"] = normalized["source_file"].map(
            lambda value: normalize_source_path(value) if pd.notna(value) else pd.NA
        )

    if "track_id" in normalized.columns:
        duplicate_track_ids = normalized["track_id"].duplicated(keep=False)
        if duplicate_track_ids.any():
            dupes = sorted(normalized.loc[duplicate_track_ids, "track_id"].dropna().unique().tolist())
            raise ValueError(f"Duplicate track_id values detected: {dupes[:10]}")

    duplicate_sources = normalized["source_file"].duplicated(keep=False)
    if duplicate_sources.any():
        dupes = sorted(normalized.loc[duplicate_sources, "source_file"].dropna().unique().tolist())
        raise ValueError(f"Duplicate source_file values detected: {dupes[:10]}")

    return normalized


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the stable schema used by the downstream dbt models."""
    df = df.copy()

    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df = df[EXPECTED_COLUMNS]

    if "source_file" in df.columns:
        df["source_file"] = df["source_file"].map(
            lambda value: normalize_source_path(value) if pd.notna(value) else pd.NA
        )

    for column in STRING_COLUMNS:
        df[column] = df[column].astype("string")

    for column in INTEGER_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    return df


def load_manifest(path: Path) -> dict[str, Any]:
    """Load the manifest, or return an empty manifest on first run."""
    if not path.exists():
        return {"version": 1, "files": {}}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid ingestion manifest: {path}") from exc

    if data.get("version") != 1 or not isinstance(data.get("files"), dict):
        raise ValueError(f"Invalid ingestion manifest: {path}")

    normalized_files: dict[str, dict[str, int | str]] = {}
    for raw_key, raw_value in data["files"].items():
        if not isinstance(raw_value, dict):
            raise TypeError(f"Manifest entry for {raw_key!r} is not a signature object")
        key = normalize_source_path(raw_key)
        if not key:
            raise ValueError(f"Manifest contains an empty path for {raw_key!r}")
        signature = {
            "size_bytes": int(raw_value["size_bytes"]),
            "mtime_ns": int(raw_value["mtime_ns"]),
        }
        if "sha256" in raw_value:
            signature["sha256"] = str(raw_value["sha256"])
        normalized_files[key] = signature

    data["files"] = normalized_files
    return data


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically replace a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    temporary_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temporary_path, path)


def atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write Parquet to a temporary file before replacing the target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(path.stem + ".tmp.parquet")

    previous = path.read_bytes() if path.exists() else None

    try:
        df.to_parquet(
            temporary_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        os.replace(temporary_path, path)
    except Exception:
        if previous is not None and path.exists():
            path.write_bytes(previous)
        raise
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def extract_dataset(
    input_dir: Path = DEFAULT_INPUT,
    output_file: Path = DEFAULT_OUTPUT,
    manifest_file: Path = MANIFEST_FILE,
    failure_file: Path | None = None,
) -> pd.DataFrame:
    """Incrementally extract changed HDF5 files into a Parquet dataset."""
    input_dir = Path(input_dir)
    output_file = Path(output_file)
    manifest_file = Path(manifest_file)
    failure_file = (
        Path(failure_file)
        if failure_file is not None
        else output_file.parent / FAILURE_FILE.name
    )

    if not input_dir.is_dir():
        raise FileNotFoundError(f"Dataset directory not found: {input_dir}")

    files = sorted(input_dir.rglob("*.h5"))
    output_file.parent.mkdir(parents=True, exist_ok=True)

    manifest_exists = manifest_file.exists()
    old_manifest = load_manifest(manifest_file) if manifest_exists else {"version": 1, "files": {}}
    if not files and not manifest_exists:
        raise FileNotFoundError(f"No HDF5 song files found in: {input_dir}")
    old_signatures = old_manifest["files"]

    current_files: dict[str, Path] = {}
    current_signatures: dict[str, dict[str, int | str]] = {}

    for file_path in files:
        relative_path = relative_source_path(file_path)
        current_files[relative_path] = file_path
        current_signatures[relative_path] = file_signature(file_path)

    deleted_paths = set(old_signatures) - set(current_files)

    if not output_file.exists() or not manifest_exists:
        changed_paths = set(current_files)
    else:
        changed_paths = {
            path
            for path, signature in current_signatures.items()
            if old_signatures.get(path) != signature
        }

    print(f"Discovered source files: {len(files):,}")
    print(f"New or changed files:    {len(changed_paths):,}")
    print(f"Deleted source files:    {len(deleted_paths):,}")

    old_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
    if output_file.exists():
        old_df = pd.read_parquet(output_file)
        old_df = normalize_dataframe(old_df)
        validate_dataset(old_df)
    elif manifest_exists:
        old_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        old_df = normalize_dataframe(old_df)
        validate_dataset(old_df)

    paths_to_replace = changed_paths | deleted_paths
    if not old_df.empty and paths_to_replace:
        old_df = old_df[~old_df["source_file"].isin(paths_to_replace)].copy()

    ingested_at = datetime.now(UTC).isoformat()
    new_rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    successful_signatures: dict[str, dict[str, int | str]] = {}

    for index, relative_path in enumerate(sorted(changed_paths), start=1):
        file_path = current_files[relative_path]
        try:
            row = extract_song(file_path, ingested_at)
            new_rows.append(row)
            successful_signatures[relative_path] = current_signatures[relative_path]
        except (OSError, KeyError, ValueError, TypeError) as exc:
            failures.append({"source_file": relative_path, "error": str(exc)})

        if index % 500 == 0:
            print(f"Processed changed files: {index:,}/{len(changed_paths):,}")

    if failures:
        failure_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(failures).to_csv(failure_file, index=False)

    failed_paths = {failure["source_file"] for failure in failures}
    if not old_df.empty and failed_paths:
        original_df = pd.read_parquet(output_file)
        original_df = normalize_dataframe(original_df)
        validate_dataset(original_df)
        retained = original_df[original_df["source_file"].isin(failed_paths)]
        old_df = pd.concat([old_df, retained], ignore_index=True)

    if new_rows:
        new_df = normalize_dataframe(pd.DataFrame(new_rows))
    else:
        new_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        new_df = normalize_dataframe(new_df)

    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined = normalize_dataframe(combined)
    validate_dataset(combined)

    if not combined.empty:
        combined = combined.drop_duplicates(subset=["source_file"], keep="last")
        validate_dataset(combined)

    parquet_needs_update = bool(new_rows) or bool(deleted_paths) or not output_file.exists()
    if parquet_needs_update:
        if combined.empty and not deleted_paths:
            raise RuntimeError("Incremental extraction would produce an empty dataset.")
        atomic_write_parquet(combined, output_file)

    next_signatures = {
        path: signature
        for path, signature in old_signatures.items()
        if path in current_files and path not in deleted_paths
    }
    next_signatures.update(successful_signatures)

    for path in current_files:
        if path not in changed_paths and path in old_signatures:
            next_signatures[path] = old_signatures[path]

    atomic_write_json(
        manifest_file,
        {
            "version": 1,
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "files": next_signatures,
        },
    )

    if not failures and failure_file.exists():
        failure_file.unlink()

    print("\nIncremental extraction complete")
    print(f"Discovered:       {len(files):,}")
    print(f"Changed processed:{len(new_rows):,}")
    print(f"Failed:           {len(failures):,}")
    print(f"Deleted sources:  {len(deleted_paths):,}")
    print(f"Final rows:       {len(combined):,}")
    print(f"Output:           {output_file}")
    print(f"Size:             {output_file.stat().st_size:,} bytes")
    print(f"Manifest:         {manifest_file}")

    return combined


if __name__ == "__main__":
    extract_dataset()
