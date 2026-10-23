
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "MillionSongSubset"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "songs.parquet"


def decode_value(value):
    """Convert HDF5 byte strings into readable Python strings."""
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8", errors="replace").rstrip("\x00").strip()
    return value


def clean_number(value):
    """Convert non-finite numeric values to None."""
    number = float(value)
    return number if np.isfinite(number) else None


def read_scalar(record, field):
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


def extract_song(file_path: Path, ingested_at: str) -> dict:
    """Extract one song's descriptive metadata and audio features."""
    with h5py.File(file_path, "r") as h5:
        analysis = h5["analysis/songs"][()]
        metadata = h5["metadata/songs"][()]
        musicbrainz = h5["musicbrainz/songs"][()]

        a = analysis
        m = metadata
        mb = musicbrainz

        return {
            "track_id": read_scalar(a, "track_id"),
            "song_id": read_scalar(m, "song_id"),
            "title": read_scalar(m, "title"),
            "artist_id": read_scalar(m, "artist_id"),
            "artist_name": read_scalar(m, "artist_name"),
            "artist_location": read_scalar(m, "artist_location"),
            "artist_latitude": read_scalar(m, "artist_latitude"),
            "artist_longitude": read_scalar(m, "artist_longitude"),
            "release": read_scalar(m, "release"),
            "year": read_scalar(mb, "year"),
            "genre": read_scalar(m, "genre"),
            "duration_seconds": read_scalar(a, "duration"),
            "tempo": read_scalar(a, "tempo"),
            "loudness": read_scalar(a, "loudness"),
            "danceability": read_scalar(a, "danceability"),
            "energy": read_scalar(a, "energy"),
            "key": read_scalar(a, "key"),
            "mode": read_scalar(a, "mode"),
            "time_signature": read_scalar(a, "time_signature"),
            "song_hotttnesss": read_scalar(m, "song_hotttnesss"),
            "artist_hotttnesss": read_scalar(m, "artist_hotttnesss"),
            "artist_familiarity": read_scalar(m, "artist_familiarity"),
            "source_file": str(file_path.relative_to(PROJECT_ROOT)),
            "ingested_at_utc": ingested_at,
        }


def extract_dataset(
    input_dir: Path = DEFAULT_INPUT,
    output_file: Path = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """Extract all HDF5 songs and write a compressed Parquet file."""
    input_dir = Path(input_dir)
    output_file = Path(output_file)

    if not input_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {input_dir}")

    files = sorted(input_dir.rglob("*.h5"))
    if not files:
        raise FileNotFoundError(f"No HDF5 song files found in: {input_dir}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = []
    failures = []

    for index, file_path in enumerate(files, start=1):
        try:
            rows.append(extract_song(file_path, ingested_at))
        except (OSError, KeyError, ValueError, TypeError) as exc:
            failures.append({
                "source_file": str(file_path),
                "error": str(exc),
            })

        if index % 500 == 0:
            print(f"Processed {index:,}/{len(files):,} files")

    if not rows:
        raise RuntimeError(
            f"No songs extracted successfully. Failed files: {len(failures)}"
        )

    df = pd.DataFrame(rows)

    # Explicit types make the Parquet schema more consistent.
    string_columns = [
        "track_id", "song_id", "title", "artist_id", "artist_name",
        "artist_location", "release", "genre", "source_file",
        "ingested_at_utc",
    ]
    for column in string_columns:
        df[column] = df[column].astype("string")

    integer_columns = ["year", "key", "mode", "time_signature"]
    for column in integer_columns:
        df[column] = pd.to_numeric(
            df[column], errors="coerce"
        ).astype("Int64")

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    failure_file = output_file.parent / "extraction_errors.csv"
    if failures:
        pd.DataFrame(failures).to_csv(failure_file, index=False)
    elif failure_file.exists():
        failure_file.unlink()

    print("\nExtraction complete")
    print(f"Discovered: {len(files):,}")
    print(f"Extracted:  {len(df):,}")
    print(f"Failed:     {len(failures):,}")
    print(f"Output:     {output_file}")
    print(f"Size:       {output_file.stat().st_size:,} bytes")

    return df


if __name__ == "__main__":
    extract_dataset()
