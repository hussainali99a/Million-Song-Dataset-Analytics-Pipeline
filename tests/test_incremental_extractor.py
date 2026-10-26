from __future__ import annotations

import json
import os
from pathlib import Path

import h5py
import pandas as pd
import pytest

from src.processing.extract_to_parquet import (
    EXPECTED_COLUMNS,
    extract_dataset,
    normalize_source_path,
    validate_dataset,
)


def make_hdf5_file(path: Path, *, track_id: str, artist_id: str = "AR1", title: str = "Song") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        analysis = h5.create_dataset(
            "analysis/songs",
            shape=(1,),
            dtype=[
                ("track_id", "S32"),
                ("duration", "f8"),
                ("tempo", "f8"),
                ("loudness", "f8"),
                ("danceability", "f8"),
                ("energy", "f8"),
                ("key", "i4"),
                ("mode", "i4"),
                ("time_signature", "i4"),
            ],
        )
        metadata = h5.create_dataset(
            "metadata/songs",
            shape=(1,),
            dtype=[
                ("song_id", "S32"),
                ("title", "S80"),
                ("artist_id", "S32"),
                ("artist_name", "S80"),
                ("artist_location", "S80"),
                ("artist_latitude", "f8"),
                ("artist_longitude", "f8"),
                ("release", "S80"),
                ("song_hotttnesss", "f8"),
                ("artist_hotttnesss", "f8"),
                ("artist_familiarity", "f8"),
            ],
        )
        musicbrainz = h5.create_dataset(
            "musicbrainz/songs",
            shape=(1,),
            dtype=[("year", "i4"), ("genre", "S40")],
        )

        analysis[0] = (
            track_id.encode("utf-8"),
            180.0,
            101.0,
            -6.2,
            0.7,
            0.8,
            1,
            1,
            4,
        )
        metadata[0] = (
            b"song-1",
            title.encode("utf-8"),
            artist_id.encode("utf-8"),
            b"Artist One",
            b"London",
            51.5,
            -0.1,
            b"Release",
            0.7,
            0.8,
            0.9,
        )
        musicbrainz[0] = (2000, b"Rock")


def test_first_ingestion_and_second_no_change_run(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002")

    df = extract_dataset(input_dir, output_file, manifest_file)
    assert len(df) == 2
    assert df["track_id"].is_unique
    assert df["source_file"].is_unique

    second = extract_dataset(input_dir, output_file, manifest_file)
    assert len(second) == 2
    assert second["track_id"].is_unique
    assert second["source_file"].is_unique
    assert json.loads(manifest_file.read_text(encoding="utf-8"))["files"]


def test_new_file_added_and_existing_file_changed(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002")
    extract_dataset(input_dir, output_file, manifest_file)

    make_hdf5_file(input_dir / "TR00003.h5", track_id="TR00003")
    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002B", title="Changed title")

    df = extract_dataset(input_dir, output_file, manifest_file)
    assert len(df) == 3
    assert df["track_id"].is_unique
    assert df["source_file"].is_unique
    row = df[df["track_id"] == "TR00002B"].iloc[0]
    assert row["title"] == "Changed title"


def test_same_size_and_mtime_content_change_is_reprocessed(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"
    source_file = input_dir / "TR00001.h5"

    make_hdf5_file(source_file, track_id="TR00001", title="Tune")
    extract_dataset(input_dir, output_file, manifest_file)
    original_stat = source_file.stat()

    make_hdf5_file(source_file, track_id="TR00001", title="Song")
    os.utime(
        source_file,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    updated_stat = source_file.stat()
    assert updated_stat.st_size == original_stat.st_size
    assert updated_stat.st_mtime_ns == original_stat.st_mtime_ns

    df = extract_dataset(input_dir, output_file, manifest_file)
    assert df.loc[0, "title"] == "Song"


def test_deleted_source_file_is_removed(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002")
    extract_dataset(input_dir, output_file, manifest_file)

    (input_dir / "TR00001.h5").unlink()
    df = extract_dataset(input_dir, output_file, manifest_file)

    assert len(df) == 1
    assert df["track_id"].tolist() == ["TR00002"]


def test_removing_all_source_files_reconciles_to_empty_dataset(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"
    first = input_dir / "TR00001.h5"
    second = input_dir / "TR00002.h5"
    make_hdf5_file(first, track_id="TR00001")
    make_hdf5_file(second, track_id="TR00002")
    extract_dataset(input_dir, output_file, manifest_file)

    first.unlink()
    second.unlink()
    df = extract_dataset(input_dir, output_file, manifest_file)

    assert df.empty
    assert pd.read_parquet(output_file).empty
    assert json.loads(manifest_file.read_text(encoding="utf-8"))["files"] == {}
    assert extract_dataset(input_dir, output_file, manifest_file).empty


def test_all_failed_first_run_persists_failure_report(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset"
    input_dir.mkdir(parents=True)
    source_file = input_dir / "broken.h5"
    source_file.write_text("not a valid HDF5 file", encoding="utf-8")
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"
    failure_file = tmp_path / "data" / "processed" / "extraction_errors.csv"

    with pytest.raises(RuntimeError, match="empty dataset"):
        extract_dataset(input_dir, output_file, manifest_file, failure_file)

    assert failure_file.is_file()
    failure_source = pd.read_csv(failure_file)["source_file"].iloc[0].replace("\\", "/")
    assert failure_source.endswith("data/raw/MillionSongSubset/broken.h5")


def test_failed_file_keeps_old_record_and_retries_next_run(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"
    failure_file = tmp_path / "failure-reports" / "nested" / "extraction_errors.csv"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002")
    extract_dataset(input_dir, output_file, manifest_file, failure_file)
    original_parquet = output_file.read_bytes()
    original_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))["files"]
    original_record = pd.read_parquet(output_file).set_index("track_id").loc["TR00002"]

    broken = input_dir / "TR00002.h5"
    broken.write_text("not-a-valid-hdf5-file", encoding="utf-8")
    df = extract_dataset(input_dir, output_file, manifest_file, failure_file)

    assert len(df) == 2
    assert df["track_id"].is_unique
    assert "TR00002" in df["track_id"].tolist()
    retained_record = df.set_index("track_id").loc["TR00002"]
    assert retained_record["title"] == original_record["title"]
    assert output_file.read_bytes() == original_parquet

    failed_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))["files"]
    source_path = retained_record["source_file"]
    assert failed_manifest[source_path] == original_manifest[source_path]
    assert failure_file.is_file()
    assert pd.read_csv(failure_file)["source_file"].tolist() == [source_path]

    make_hdf5_file(broken, track_id="TR00002", title="Recovered title")
    df = extract_dataset(input_dir, output_file, manifest_file, failure_file)
    assert len(df) == 2
    assert df[df["track_id"] == "TR00002"].iloc[0]["title"] == "Recovered title"
    assert not failure_file.exists()
    recovered_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))["files"]
    assert recovered_manifest[source_path] != original_manifest[source_path]


def test_invalid_manifest_is_rejected(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text('{"version": 0, "files": []}', encoding="utf-8")

    with pytest.raises(ValueError):
        extract_dataset(input_dir, output_file, manifest_file)


def test_missing_parquet_with_existing_manifest_is_reconciled(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    extract_dataset(input_dir, output_file, manifest_file)
    output_file.unlink()

    df = extract_dataset(input_dir, output_file, manifest_file)
    assert len(df) == 1
    assert df["track_id"].is_unique


def test_duplicate_track_and_source_validation(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "track_id": ["TR1", "TR1"],
            "song_id": ["S1", "S2"],
            "title": ["A", "B"],
            "artist_id": ["AR1", "AR2"],
            "artist_name": ["Art1", "Art2"],
            "artist_location": ["L1", "L2"],
            "artist_latitude": [1.0, 2.0],
            "artist_longitude": [3.0, 4.0],
            "release": ["Rel", "Rel"],
            "year": [2000, 2001],
            "genre": ["Rock", "Jazz"],
            "duration_seconds": [180.0, 200.0],
            "tempo": [100.0, 110.0],
            "loudness": [-5.0, -4.0],
            "danceability": [0.7, 0.6],
            "energy": [0.8, 0.9],
            "key": [1, 2],
            "mode": [1, 0],
            "time_signature": [4, 4],
            "song_hotttnesss": [0.7, 0.8],
            "artist_hotttnesss": [0.9, 0.8],
            "artist_familiarity": [0.7, 0.9],
            "source_file": ["a.h5", "a.h5"],
            "ingested_at_utc": ["2024-01-01", "2024-01-01"],
        }
    )

    with pytest.raises(ValueError):
        validate_dataset(df)


def test_atomic_write_failure_preserves_previous_dataset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_dir = tmp_path / "data" / "raw" / "MillionSongSubset" / "A" / "A" / "A"
    output_file = tmp_path / "data" / "processed" / "songs.parquet"
    manifest_file = tmp_path / "data" / "processed" / "ingestion_manifest.json"

    make_hdf5_file(input_dir / "TR00001.h5", track_id="TR00001")
    # initial = extract_dataset(input_dir, output_file, manifest_file)
    # original_bytes = output_file.read_bytes()
    extract_dataset(input_dir, output_file, manifest_file)
    original_bytes = output_file.read_bytes()

    make_hdf5_file(input_dir / "TR00002.h5", track_id="TR00002")

    def fail_to_parquet(self, *args, **kwargs):
        raise OSError("simulated write failure")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_to_parquet)

    with pytest.raises(OSError):
        extract_dataset(input_dir, output_file, manifest_file)

    assert output_file.read_bytes() == original_bytes


def test_normalizes_windows_absolute_path_under_configured_project_root() -> None:
    assert normalize_source_path(
        "D:/million-song-data-pipeline/data/raw/MillionSongSubset/A/A/A/TR00001.h5",
        project_root=r"D:\Million-Song-Data-Pipeline",
    ) == "data/raw/MillionSongSubset/A/A/A/TR00001.h5"


def test_normalizes_backslash_separated_path_under_project_root() -> None:
    assert normalize_source_path(
        r"D:\million-song-data-pipeline\data\raw\MillionSongSubset\A\A\A\TR00001.h5",
        project_root=r"D:\million-song-data-pipeline",
    ) == "data/raw/MillionSongSubset/A/A/A/TR00001.h5"


def test_normalizes_forward_slash_separated_path_under_project_root() -> None:
    assert normalize_source_path(
        "D:/million-song-data-pipeline/data/raw/MillionSongSubset/A/A/A/TR00001.h5",
        project_root="D:/million-song-data-pipeline",
    ) == "data/raw/MillionSongSubset/A/A/A/TR00001.h5"


def test_normalizes_already_relative_path() -> None:
    assert normalize_source_path("./data/raw/x.h5") == "data/raw/x.h5"


def test_path_outside_project_root_is_not_made_relative() -> None:
    assert normalize_source_path(
        r"C:\tmp\input.h5",
        project_root=r"D:\million-song-data-pipeline",
    ) == "C:/tmp/input.h5"


@pytest.mark.parametrize("column", ["track_id", "source_file"])
def test_required_columns_exist_in_schema(column: str) -> None:
    assert column in EXPECTED_COLUMNS
