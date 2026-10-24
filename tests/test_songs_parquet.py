import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_PARQUET_FILE = PROJECT_ROOT / "data" / "processed" / "songs.parquet"
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
AUDIO_FEATURE_COLUMNS = ["tempo", "loudness", "danceability", "energy"]


@pytest.fixture
def songs(tmp_path: Path) -> pd.DataFrame:
    parquet_file = tmp_path / "songs.parquet"
    records = [
        {
            "track_id": f"TR{index:05}",
            "song_id": f"SO{index:05}",
            "title": f"Song {index}",
            "artist_id": f"AR{index:05}",
            "artist_name": f"Artist {index}",
            "artist_location": "London",
            "artist_latitude": 51.5,
            "artist_longitude": -0.1,
            "release": "Synthetic release",
            "year": year,
            "genre": "Synthetic",
            "duration_seconds": 180.0 + index,
            "tempo": 100.0 + index,
            "loudness": -6.0 - index,
            "danceability": 0.5 + index / 10,
            "energy": 0.6 + index / 10,
            "key": 1,
            "mode": 1,
            "time_signature": 4,
            "song_hotttnesss": 0.7,
            "artist_hotttnesss": 0.8,
            "artist_familiarity": 0.9,
            "source_file": f"data/raw/synthetic/{index}.h5",
            "ingested_at_utc": "2026-01-01T00:00:00+00:00",
        }
        for index, year in enumerate([0, 2000, 2020], start=1)
    ]
    pd.DataFrame(records, columns=EXPECTED_COLUMNS).to_parquet(
        parquet_file,
        index=False,
    )
    return duckdb.sql(
        "SELECT * FROM read_parquet(?)",
        params=[str(parquet_file)],
    ).df()


def test_synthetic_parquet_fixture_has_expected_rows(songs: pd.DataFrame) -> None:
    assert len(songs) == 3


def test_synthetic_parquet_has_expected_schema(songs: pd.DataFrame) -> None:
    assert songs.columns.tolist() == EXPECTED_COLUMNS


def test_track_ids_are_present(songs: pd.DataFrame) -> None:
    assert songs["track_id"].notna().all()
    assert (songs["track_id"].str.len() > 0).all()


def test_track_ids_are_unique(songs: pd.DataFrame) -> None:
    assert songs["track_id"].is_unique


def test_song_durations_are_positive(songs: pd.DataFrame) -> None:
    assert (songs["duration_seconds"].dropna() > 0).all()


def test_release_years_are_valid_or_unknown(songs: pd.DataFrame) -> None:
    years = songs["year"].dropna()
    known_years = years[years != 0]
    assert known_years.between(1900, 2020).all()


def test_audio_features_are_finite_when_present(songs: pd.DataFrame) -> None:
    for column in AUDIO_FEATURE_COLUMNS:
        values = songs[column].dropna().to_numpy()
        assert np.isfinite(values).all()


@pytest.mark.integration
def test_real_dataset_quality_when_opted_in() -> None:
    if os.environ.get("RUN_DATASET_INTEGRATION") != "1":
        pytest.skip("Set RUN_DATASET_INTEGRATION=1 to validate the local real dataset.")
    if not REAL_PARQUET_FILE.is_file():
        pytest.skip(f"Real dataset is unavailable: {REAL_PARQUET_FILE}")

    songs = duckdb.sql(
        "SELECT * FROM read_parquet(?)",
        params=[str(REAL_PARQUET_FILE)],
    ).df()
    assert len(songs) == 10_000
    assert songs.columns.tolist() == EXPECTED_COLUMNS
    assert songs["track_id"].notna().all()
    assert (songs["track_id"].str.len() > 0).all()
    assert songs["track_id"].is_unique
    assert songs["source_file"].notna().all()
    assert songs["source_file"].is_unique
    assert (songs["duration_seconds"].dropna() > 0).all()
    years = songs["year"].dropna()
    assert years[years != 0].between(1900, 2020).all()
    for column in AUDIO_FEATURE_COLUMNS:
        assert np.isfinite(songs[column].dropna().to_numpy()).all()
