
from pathlib import Path

import duckdb
import pytest


PARQUET_FILE = Path("data/processed/songs.parquet")


@pytest.fixture(scope="module")
def songs():
    if not PARQUET_FILE.exists():
        pytest.fail(f"Parquet file not found: {PARQUET_FILE}")

    return duckdb.sql(
        "SELECT * FROM read_parquet(?)",
        params=[str(PARQUET_FILE)],
    ).df()


def test_parquet_has_expected_number_of_songs(songs):
    assert len(songs) == 10_000


def test_track_ids_are_present(songs):
    assert songs["track_id"].notna().all()
    assert (songs["track_id"].str.len() > 0).all()


def test_track_ids_are_unique(songs):
    assert songs["track_id"].is_unique


def test_song_durations_are_positive(songs):
    assert (songs["duration_seconds"].dropna() > 0).all()


def test_release_years_are_valid_or_unknown(songs):
    years = songs["year"].dropna()

    # Zero is the source's unknown-year sentinel.
    known_years = years[years != 0]
    assert known_years.between(1900, 2020).all()


def test_audio_features_are_finite_when_present(songs):
    for column in ["tempo", "loudness", "danceability", "energy"]:
        values = songs[column].dropna()
        assert values.map(lambda value: abs(value) != float("inf")).all()
