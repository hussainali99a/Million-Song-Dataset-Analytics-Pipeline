from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.warehouse import build_warehouse


@pytest.fixture
def warehouse_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    parquet_file = tmp_path / "input" / "songs.parquet"
    warehouse_dir = tmp_path / "warehouse"
    database_file = warehouse_dir / "analytics.duckdb"

    monkeypatch.setattr(build_warehouse, "PARQUET_FILE", parquet_file)
    monkeypatch.setattr(build_warehouse, "WAREHOUSE_DIR", warehouse_dir)
    monkeypatch.setattr(build_warehouse, "DB_FILE", database_file)
    return parquet_file, database_file


@pytest.fixture
def synthetic_songs(warehouse_paths: tuple[Path, Path]) -> Path:
    parquet_file, _ = warehouse_paths
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "track_id": "TR1",
                "song_id": "SO1",
                "title": " First Song ",
                "artist_id": "AR1",
                "artist_name": " Artist One ",
                "artist_location": " New York ",
                "release": " Album ",
                "year": 0,
                "genre": " Rock ",
                "duration_seconds": 180.0,
                "tempo": 100.0,
                "loudness": -5.0,
                "danceability": 0.7,
                "energy": 0.8,
                "key": 1,
                "mode": 1,
                "time_signature": 4,
                "song_hotttnesss": 0.6,
                "artist_hotttnesss": 0.8,
                "artist_familiarity": 0.7,
                "source_file": "first.h5",
                "ingested_at_utc": "2026-01-01T00:00:00+00:00",
            },
            {
                "track_id": "TR2",
                "song_id": "SO2",
                "title": "",
                "artist_id": "AR1",
                "artist_name": "Artist One",
                "artist_location": "New York",
                "release": "Album",
                "year": 2001,
                "genre": "Jazz",
                "duration_seconds": 200.0,
                "tempo": 110.0,
                "loudness": -4.0,
                "danceability": 0.6,
                "energy": 0.9,
                "key": 2,
                "mode": 0,
                "time_signature": 3,
                "song_hotttnesss": 0.9,
                "artist_hotttnesss": 1.0,
                "artist_familiarity": 0.9,
                "source_file": "second.h5",
                "ingested_at_utc": "2026-01-02T00:00:00+00:00",
            },
            {
                "track_id": "TR3",
                "song_id": "SO3",
                "title": "Uncredited",
                "artist_id": "",
                "artist_name": "Unknown",
                "artist_location": "",
                "release": "Single",
                "year": 1999,
                "genre": "",
                "duration_seconds": 190.0,
                "tempo": 105.0,
                "loudness": -6.0,
                "danceability": 0.5,
                "energy": 0.5,
                "key": 3,
                "mode": 1,
                "time_signature": 4,
                "song_hotttnesss": 0.4,
                "artist_hotttnesss": 0.5,
                "artist_familiarity": 0.4,
                "source_file": "third.h5",
                "ingested_at_utc": "2026-01-03T00:00:00+00:00",
            },
            {
                "track_id": "TR4",
                "song_id": "SO4",
                "title": "No Artist Id",
                "artist_id": None,
                "artist_name": "Unknown",
                "artist_location": None,
                "release": "Single",
                "year": None,
                "genre": None,
                "duration_seconds": 210.0,
                "tempo": 120.0,
                "loudness": -7.0,
                "danceability": 0.4,
                "energy": 0.6,
                "key": 4,
                "mode": 0,
                "time_signature": 4,
                "song_hotttnesss": 0.3,
                "artist_hotttnesss": None,
                "artist_familiarity": None,
                "source_file": "fourth.h5",
                "ingested_at_utc": "2026-01-04T00:00:00+00:00",
            },
        ]
    ).to_parquet(parquet_file, index=False)
    return parquet_file


def test_builds_expected_warehouse_tables_and_rows(
    synthetic_songs: Path,
    warehouse_paths: tuple[Path, Path],
) -> None:
    _, database_file = warehouse_paths

    build_warehouse.build_warehouse()

    assert database_file.is_file()
    with duckdb.connect(str(database_file), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute("SHOW TABLES").fetchall()
        }
        assert table_names == {"dim_song", "dim_artist", "fact_song_audio"}

        assert connection.execute("SELECT COUNT(*) FROM dim_song").fetchone() == (4,)
        assert connection.execute("SELECT COUNT(*) FROM dim_artist").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM fact_song_audio").fetchone() == (4,)

        song = connection.execute(
            """
            SELECT title, release_year, artist_name, artist_location, genre, musical_key
            FROM dim_song
            WHERE track_id = 'TR1'
            """
        ).fetchone()
        assert song == (
            "First Song",
            None,
            "Artist One",
            "New York",
            "Rock",
            1,
        )

        assert connection.execute(
            """
            SELECT artist_id, track_count, avg_artist_hotttnesss, avg_artist_familiarity
            FROM dim_artist
            """
        ).fetchone() == ("AR1", 2, 0.9, 0.8)

        assert connection.execute(
            "SELECT track_id, release_year, musical_mode FROM fact_song_audio "
            "WHERE track_id = 'TR1'"
        ).fetchone() == ("TR1", None, 1)

    build_warehouse.build_warehouse()
    with duckdb.connect(str(database_file), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM dim_song").fetchone() == (4,)


def test_missing_parquet_raises_without_creating_warehouse(
    warehouse_paths: tuple[Path, Path],
) -> None:
    _, database_file = warehouse_paths

    with pytest.raises(FileNotFoundError, match="Parquet file not found"):
        build_warehouse.build_warehouse()

    assert not database_file.parent.exists()
    assert not database_file.exists()


def test_invalid_parquet_surfaces_duckdb_error(
    warehouse_paths: tuple[Path, Path],
) -> None:
    parquet_file, database_file = warehouse_paths
    parquet_file.parent.mkdir(parents=True)
    parquet_file.write_text("not a parquet file", encoding="utf-8")

    with pytest.raises(duckdb.Error):
        build_warehouse.build_warehouse()

    assert database_file.is_file()
