
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
PARQUET_FILE = ROOT / "data" / "processed" / "songs.parquet"
WAREHOUSE_DIR = ROOT / "data" / "warehouse"
DB_FILE = WAREHOUSE_DIR / "music_analytics.duckdb"


def build_warehouse():
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_FILE))

    try:
        parquet_path = PARQUET_FILE.as_posix().replace("'", "''")

        con.execute(f"""
            CREATE OR REPLACE TABLE dim_song AS
            SELECT
                track_id,
                song_id,
                NULLIF(TRIM(title), '') AS title,
                artist_id,
                NULLIF(TRIM(artist_name), '') AS artist_name,
                NULLIF(TRIM(artist_location), '') AS artist_location,
                NULLIF(TRIM(release), '') AS release,
                NULLIF(year, 0) AS release_year,
                NULLIF(TRIM(genre), '') AS genre,
                duration_seconds,
                tempo,
                loudness,
                danceability,
                energy,
                key AS musical_key,
                mode AS musical_mode,
                time_signature,
                song_hotttnesss,
                artist_hotttnesss,
                artist_familiarity,
                source_file,
                ingested_at_utc
            FROM read_parquet('{parquet_path}')
        """)

        con.execute("""
            CREATE OR REPLACE TABLE dim_artist AS
            SELECT
                artist_id,
                MAX(artist_name) AS artist_name,
                MAX(artist_location) AS artist_location,
                COUNT(*) AS track_count,
                AVG(artist_hotttnesss) AS avg_artist_hotttnesss,
                AVG(artist_familiarity) AS avg_artist_familiarity
            FROM dim_song
            WHERE artist_id IS NOT NULL
              AND TRIM(artist_id) <> ''
            GROUP BY artist_id
        """)

        con.execute("""
            CREATE OR REPLACE TABLE fact_song_audio AS
            SELECT
                track_id,
                artist_id,
                release_year,
                duration_seconds,
                tempo,
                loudness,
                danceability,
                energy,
                musical_key,
                musical_mode,
                time_signature,
                song_hotttnesss
            FROM dim_song
        """)

        print("\nWarehouse build completed.")
        print(f"Database: {DB_FILE}")

        for table in ["dim_song", "dim_artist", "fact_song_audio"]:
            count = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            print(f"{table}: {count:,} rows")

    finally:
        con.close()


if __name__ == "__main__":
    build_warehouse()
