import duckdb

from dashboard.analytics import (
    deduplicated_artist_join,
    find_column,
    qualified_name,
    quote_identifier,
)


def test_quote_identifier_escapes_embedded_quotes() -> None:
    assert quote_identifier('artist"name') == '"artist""name"'
    assert qualified_name("analytics", 'dim"artists') == '"analytics"."dim""artists"'


def test_find_column_matches_names_without_case_sensitivity() -> None:
    assert find_column(["Track_ID", "Artist_Name"], "artist_name", "artist") == "Artist_Name"
    assert find_column(["Track_ID"], "missing") is None


def test_artist_join_deduplicates_dimension_keys_without_losing_fact_rows() -> None:
    with duckdb.connect(":memory:") as con:
        con.execute("CREATE SCHEMA analytics")
        con.execute(
            "CREATE TABLE analytics.dim_artists (artist_id VARCHAR, artist_name VARCHAR)"
        )
        con.execute(
            "CREATE TABLE analytics.songs (track_id VARCHAR, artist_id VARCHAR)"
        )
        con.execute(
            "INSERT INTO analytics.dim_artists VALUES "
            "('a', 'Artist'), ('a', 'Artist'), ('b', NULL), (NULL, 'Ignored')"
        )
        con.execute(
            "INSERT INTO analytics.songs VALUES "
            "('track-1', 'a'), ('track-2', 'b'), ('track-3', NULL), ('track-4', 'missing')"
        )

        join_sql = deduplicated_artist_join(
            "songs",
            "artist_id",
            (("analytics", "dim_artists"), "artist_id", "artist_name"),
        )
        joined_rows, unique_tracks = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT songs.track_id) "
            f"FROM analytics.songs AS songs{join_sql}"
        ).fetchone()
        result = con.execute(
            f"SELECT songs.track_id, artist_dim.artist_join_name "
            f"FROM analytics.songs AS songs{join_sql} ORDER BY songs.track_id"
        ).fetchall()

    assert joined_rows == unique_tracks == 4
    assert result == [
        ("track-1", "Artist"),
        ("track-2", None),
        ("track-3", None),
        ("track-4", None),
    ]
