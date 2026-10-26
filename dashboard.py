from __future__ import annotations

"""Local Streamlit dashboard for the Million Song Dataset DuckDB warehouse.

Run from the repository root:
    python -m streamlit run dashboard.py
"""

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.analytics import deduplicated_artist_join
from dashboard.analytics import find_column as find_col
from dashboard.analytics import qualified_name as qname
from dashboard.analytics import quote_identifier as qi

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "warehouse" / "music_analytics.duckdb"
ARTIST_KEY_NAMES = ("artist_id", "artist_key", "artist_sk")

st.set_page_config(page_title="Million Song Dataset Analytics", page_icon="🎵", layout="wide")
st.title("🎵 Million Song Dataset Analytics")
st.caption("A local analytics dashboard powered by DuckDB, dbt, and Streamlit.")


@st.cache_data(show_spinner=False)
def get_tables(db_path: str) -> list[tuple[str, str]]:
    with duckdb.connect(db_path, read_only=True) as con:
        rows = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchall()
    return [(str(schema), str(table)) for schema, table in rows]


@st.cache_data(show_spinner=False)
def get_columns(db_path: str, schema: str, table: str) -> list[str]:
    with duckdb.connect(db_path, read_only=True) as con:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
    return [str(row[0]) for row in rows]


def metric(label: str, value: Any) -> None:
    if value is None or pd.isna(value):
        st.metric(label, "N/A")
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        st.metric(label, f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}")
    else:
        st.metric(label, str(value))


def artist_dimension(
    db_path: str,
    tables: list[tuple[str, str]],
) -> tuple[tuple[str, str], str, str] | None:
    dimension = next(
        ((schema, table) for schema, table in tables if table.lower() == "dim_artists"),
        None,
    )
    if dimension is None:
        return None
    columns = get_columns(db_path, *dimension)
    key = find_col(columns, *ARTIST_KEY_NAMES)
    name = find_col(columns, "artist_name", "artist")
    if key is None or name is None:
        return None
    return dimension, key, name


def artist_chart_data(
    db_path: str,
    selected_table: tuple[str, str],
    artist_key: str | None,
    source_artist_name: str | None,
    dimension: tuple[tuple[str, str], str, str] | None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    schema, table = selected_table
    from_clause = f"FROM {qname(schema, table)} AS songs"
    if artist_key and dimension:
        from_clause += deduplicated_artist_join("songs", artist_key, dimension)
        name_expression = "artist_dim.artist_join_name"
        key_expression = (
            f"NULLIF(TRIM(CAST(songs.{qi(artist_key)} AS VARCHAR)), '')"
        )
    elif source_artist_name:
        name_expression = f"NULLIF(TRIM(CAST(songs.{qi(source_artist_name)} AS VARCHAR)), '')"
        key_expression = (
            f"NULLIF(TRIM(CAST(songs.{qi(artist_key)} AS VARCHAR)), '')"
            if artist_key
            else "TRUE"
        )
    else:
        return pd.DataFrame(columns=["Artist", "Song count"]), {}

    null_key_count = (
        f"COUNT(*) FILTER (WHERE {key_expression} IS NULL)"
        if artist_key
        else "0"
    )
    missing_name_count = (
        f"COUNT(*) FILTER (WHERE {key_expression} IS NOT NULL "
        f"AND {name_expression} IS NULL)"
    )
    with duckdb.connect(db_path, read_only=True) as con:
        chart = con.execute(
            f"""
            SELECT {name_expression} AS artist, COUNT(*) AS song_count
            {from_clause}
            WHERE {key_expression} {'IS NOT NULL' if artist_key else ''}
              AND {name_expression} IS NOT NULL
            GROUP BY {key_expression}, artist
            ORDER BY song_count DESC, artist
            LIMIT 15
            """
        ).fetchdf()
        coverage = con.execute(
            f"""
            SELECT
                {null_key_count} AS null_artist_ids,
                {missing_name_count} AS missing_artist_names
            {from_clause}
            """
        ).fetchone()
    chart = chart.rename(columns={"artist": "Artist", "song_count": "Song count"})
    return chart, {
        "null_artist_ids": int(coverage[0]),
        "missing_artist_names": int(coverage[1]),
    }


def scalar_query(db_path: str, query: str) -> tuple[Any, ...]:
    with duckdb.connect(db_path, read_only=True) as con:
        row = con.execute(query).fetchone()
    return row if row is not None else ()


if not DB_PATH.exists():
    st.error(f"DuckDB warehouse not found: `{DB_PATH}`")
    st.info("Run your project pipeline first to create the local warehouse.")
    st.stop()

try:
    tables = get_tables(str(DB_PATH))
except (duckdb.Error, OSError) as exc:
    st.error(f"Could not open the warehouse: {exc}")
    st.stop()

if not tables:
    st.warning("No tables or views were found in the warehouse.")
    st.stop()

options = [f"{schema}.{table}" for schema, table in tables]
preferred = "analytics.fct_song_audio"
default_index = options.index(preferred) if preferred in options else 0

with st.sidebar:
    st.header("Dashboard settings")
    st.caption(f"Database: `{DB_PATH.relative_to(ROOT)}`")
    selected_label = st.selectbox("Song-level table", options, index=default_index)
    if st.button("Refresh dashboard data"):
        st.cache_data.clear()
        st.rerun()

selected_table = tables[options.index(selected_label)]
schema, table = selected_table
try:
    cols = get_columns(str(DB_PATH), schema, table)
except (duckdb.Error, OSError) as exc:
    st.error(f"Could not inspect `{selected_label}`: {exc}")
    st.stop()

if not cols:
    st.warning(f"`{selected_label}` has no discoverable columns.")
    st.stop()

track = find_col(cols, "track_id", "song_id")
artist_id = find_col(cols, *ARTIST_KEY_NAMES)
artist = find_col(cols, "artist_name", "artist")
title = find_col(cols, "title", "song_title", "name")
year = find_col(cols, "year", "release_year")
audio_features = {
    label: find_col(cols, *names)
    for label, names in (
        ("Tempo", ("tempo",)),
        ("Energy", ("energy",)),
        ("Danceability", ("danceability",)),
        ("Loudness", ("loudness",)),
        ("Duration", ("duration_seconds", "duration")),
        ("Song hotness", ("song_hotttnesss", "song_hotness")),
    )
}
audio_features = {label: column for label, column in audio_features.items() if column}
dimension = artist_dimension(str(DB_PATH), tables)

table_name = qname(schema, table)
aggregates = ["COUNT(*) AS row_count"]
aggregates.append(
    f"COUNT(DISTINCT {qi(track)}) AS unique_tracks" if track else "NULL AS unique_tracks"
)
aggregates.append(
    f"COUNT(DISTINCT {qi(artist_id)}) AS unique_artists"
    if artist_id
    else "NULL AS unique_artists"
)
aggregates.append(
    f"COUNT(DISTINCT TRY_CAST({qi(year)} AS BIGINT)) "
    f"FILTER (WHERE TRY_CAST({qi(year)} AS BIGINT) > 0) AS valid_years"
    if year
    else "NULL AS valid_years"
)
try:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        stats = con.execute(f"SELECT {', '.join(aggregates)} FROM {table_name}").fetchone()
except (duckdb.Error, OSError) as exc:
    st.error(f"Could not query `{selected_label}`: {exc}")
    st.stop()

row_count, unique_tracks, unique_artists, valid_years = stats
if row_count == 0:
    st.warning(f"`{selected_label}` exists but contains no rows.")
    st.stop()

st.subheader("Dataset overview")
kpis = st.columns(4)
with kpis[0]:
    metric("Rows in selected table", row_count)
with kpis[1]:
    metric("Unique tracks", unique_tracks)
with kpis[2]:
    metric("Unique artists", unique_artists)
with kpis[3]:
    metric("Distinct valid years", valid_years)

if year:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        yearly = con.execute(
            f"""
            SELECT TRY_CAST({qi(year)} AS BIGINT) AS Year, COUNT(*) AS "Song count"
            FROM {table_name}
            WHERE TRY_CAST({qi(year)} AS BIGINT) > 0
            GROUP BY Year
            ORDER BY Year
            """
        ).fetchdf()
    if not yearly.empty:
        st.subheader("Songs by release year")
        st.plotly_chart(
            px.line(yearly, x="Year", y="Song count", markers=True,
                    title="Number of songs per release year"),
            width="stretch",
        )

left, right = st.columns(2)
with left:
    st.subheader("Artists with the most records")
    if artist or (artist_id and dimension):
        try:
            top_artists, artist_coverage = artist_chart_data(
                str(DB_PATH), selected_table, artist_id, artist, dimension
            )
        except (duckdb.Error, OSError) as exc:
            st.error(f"Could not build the artist chart: {exc}")
        else:
            if not top_artists.empty:
                st.plotly_chart(
                    px.bar(
                        top_artists.sort_values("Song count"),
                        x="Song count",
                        y="Artist",
                        orientation="h",
                        title="Top 15 artists by song records",
                    ),
                    width="stretch",
                )
            else:
                st.info("No matched artist names were found for the selected song records.")
            st.caption(
                f"Excluded from chart: {artist_coverage['null_artist_ids']:,} null artist IDs "
                f"and {artist_coverage['missing_artist_names']:,} unmatched or unnamed artists."
            )
    else:
        st.info("No compatible artist ID/name columns or artist dimension were found.")

with right:
    st.subheader("Audio feature distributions")
    if audio_features:
        feature_label = st.selectbox("Choose an audio feature", list(audio_features))
        feature_column = audio_features[feature_label]
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            feature_stats = con.execute(
                f"""
                SELECT
                    COUNT(value),
                    AVG(value),
                    MEDIAN(value),
                    MIN(value),
                    MAX(value)
                FROM (
                    SELECT TRY_CAST({qi(feature_column)} AS DOUBLE) AS value
                    FROM {table_name}
                )
                """
            ).fetchone()
            value_count, mean, median, minimum, maximum = feature_stats
            if value_count:
                histogram = con.execute(
                    f"""
                    WITH numeric_values AS (
                        SELECT TRY_CAST({qi(feature_column)} AS DOUBLE) AS value
                        FROM {table_name}
                        WHERE TRY_CAST({qi(feature_column)} AS DOUBLE) IS NOT NULL
                    ),
                    bounds AS (
                        SELECT MIN(value) AS min_value, MAX(value) AS max_value
                        FROM numeric_values
                    )
                    SELECT
                        CASE WHEN bounds.min_value = bounds.max_value THEN bounds.min_value
                             ELSE bounds.min_value
                                  + (bounds.max_value - bounds.min_value)
                                  * (LEAST(34, GREATEST(0, CAST(FLOOR(
                                      (numeric_values.value - bounds.min_value)
                                      / (bounds.max_value - bounds.min_value) * 35
                                  ) AS INTEGER))) + 0.5) / 35
                        END AS "Bin center",
                        COUNT(*) AS "Record count"
                    FROM numeric_values CROSS JOIN bounds
                    GROUP BY "Bin center"
                    ORDER BY "Bin center"
                    """
                ).fetchdf()
        if value_count:
            st.plotly_chart(
                px.bar(
                    histogram,
                    x="Bin center",
                    y="Record count",
                    title=f"Distribution of {feature_label.lower()}",
                ),
                width="stretch",
            )
            st.caption(
                f"Non-null records: {value_count:,} · Mean: {mean:,.2f} · "
                f"Median: {median:,.2f}"
            )
        else:
            st.info("No numeric values available for this feature.")
    else:
        st.info("No recognized audio feature columns were found.")

st.subheader("Data quality snapshot")
quality = st.columns(3)
with quality[0]:
    duplicate_count = (
        scalar_query(
            str(DB_PATH),
            f"SELECT COUNT(*) - COUNT(DISTINCT {qi(track)}) FROM {table_name}",
        )[0]
        if track
        else None
    )
    metric("Duplicate track IDs", duplicate_count)

with quality[1]:
    missing_artist_count: int | None = None
    if artist_id and dimension:
        artist_join = deduplicated_artist_join("songs", artist_id, dimension)
        missing_artist_count = scalar_query(
            str(DB_PATH),
            f"""
            SELECT COUNT(*) FILTER (
                WHERE artist_dim.artist_join_name IS NULL
            )
            FROM {table_name} AS songs{artist_join}
            """,
        )[0]
    elif artist:
        missing_artist_count = scalar_query(
            str(DB_PATH),
            f"SELECT COUNT(*) FILTER (WHERE {qi(artist)} IS NULL "
            f"OR TRIM(CAST({qi(artist)} AS VARCHAR)) = '') FROM {table_name}",
        )[0]
    metric("Missing artist names", missing_artist_count)

with quality[2]:
    missing_year_count = (
        scalar_query(
            str(DB_PATH),
            f"SELECT COUNT(*) FILTER (WHERE {qi(year)} IS NULL) FROM {table_name}",
        )[0]
        if year
        else None
    )
    metric("Missing years", missing_year_count)

st.subheader("Explore song records")
search = st.text_input("Filter by title or artist", placeholder="Type a song or artist name")
display_artist = artist
records_from = f"FROM {table_name} AS songs"
if artist_id and dimension:
    records_from += deduplicated_artist_join("songs", artist_id, dimension)
    display_artist = None
    artist_expression = "artist_dim.artist_join_name"
else:
    artist_expression = f"songs.{qi(artist)}" if artist else None

selected_fields: list[str] = []
for column in (track, title, year, *(audio_features.values())):
    if column and column not in selected_fields:
        selected_fields.append(column)
select_expressions = [f"songs.{qi(column)} AS {qi(column)}" for column in selected_fields]
if artist_expression:
    select_expressions.append(f"{artist_expression} AS {qi('artist_name')}")
search_expressions = [
    f"contains(LOWER(COALESCE(CAST(songs.{qi(column)} AS VARCHAR), '')), LOWER(?))"
    for column in (title, display_artist)
    if column
]
if artist_id and dimension and artist_expression:
    search_expressions.append(
        "contains(LOWER(COALESCE(CAST(artist_dim.artist_join_name AS VARCHAR), '')), LOWER(?))"
    )

where_clause = ""
parameters: list[str] = []
if search.strip() and search_expressions:
    where_clause = " WHERE (" + " OR ".join(search_expressions) + ")"
    parameters = [search.strip()] * len(search_expressions)
elif search.strip():
    st.info("The selected table has no recognized title or artist column to search.")

if not select_expressions:
    selected_fields = cols[:10]
    select_expressions = [f"songs.{qi(column)} AS {qi(column)}" for column in selected_fields]
if artist_expression and "artist_name" not in selected_fields:
    display_columns = selected_fields + ["artist_name"]
else:
    display_columns = selected_fields

try:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        records = con.execute(
            f"SELECT {', '.join(select_expressions)} {records_from}{where_clause} LIMIT 500",
            parameters,
        ).fetchdf()
except (duckdb.Error, OSError) as exc:
    st.error(f"Could not search `{selected_label}`: {exc}")
    st.stop()

st.dataframe(records[display_columns], width="stretch", hide_index=True)
st.caption(
    "Showing up to 500 matching rows. Charts and metrics are aggregated in DuckDB "
    "from the selected local table."
)
