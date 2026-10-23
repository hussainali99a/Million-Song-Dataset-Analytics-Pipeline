SELECT
    NULLIF(TRIM(track_id), '') AS track_id,
    NULLIF(TRIM(song_id), '') AS song_id,
    NULLIF(TRIM(title), '') AS title,
    NULLIF(TRIM(artist_id), '') AS artist_id,
    NULLIF(TRIM(artist_name), '') AS artist_name,
    NULLIF(TRIM(artist_location), '') AS artist_location,
    NULLIF(TRIM(release), '') AS release,
    CASE
        WHEN year BETWEEN 1900 AND 2020 THEN year
        ELSE NULL
    END AS release_year,
    NULLIF(TRIM(genre), '') AS genre,
    duration_seconds,
    tempo,
    loudness,
    danceability,
    energy,
    key,
    mode,
    time_signature,
    song_hotttnesss,
    artist_hotttnesss,
    artist_familiarity,
    source_file,
    ingested_at_utc
FROM read_parquet('D:/million-song-data-pipeline/data/processed/songs.parquet')
