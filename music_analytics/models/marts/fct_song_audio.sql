{{ config(materialized='table') }}

SELECT
    track_id,
    song_id,
    artist_id,
    title,
    release_year,
    duration_seconds,
    tempo,
    loudness,
    danceability,
    energy,
    key,
    mode,
    time_signature,
    song_hotttnesss
FROM {{ ref('stg_songs') }}
WHERE track_id IS NOT NULL
