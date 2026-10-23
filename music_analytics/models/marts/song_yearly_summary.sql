{{ config(materialized='table') }}

SELECT
    release_year,
    COUNT(*) AS song_count,
    COUNT(DISTINCT artist_id) AS distinct_artist_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(tempo) AS avg_tempo,
    AVG(loudness) AS avg_loudness,
    AVG(danceability) AS avg_danceability,
    AVG(energy) AS avg_energy
FROM {{ ref('stg_songs') }}
WHERE release_year IS NOT NULL
GROUP BY release_year
