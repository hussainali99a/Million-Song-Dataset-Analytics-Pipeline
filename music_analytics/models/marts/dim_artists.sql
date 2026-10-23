{{ config(materialized='table') }}

SELECT
    artist_id,
    MAX(artist_name) AS artist_name,
    MAX(artist_location) AS artist_location,
    COUNT(*) AS track_count,
    AVG(artist_hotttnesss) AS avg_artist_hotness,
    AVG(artist_familiarity) AS avg_artist_familiarity
FROM {{ ref('stg_songs') }}
WHERE artist_id IS NOT NULL
GROUP BY artist_id
