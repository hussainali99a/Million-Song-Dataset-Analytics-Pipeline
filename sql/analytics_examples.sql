
-- Million Song Dataset
-- Example analytical queries for DuckDB
-- Run against the database after the pipeline and dbt models succeed.

-- ============================================================
-- 1. Dataset overview
-- ============================================================

SELECT
    COUNT(*) AS total_songs,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT artist_id) AS unique_artists,
    COUNT(*) FILTER (WHERE year IS NULL OR year = 0)
        AS songs_without_valid_year
FROM analytics.fct_song_audio;


-- ============================================================
-- 2. Songs released by year
-- ============================================================

SELECT
    year,
    COUNT(*) AS song_count
FROM analytics.fct_song_audio
WHERE year > 0
GROUP BY year
ORDER BY song_count DESC, year DESC
LIMIT 20;


-- ============================================================
-- 3. Artists with the most songs in the dataset
-- ============================================================

SELECT
    artist_id,
    artist_name,
    COUNT(*) AS song_count
FROM analytics.fct_song_audio
WHERE artist_name IS NOT NULL
GROUP BY artist_id, artist_name
ORDER BY song_count DESC, artist_name
LIMIT 20;


-- ============================================================
-- 4. Highest-tempo songs
-- ============================================================

SELECT
    title,
    artist_name,
    year,
    tempo
FROM analytics.fct_song_audio
WHERE tempo IS NOT NULL
  AND tempo > 0
ORDER BY tempo DESC
LIMIT 20;


-- ============================================================
-- 5. Danceability and energy comparison
-- ============================================================

SELECT
    CASE
        WHEN danceability >= 0.7 THEN 'High danceability'
        WHEN danceability >= 0.4 THEN 'Medium danceability'
        ELSE 'Low danceability'
    END AS danceability_group,
    COUNT(*) AS song_count,
    ROUND(AVG(energy), 4) AS average_energy
FROM analytics.fct_song_audio
WHERE danceability IS NOT NULL
GROUP BY danceability_group
ORDER BY average_energy DESC NULLS LAST;


-- ============================================================
-- 6. Average audio features by release year
-- ============================================================

SELECT
    year,
    COUNT(*) AS song_count,
    ROUND(AVG(tempo), 2) AS average_tempo,
    ROUND(AVG(loudness), 2) AS average_loudness,
    ROUND(AVG(duration_seconds), 2) AS average_duration_seconds,
    ROUND(AVG(danceability), 4) AS average_danceability,
    ROUND(AVG(energy), 4) AS average_energy
FROM analytics.fct_song_audio
WHERE year > 0
GROUP BY year
HAVING COUNT(*) >= 10
ORDER BY year;


-- ============================================================
-- 7. Longest songs
-- ============================================================

SELECT
    title,
    artist_name,
    year,
    ROUND(duration_seconds, 2) AS duration_seconds
FROM analytics.fct_song_audio
WHERE duration_seconds IS NOT NULL
  AND duration_seconds > 0
ORDER BY duration_seconds DESC
LIMIT 20;


-- ============================================================
-- 8. Audio feature completeness
-- ============================================================

SELECT
    COUNT(*) AS total_rows,
    COUNT(tempo) AS rows_with_tempo,
    COUNT(loudness) AS rows_with_loudness,
    COUNT(danceability) AS rows_with_danceability,
    COUNT(energy) AS rows_with_energy,
    ROUND(100.0 * COUNT(tempo) / NULLIF(COUNT(*), 0), 2)
        AS tempo_completeness_pct,
    ROUND(100.0 * COUNT(energy) / NULLIF(COUNT(*), 0), 2)
        AS energy_completeness_pct
FROM analytics.fct_song_audio;


-- ============================================================
-- 9. Check for duplicate track IDs
-- Expected result: zero rows if track_id is unique.
-- ============================================================

SELECT
    track_id,
    COUNT(*) AS occurrences
FROM analytics.fct_song_audio
GROUP BY track_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;


-- ============================================================
-- 10. Inspect the yearly summary model
-- ============================================================

SELECT *
FROM analytics.song_yearly_summary
ORDER BY year
LIMIT 25;
