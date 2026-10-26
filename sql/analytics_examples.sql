-- Million Song Dataset analytics examples.
-- Run from DuckDB after the dbt models have been built.

-- 1. Dataset size, track uniqueness, and artist coverage.
SELECT
    COUNT(*) AS song_rows,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT artist_id) AS unique_artists,
    COUNT(*) FILTER (WHERE release_year IS NULL OR release_year <= 0)
        AS songs_without_valid_year
FROM analytics.fct_song_audio;

-- 2. Songs by valid release year.
SELECT
    release_year,
    COUNT(*) AS song_count
FROM analytics.fct_song_audio
WHERE release_year > 0
GROUP BY release_year
ORDER BY release_year;

-- 3. Top artists. Aggregate the dimension to one row per key before joining.
SELECT
    artists.artist_name,
    COUNT(*) AS song_count
FROM analytics.fct_song_audio AS songs
JOIN (
    SELECT
        artist_id,
        MAX(NULLIF(TRIM(artist_name), '')) AS artist_name
    FROM analytics.dim_artists
    WHERE artist_id IS NOT NULL
    GROUP BY artist_id
) AS artists
    ON songs.artist_id = artists.artist_id
WHERE songs.artist_id IS NOT NULL
  AND artists.artist_name IS NOT NULL
GROUP BY songs.artist_id, artists.artist_name
ORDER BY song_count DESC, artists.artist_name
LIMIT 15;

-- 4. Highest-tempo songs with artist names from the dimension.
SELECT
    songs.title,
    artists.artist_name,
    songs.release_year,
    songs.tempo
FROM analytics.fct_song_audio AS songs
LEFT JOIN (
    SELECT
        artist_id,
        MAX(NULLIF(TRIM(artist_name), '')) AS artist_name
    FROM analytics.dim_artists
    WHERE artist_id IS NOT NULL
    GROUP BY artist_id
) AS artists
    ON songs.artist_id = artists.artist_id
WHERE songs.tempo IS NOT NULL
  AND songs.tempo > 0
ORDER BY songs.tempo DESC
LIMIT 20;

-- 5. Tempo distribution summary.
SELECT
    COUNT(tempo) AS songs_with_tempo,
    ROUND(MIN(tempo), 2) AS minimum_tempo,
    ROUND(AVG(tempo), 2) AS average_tempo,
    ROUND(MEDIAN(tempo), 2) AS median_tempo,
    ROUND(MAX(tempo), 2) AS maximum_tempo
FROM analytics.fct_song_audio;

-- 6. Average audio features by release year.
SELECT
    release_year,
    COUNT(*) AS song_count,
    ROUND(AVG(tempo), 2) AS average_tempo,
    ROUND(AVG(loudness), 2) AS average_loudness,
    ROUND(AVG(duration_seconds), 2) AS average_duration_seconds,
    ROUND(AVG(danceability), 4) AS average_danceability,
    ROUND(AVG(energy), 4) AS average_energy
FROM analytics.fct_song_audio
WHERE release_year > 0
GROUP BY release_year
ORDER BY release_year;

-- 7. Longest songs, with artist details from the dimension.
SELECT
    songs.title,
    artists.artist_name,
    songs.release_year,
    ROUND(songs.duration_seconds, 2) AS duration_seconds
FROM analytics.fct_song_audio AS songs
LEFT JOIN (
    SELECT
        artist_id,
        MAX(NULLIF(TRIM(artist_name), '')) AS artist_name
    FROM analytics.dim_artists
    WHERE artist_id IS NOT NULL
    GROUP BY artist_id
) AS artists
    ON songs.artist_id = artists.artist_id
WHERE songs.duration_seconds IS NOT NULL
  AND songs.duration_seconds > 0
ORDER BY songs.duration_seconds DESC
LIMIT 20;

-- 8. Audio-feature completeness.
SELECT
    COUNT(*) AS total_rows,
    COUNT(tempo) AS rows_with_tempo,
    COUNT(loudness) AS rows_with_loudness,
    COUNT(danceability) AS rows_with_danceability,
    COUNT(energy) AS rows_with_energy,
    COUNT(duration_seconds) AS rows_with_duration,
    ROUND(100.0 * COUNT(tempo) / NULLIF(COUNT(*), 0), 2) AS tempo_completeness_pct,
    ROUND(100.0 * COUNT(energy) / NULLIF(COUNT(*), 0), 2) AS energy_completeness_pct
FROM analytics.fct_song_audio;

-- 9. Missing values by column.
SELECT
    COUNT(*) FILTER (WHERE track_id IS NULL) AS missing_track_ids,
    COUNT(*) FILTER (WHERE artist_id IS NULL) AS missing_artist_ids,
    COUNT(*) FILTER (WHERE title IS NULL) AS missing_titles,
    COUNT(*) FILTER (WHERE release_year IS NULL) AS missing_release_years
FROM analytics.fct_song_audio;

-- 10. Duplicate track IDs. Expected result: zero rows.
SELECT
    track_id,
    COUNT(*) AS occurrences
FROM analytics.fct_song_audio
GROUP BY track_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 11. Yearly summary model.
SELECT
    release_year,
    song_count,
    distinct_artist_count,
    avg_duration_seconds,
    avg_tempo,
    avg_loudness,
    avg_danceability,
    avg_energy
FROM analytics.song_yearly_summary
ORDER BY release_year
LIMIT 25;
