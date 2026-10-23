
-- 1. Top 10 artists by number of tracks
SELECT artist_name, track_count
FROM dim_artist
ORDER BY track_count DESC, artist_name
LIMIT 10;

-- 2. Songs released by year
SELECT release_year, COUNT(*) AS song_count
FROM dim_song
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;

-- 3. Average audio characteristics
SELECT
    ROUND(AVG(tempo), 2) AS avg_tempo,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_seconds,
    ROUND(AVG(NULLIF(danceability, 0)), 4) AS avg_nonzero_danceability,
    ROUND(AVG(NULLIF(energy, 0)), 4) AS avg_nonzero_energy
FROM dim_song;

-- 4. Missing or unknown release years
SELECT COUNT(*) AS unknown_year_songs
FROM dim_song
WHERE release_year IS NULL;
