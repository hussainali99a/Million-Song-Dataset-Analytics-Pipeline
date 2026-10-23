
SELECT
    track_id,
    duration_seconds,
    tempo,
    release_year
FROM {{ ref('stg_songs') }}
WHERE
    (duration_seconds IS NOT NULL AND duration_seconds < 0)
    OR (tempo IS NOT NULL AND tempo < 0)
    OR (release_year IS NOT NULL AND release_year < 0)
