SELECT COUNT(*) AS actual_count
FROM {{ ref('stg_songs') }}
HAVING COUNT(*) = 0