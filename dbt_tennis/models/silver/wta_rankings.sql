{{ config(
    materialized = 'table',
    schema = 'silver'
)}}

with

rankings_wta as (
  select
    player_id,
    ranking_date,
    ranking_position
  from {{ source('raw_layer', 'rankings_wta') }}
),

player_date_range AS (
  SELECT 
    player_id,
    MIN(ranking_date) AS min_date,
    MAX(ranking_date) AS max_date
  FROM rankings_wta
  GROUP BY player_id
),

all_weeks AS (
  SELECT
    player_id,
    GENERATE_DATE_ARRAY(min_date, max_date, INTERVAL 7 DAY) AS date_array
  FROM player_date_range
),

all_weeks_unnested AS (
  SELECT
    player_id,
    weekly_date AS ranking_date
  FROM all_weeks
  CROSS JOIN UNNEST(date_array) AS weekly_date
),

combined_data AS (
  SELECT
    a.player_id,
    a.ranking_date,
    COALESCE(r.ranking_position, 
             LAG(r.ranking_position) OVER (PARTITION BY a.player_id ORDER BY a.ranking_date)
            ) AS ranking_position
  FROM all_weeks_unnested a
  LEFT JOIN rankings_wta r
    ON a.player_id = r.player_id 
    AND a.ranking_date = r.ranking_date
)

SELECT 
  player_id,
  ranking_date,
  -- Final forward fill for initial NULLs
  LAST_VALUE(ranking_position IGNORE NULLS) OVER (
    PARTITION BY player_id 
    ORDER BY ranking_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS ranking_position
FROM combined_data
ORDER BY player_id, ranking_date