{{ config(
    materialized = 'table',
    schema = 'model_layer',
    partition_by = {
      "field": "match_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with atp_matches as (
  select * from {{ ref('atp_matches') }}
),

final as (    

  select
    match_date,
    p1_name,
    p2_name,
    case
      when surface = 'Carpet' then 'Indoor Hard'
      else surface
    end as surface,
    case
      when tournament_tier in ('ATP 250', 'ATP 500', 'ATP Cup', 'ATP Finals', 'Masters 1000', 'Grand Slam')
        then 'Main Tour'
      else tournament_tier
    end as tournament_level,
    case
      when round in ('Qualifying First Round',
                     'Qualifying Second Round',
                     'Qualifying Final Round')
        then 1
      when round in ('First Round', 'Round Robin') then 2
      when round = 'Second Round' then 3
      when round = 'Third Round' then 4
      when round = 'Fourth Round' then 5
      when round = 'Quarter-Finals' then 6
      when round in ('Semi-Finals', 'Bronze Match') then 7
      when round = 'Final' then 8
    end as round,
    (p1_service_points_won/p1_service_points_played)
      - (p2_service_points_won/ p2_service_points_played)
        as margins
  from atp_matches
  where
    regexp_extract(result, r'([a-zA-Z]+)') is null
    -- and tournament_tier not in ('Future', 'David Cup', 'Laver Cup', 'Next Gen ATP Finals')
    and tournament_tier not in ('David Cup', 'Laver Cup', 'Next Gen ATP Finals')
    and p1_service_points_won > 0
    and p1_service_points_played > 0
    and p2_service_points_won > 0
    and p2_service_points_played > 0
)

select * from final
