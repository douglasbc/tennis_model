{{ config(
    materialized = 'table',
    schema = 'model_layer',
    partition_by = {
      "field": "match_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with wta_matches as (
  select * from {{ ref('wta_matches') }}
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
      when tournament_tier in ('WTA 250', 'WTA 500', 'WTA 1000', 'Grand Slam')
        then 'Main Tour'
      when tournament_tier in ('10k', '15k', '25k')
        then 'Lower Level'    
      else 'Second Level'
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
  from wta_matches
  where
    regexp_extract(result, r'([a-zA-Z]+)') is null
    -- and tournament_tier not in ('$10K', '$15K', '$25K', 'Fed Cup', 'Billie Jean King Cup')
    and tournament_tier not in ('Fed Cup', 'Billie Jean King Cup')
    and p1_service_points_won > 0
    and p1_service_points_played > 0
    and p2_service_points_won > 0
    and p2_service_points_played > 0
)

select * from final
