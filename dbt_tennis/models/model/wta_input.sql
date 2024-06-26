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

filtered_matches as (
  select * from wta_matches
  where
    regexp_extract(result, r'([a-zA-Z]+)') is null
    and p1_service_points_won > 0
    and p1_service_points_played > 0
    and p2_service_points_won > 0
    and p2_service_points_played > 0
),

not_newbies as (
  select
    player,
    count(1) as matches_count
  from 
    (select p1_name as player
    from filtered_matches
    union all
    select p2_name as player
    from filtered_matches)
  group by player
  having matches_count >= 10
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
    tournament_tier,
    case
      when round in ('Pre Qualifying',
                     'Qualifying First Round',
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
        as margins,
    p1_win_match_odds as p1_odds,
    p2_win_match_odds as p2_odds
  from filtered_matches
  where
    p1_name in (select player from not_newbies)
    and p2_name in (select player from not_newbies)
)

select * from final
