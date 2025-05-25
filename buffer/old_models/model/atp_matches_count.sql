{{ config(
    materialized = 'table',
    schema = 'model_layer',
)}}

with 

atp_input as (
  select * from {{ ref('atp_input') }}
),

player_matches_year as (
  select
    p1_name as player
  from atp_input
  where
    match_date >= date_sub(current_date('America/Sao_Paulo'), interval 1 year)
  union all
  select
    p2_name as player
  from atp_input
  where
    match_date >= date_sub(current_date('America/Sao_Paulo'), interval 1 year)
),

player_matches_quarter as (
  select
    p1_name as player
  from atp_input
  where
    match_date >= date_sub(current_date('America/Sao_Paulo'), interval 90 day)
  union all
  select
    p2_name as player
  from atp_input
  where
    match_date >= date_sub(current_date('America/Sao_Paulo'), interval 90 day)
),

player_group_year as (
  select
    player,
    count(1) as matches_past_year
  from player_matches_year
  group by player
),

player_group_quarter as (
  select
    player,
    count(1) as matches_past_quarter
  from player_matches_quarter
  group by player
),

final as (
  select
    y.player,
    y.matches_past_year,
    q.matches_past_quarter
  from player_group_year as y
    left join player_group_quarter as q
      on y.player = q.player
)

select * from final
