{{ config(
    materialized = 'table',
    schema = 'treated_layer',
)}}

with

matches_wta as (
  select * from {{ source('raw_layer', 'matches_wta') }}
),

player_last_match_won_date as (
  select
    player_1_id as player_id,
    max(match_date) as match_date
  from matches_wta
  where match_date is not null
  group by 1
),

player_last_match_lost_date as (
  select
    player_2_id as player_id,
    max(match_date) as match_date
  from matches_wta
  where match_date is not null
  group by 1
),

player_union as (
  select
    player_id,
    match_date
  from player_last_match_won_date

  union all

  select
    player_id,
    match_date
  from player_last_match_lost_date
),

player_last_match_date as (
  select
    player_id,
    max(match_date) as last_match_date
  from player_union
  group by 1
),

final as (
  select
    player_id,
    last_match_date,
    if(date_diff(current_date(), last_match_date, day) < 730, true, false) as active_last_2_years,
    true as active_since_2015
  from player_last_match_date
  where last_match_date is not null
)

select * from final
