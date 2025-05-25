{{ config(
    materialized = 'table',
    schema = 'model_layer',
)}}

with 

pinnacle_odds as (
  select * from {{ source('raw_layer', 'pinnacle_odds') }}
),

fix_player_names as (
  select * from {{ source('raw_layer', 'fix_player_names') }}
),

atp_predictions as (
  select * from {{ source('raw_layer', 'atp_predictions') }}
),

wta_predictions as (
  select * from {{ source('raw_layer', 'wta_predictions') }}
),

bets as (
  select event_id from {{ ref('bets') }}
),

pinnacle as (
  select
    event_id,
    tournament_round,
    -- datetime(match_start_at, 'America/Sao_Paulo') as match_start_at,
    datetime_sub(match_start_at, interval 3 hour) as match_start_at,
    coalesce(f1.oncourt_name, p1_name) as p1_name,
    coalesce(f2.oncourt_name, p2_name) as p2_name,
    p1_pinnacle_odds,
    p2_pinnacle_odds,
    1/p1_pinnacle_odds as p1_pinnacle_odds_p,
    1/p2_pinnacle_odds as p2_pinnacle_odds_p,
  from pinnacle_odds as p
    left join fix_player_names as f1 on p.p1_name = f1.pinnacle_name
    left join fix_player_names as f2 on p.p2_name = f2.pinnacle_name
  where resulting_unit = 'Sets'
    and event_type = 'prematch'
    and p1_pinnacle_odds is not null
    and tournament_round not like '%Doubles%'
),

predictions as (
  select
    p1_name,
    p2_name,
    p1_probability as p1_model_p,
    p2_probability as p2_model_p,
    p1_fair_odds as p1_model_odds,
    p2_fair_odds as p2_model_odds
  from atp_predictions
  union all
  select
    p1_name,
    p2_name,
    p1_probability as p1_model_p,
    p2_probability as p2_model_p,
    p1_fair_odds as p1_model_odds,
    p2_fair_odds as p2_model_odds
  from wta_predictions
),

final as (
  select
    o.event_id,
    tournament_round,
    match_start_at,
    o.p1_name,
    o.p2_name,
    100*greatest(
        p1_model_p - p1_pinnacle_odds_p,
        p2_model_p - p2_pinnacle_odds_p
        ) as diff,
    p1_pinnacle_odds,
    p2_pinnacle_odds,
    p1_model_odds,
    p2_model_odds,
    p1_model_p,
    p2_model_p,
    p1_pinnacle_odds_p,
    p2_pinnacle_odds_p
  from pinnacle as o
    left join predictions as p
      on (o.p1_name = p.p1_name
        and o.p2_name = p.p2_name)
  where
    (p1_model_p - p1_pinnacle_odds_p >= 0
    and p1_pinnacle_odds < 2.2)
    or (p2_model_p - p2_pinnacle_odds_p >= 0
    and p2_pinnacle_odds < 2.2)
    and o.event_id not in (select event_id from bets)
)

select * from final




