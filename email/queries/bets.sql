with 

players as (
  select
    player_name
  from `tennis-358702.treated_layer.wta_players`
  union all
    select
    player_name
  from `tennis-358702.treated_layer.atp_players`
),

fix_player_names as (
  select 
    pinnacle_name
  from `tennis-358702.raw_layer.fix_player_names`
),
pinnacle as (
  select
    event_id,
    tournament_round,
    datetime(match_start_at, 'America/Sao_Paulo') as match_start_at,
    p1_name,
    p2_name,
    p1_pinnacle_odds,
    p2_pinnacle_odds,
    1/p1_pinnacle_odds as p1_pinnacle_odds_p,
    1/p2_pinnacle_odds as p2_pinnacle_odds_p,
  from `tennis-358702.raw_layer.pinnacle_odds`
  where resulting_unit = 'Sets'
    and event_type = 'prematch'
    and p1_pinnacle_odds is not null
    and tournament_round not like '%Doubles%'
    and p1_name not in (select pinnacle_name from fix_player_names)
    and p2_name not in (select pinnacle_name from fix_player_names)
),
predictions as (
  select
    p1_name,
    p2_name,
    p1_probability as p1_model_p,
    p2_probability as p2_model_p,
    p1_fair_odds as p1_model_odds,
    p2_fair_odds as p2_model_odds
  from `tennis-358702.model_layer.atp_predictions`
  union all
  select
    p1_name,
    p2_name,
    p1_probability as p1_model_p,
    p2_probability as p2_model_p,
    p1_fair_odds as p1_model_odds,
    p2_fair_odds as p2_model_odds
  from `tennis-358702.model_layer.wta_predictions`
)
select
  match_start_at,
  tournament_round,
  o.p1_name,
  o.p2_name,
  cast(null as float64) as diff,
  p1_pinnacle_odds,
  p2_pinnacle_odds,
  cast(null as float64) as p1_model_odds,
  cast(null as float64) as p2_model_odds,
  cast(null as int64) as p1_matches_past_quarter,
  cast(null as int64) as p1_matches_past_year,
  cast(null as int64) as p2_matches_past_quarter,
  cast(null as int64) as p2_matches_past_year
from pinnacle as o
  left join predictions as p
    on (o.p1_name = p.p1_name
        and o.p2_name = p.p2_name)
where
  p1_model_odds is null
  and
    (o.p1_name not in (select player_name from players)
    or o.p2_name not in (select player_name from players))

union all

select
  match_start_at,
  tournament_round,
  p1_name as player_1,
  p2_name as player_2,
  round(diff, 2) as diff_percentage,
  p1_pinnacle_odds,
  p2_pinnacle_odds,
  round(p1_model_odds, 3) as p1_model_odds,
  round(p2_model_odds, 3) as p2_model_odds,
  p1_matches_past_quarter as p1_quarter,
  p1_matches_past_year as p1_year,
  p2_matches_past_quarter as p2_quarter,
  p2_matches_past_year as p2_year
from `tennis-358702.model_layer.bets` order by match_start_at