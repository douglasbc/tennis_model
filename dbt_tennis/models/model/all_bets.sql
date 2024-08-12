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

atp_players_clusters as (
  select * from {{ source('raw_layer', 'atp_players_clusters') }}
),

wta_players_clusters as (
  select * from {{ source('raw_layer', 'wta_players_clusters') }}
),

atp_player_cluster_roi as (
  select * from {{ ref('atp_player_cluster_roi') }}
),

wta_player_cluster_roi as (
  select * from {{ ref('wta_player_cluster_roi') }}
),

atp_matches_count as (
  select * from {{ ref('atp_matches_count') }}
),

wta_matches_count as (
  select * from {{ ref('wta_matches_count') }}
),

matches_count as (
  select * from atp_matches_count
  union all
  select * from wta_matches_count
),

pinnacle as (
  select
    event_id,
    tournament_round,
    datetime_sub(match_start_at, interval 3 hour) as match_start_at,
    -- match_start_at,
    -- datetime(match_start_at, 'America/Sao_Paulo') as match_start_at,
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

bets_clusters as (
  select
    event_id,
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
    p2_pinnacle_odds_p,
    coalesce(atp_c_1.cluster, wta_c_1.cluster) as p1_cluster,
    coalesce(atp_c_2.cluster, wta_c_2.cluster) as p2_cluster,
    c1.matches_past_quarter as p1_matches_past_quarter,
    c1.matches_past_year as p1_matches_past_year,
    c2.matches_past_quarter as p2_matches_past_quarter,
    c2.matches_past_year as p2_matches_past_year,
  from pinnacle as o
    left join predictions as p
      on (o.p1_name = p.p1_name
        and o.p2_name = p.p2_name)
    left join matches_count as c1
      on o.p1_name = c1.player
    left join matches_count as c2
      on o.p2_name = c2.player
    left join atp_players_clusters as atp_c_1
      on o.p1_name = atp_c_1.player_name
    left join atp_players_clusters as atp_c_2
      on o.p2_name = atp_c_2.player_name
    left join wta_players_clusters as wta_c_1
      on o.p1_name = wta_c_1.player_name
    left join wta_players_clusters as wta_c_2
      on o.p2_name = wta_c_2.player_name
),

atp_player_roi as (
  select
    player_name,
    sum(matches) as matches,
    sum(net_units) as net_units,
    round(SAFE_DIVIDE(sum(net_units),sum(matches)), 2) * 100 as net_roi,
    sum(handicap_matches) as handicap_matches,
    sum(handicap_net_units) as handicap_net_units,
    sum(handicap_net_roi) as handicap_net_roi
  from atp_player_cluster_roi
  group by 1
),

wta_player_roi as (
  select
    player_name,
    sum(matches) as matches,
    sum(net_units) as net_units,
    round(SAFE_DIVIDE(sum(net_units),sum(matches)), 2) * 100 as net_roi,
    sum(handicap_matches) as handicap_matches,
    sum(handicap_net_units) as handicap_net_units,
    sum(handicap_net_roi) as handicap_net_roi
  from wta_player_cluster_roi
  group by 1
),

final as (
  select
    event_id,
    tournament_round,
    match_start_at,
    p1_name,
    p2_name,
    round(diff, 2) as diff,
    round(p1_pinnacle_odds, 3) as p1_pinnacle_odds,
    round(p2_pinnacle_odds, 3) as p2_pinnacle_odds,
    round(p1_model_odds, 3) as p1_model_odds,
    round(p2_model_odds, 3) as p2_model_odds,
    round(p1_model_p, 2) as p1_model_p,
    round(p2_model_p, 2) as p2_model_p,
    round(p1_pinnacle_odds_p, 2) as p1_pinnacle_odds_p,
    round(p2_pinnacle_odds_p, 2) as p2_pinnacle_odds_p,
    p1_cluster,
    p2_cluster,
    coalesce(atp_r_1.matches, wta_r_1.matches) as p1_matches,
    round(coalesce(atp_r_1.net_units, wta_r_1.net_units), 2) as p1_net_units,
    round(coalesce(atp_r_1.net_roi, wta_r_1.net_roi), 2) as p1_net_roi,
    coalesce(atp_r_2.matches, wta_r_2.matches) as p2_matches,
    round(coalesce(atp_r_2.net_units, wta_r_2.net_units), 2) as p2_net_units,
    round(coalesce(atp_r_2.net_roi, wta_r_2.net_roi), 2) as p2_net_roi,
    coalesce(atp_r_1.handicap_matches, wta_r_1.handicap_matches) as p1_handicap_matches,
    round(coalesce(atp_r_1.handicap_net_units, wta_r_1.handicap_net_units), 2) as p1_handicap_net_units,
    round(coalesce(atp_r_1.handicap_net_roi, wta_r_1.handicap_net_roi), 2) as p1_handicap_net_roi,
    coalesce(atp_r_2.handicap_matches, wta_r_2.handicap_matches) as p2_handicap_matches,
    round(coalesce(atp_r_2.handicap_net_units, wta_r_2.handicap_net_units), 2) as p2_handicap_net_units,
    round(coalesce(atp_r_2.handicap_net_roi, wta_r_2.handicap_net_roi), 2) as p2_handicap_net_roi,
    coalesce(atp_cr_1.matches, wta_cr_1.matches) as p1_matches_against_cluster,
    round(coalesce(atp_cr_1.net_units, wta_cr_1.net_units), 2) as p1_net_units_against_cluster,
    round(coalesce(atp_cr_1.net_roi, wta_cr_1.net_roi), 2) as p1_net_roi_against_cluster,
    coalesce(atp_cr_2.matches, wta_cr_2.matches) as p2_matches_against_cluster,
    round(coalesce(atp_cr_2.net_units, wta_cr_2.net_units, 2)) as p2_net_units_against_cluster,
    round(coalesce(atp_cr_2.net_roi, wta_cr_2.net_roi), 2) as p2_net_roi_against_cluster,
    coalesce(atp_cr_1.handicap_matches, wta_cr_1.handicap_matches) as p1_handicap_matches_against_cluster,
    round(coalesce(atp_cr_1.handicap_net_units, wta_cr_1.handicap_net_units), 2) as p1_handicap_net_units_against_cluster,
    round(coalesce(atp_cr_1.handicap_net_roi, wta_cr_1.handicap_net_roi), 2) as p1_handicap_net_roi_against_cluster,
    coalesce(atp_cr_2.handicap_matches, wta_cr_2.handicap_matches) as p2_handicap_matches_against_cluster,
    round(coalesce(atp_cr_2.handicap_net_units, wta_cr_2.handicap_net_units), 2) as p2_handicap_net_units_against_cluster,
    round(coalesce(atp_cr_2.handicap_net_roi, wta_cr_2.handicap_net_roi), 2) as p2_handicap_net_roi_against_cluster,
    p1_matches_past_quarter,
    p1_matches_past_year,
    p2_matches_past_quarter,
    p2_matches_past_year,
  from bets_clusters as o
    left join atp_player_roi as atp_r_1
      on o.p1_name = atp_r_1.player_name
    left join atp_player_roi as atp_r_2
      on o.p2_name = atp_r_2.player_name
    left join wta_player_roi as wta_r_1
      on o.p1_name = wta_r_1.player_name
    left join wta_player_roi as wta_r_2
      on o.p2_name = wta_r_2.player_name
    left join atp_player_cluster_roi as atp_cr_1
      on o.p1_name = atp_cr_1.player_name and o.p2_cluster = atp_cr_1.opponent_cluster
    left join atp_player_cluster_roi as atp_cr_2
      on o.p2_name = atp_cr_2.player_name and o.p1_cluster = atp_cr_2.opponent_cluster
    left join wta_player_cluster_roi as wta_cr_1
      on o.p1_name = wta_cr_1.player_name and o.p2_cluster = wta_cr_1.opponent_cluster
    left join wta_player_cluster_roi as wta_cr_2
      on o.p2_name = wta_cr_2.player_name and o.p1_cluster = wta_cr_2.opponent_cluster
)

select * from final
