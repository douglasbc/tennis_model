{{ config(
    materialized = 'table',
    schema = 'analytics',
)}}

select
  'ATP' as tour,
  tournament_tier,
  tournament_round,
  match_start_at,
  p1_name,
  p2_name,
  surface,
  p1_pinnacle_odds,
  p2_pinnacle_odds,
  p1_model_odds,
  p2_model_odds,
  cast(null as int) as diff,
  p1_is_left_handed,
  p2_is_left_handed,
  p1_rally_cluster,
  p1_net_cluster,
  p1_serve_cluster,
  p2_rally_cluster,
  p2_net_cluster,
  p2_serve_cluster
from {{ ref('atp_bets') }}
union all
select
  'WTA' AS tour,
  tournament_tier,
  tournament_round,
  match_start_at,
  p1_name,
  p2_name,
  surface,
  p1_pinnacle_odds,
  p2_pinnacle_odds,
  p1_model_odds,
  p2_model_odds,
  cast(null as int) as diff,
  p1_is_left_handed,
  p2_is_left_handed,
  p1_rally_cluster,
  p1_net_cluster,
  p1_serve_cluster,
  p2_rally_cluster,
  p2_net_cluster,
  p2_serve_cluster
from {{ ref('wta_bets') }}