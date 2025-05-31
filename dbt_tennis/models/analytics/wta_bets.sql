{{ config(
    materialized = 'table',
    schema = 'analytics',
)}}

with

fix_player_names as (
  select * from {{ source('raw_layer', 'fix_player_names') }}
),

pinnacle_odds as (
  select
    event_id,
    tournament_round,
--     datetime_sub(match_start_at, interval 3 hour) as match_start_at,
    match_start_at,
    coalesce(f1.oncourt_name, p1_name) as p1_name,
    coalesce(f2.oncourt_name, p2_name) as p2_name,
    p1_pinnacle_odds,
    p2_pinnacle_odds,
    1/p1_pinnacle_odds as p1_pinnacle_odds_p,
    1/p2_pinnacle_odds as p2_pinnacle_odds_p,
  from {{ source('raw_layer', 'pinnacle_odds') }} as p
    left join fix_player_names as f1 on p.p1_name = f1.pinnacle_name
    left join fix_player_names as f2 on p.p2_name = f2.pinnacle_name
  where resulting_unit = 'Sets'
    and event_type = 'prematch'
    and p1_pinnacle_odds is not null
    and tournament_round not like '%Doubles%'
),

wta_predictions as (
  select * from {{ source('raw_layer', 'wta_predictions') }}
),

wta_serve_dependency_clusters as (
  select * from {{ source('raw_layer', 'wta_serve_dependency_clusters') }}
),

wta_rally_aggression_clusters as (
  select * from {{ source('raw_layer', 'wta_rally_aggression_clusters') }}
),

wta_net_points_clusters as (
  select * from {{ source('raw_layer', 'wta_net_points_clusters') }}
),

wta_roi as (
  select * from {{ ref('wta_roi') }}
),

wta_matches_count as (
  select * from {{ ref('wta_matches_count') }}
),

clusters as (
  select
    s.player_name,
    s.best_cluster as serve_cluster,
    r.best_cluster as rally_cluster,
    n.best_cluster as net_cluster
  from wta_serve_dependency_clusters s
  left join wta_net_points_clusters n on s.player_name = n.player_name
  left join wta_rally_aggression_clusters r on s.player_name = r.player_name
),

roi_data as (
  select
    player_name,
    -- Overall ROIs (against all opponents)
    overall_match_win_roi,
    overall_plus_handicap_roi,
    overall_minus_handicap_roi,

    -- NEW: Against left-handed opponents
    vs_left_handed_match_roi,
    vs_left_handed_plus_handicap_roi,
    vs_left_handed_minus_handicap_roi,

    -- Surface ROIs
    clay_match_win_roi, clay_plus_handicap_roi, clay_minus_handicap_roi,
    grass_match_win_roi, grass_plus_handicap_roi, grass_minus_handicap_roi,
    hard_match_win_roi, hard_plus_handicap_roi, hard_minus_handicap_roi,
    indoor_hard_match_win_roi, indoor_hard_plus_handicap_roi, indoor_hard_minus_handicap_roi,

    -- Rally clusters
    roi_vs_rally1_match, roi_vs_rally1_plus_handicap, roi_vs_rally1_minus_handicap,
    roi_vs_rally2_match, roi_vs_rally2_plus_handicap, roi_vs_rally2_minus_handicap,
    roi_vs_rally3_match, roi_vs_rally3_plus_handicap, roi_vs_rally3_minus_handicap,
    roi_vs_rally4_match, roi_vs_rally4_plus_handicap, roi_vs_rally4_minus_handicap,
    -- Net clusters
    roi_vs_net1_match, roi_vs_net1_plus_handicap, roi_vs_net1_minus_handicap,
    roi_vs_net2_match, roi_vs_net2_plus_handicap, roi_vs_net2_minus_handicap,
    roi_vs_net3_match, roi_vs_net3_plus_handicap, roi_vs_net3_minus_handicap,
    -- Serve clusters
    roi_vs_serve1_match, roi_vs_serve1_plus_handicap, roi_vs_serve1_minus_handicap,
    roi_vs_serve2_match, roi_vs_serve2_plus_handicap, roi_vs_serve2_minus_handicap,
    roi_vs_serve3_match, roi_vs_serve3_plus_handicap, roi_vs_serve3_minus_handicap,
    roi_vs_serve4_match, roi_vs_serve4_plus_handicap, roi_vs_serve4_minus_handicap,

    -- Special conditions
    grand_slam_match_win_roi, grand_slam_plus_handicap_roi, grand_slam_minus_handicap_roi,
    home_match_win_roi, home_plus_handicap_roi, home_minus_handicap_roi
  from wta_roi
),

base_matches as (
  select
    po.event_id,
    po.tournament_round,
    ap.surface,
    datetime_sub(po.match_start_at, interval 3 hour) as match_start_at,
    po.p1_name,
    po.p2_name,
    po.p1_pinnacle_odds,
    po.p2_pinnacle_odds,
    1/po.p1_pinnacle_odds as p1_implied_prob,
    1/po.p2_pinnacle_odds as p2_implied_prob,
    ap.p1_probability as p1_model_prob,
    ap.p2_probability as p2_model_prob,
    ap.p1_fair_odds as p1_model_odds,
    ap.p2_fair_odds as p2_model_odds
  from pinnacle_odds po
  left join wta_predictions ap
    on po.p1_name = ap.p1_name
    and po.p2_name = ap.p2_name
),

match_clusters as (
  select
    bm.*,
    c1.rally_cluster as p1_rally_cluster,
    c1.net_cluster as p1_net_cluster,
    c1.serve_cluster as p1_serve_cluster,
    c2.rally_cluster as p2_rally_cluster,
    c2.net_cluster as p2_net_cluster,
    c2.serve_cluster as p2_serve_cluster
  from base_matches bm
  join clusters c1 on bm.p1_name = c1.player_name
  join clusters c2 on bm.p2_name = c2.player_name
),

roi_enhancements as (
  select
    mc.*,

    -- NEW: Overall ROIs for Player 1
    r1.overall_match_win_roi as p1_overall_match_roi,
    r1.overall_plus_handicap_roi as p1_overall_plus_handicap_roi,
    r1.overall_minus_handicap_roi as p1_overall_minus_handicap_roi,

    -- NEW: Left-handed ROIs
    r1.vs_left_handed_match_roi as p1_vs_left_handed_roi,
    r2.vs_left_handed_match_roi as p2_vs_left_handed_roi,

    -- NEW: Surface ROIs
    case
      when surface = 'Clay' then r1.clay_match_win_roi
      when surface = 'Grass' then r1.grass_match_win_roi
      when surface = 'Hard' then r1.hard_match_win_roi
      when surface = 'Indoor Hard' then r1.indoor_hard_match_win_roi
      else null
    end as p1_surface_roi,

    case
      when surface = 'Clay' then r2.clay_match_win_roi
      when surface = 'Grass' then r2.grass_match_win_roi
      when surface = 'Hard' then r2.hard_match_win_roi
      when surface = 'Indoor Hard' then r2.indoor_hard_match_win_roi
      else null
    end as p2_surface_roi,

    -- Player 1 ROI against Player 2's clusters
    case p2_rally_cluster
      when 1 then r1.roi_vs_rally1_match
      when 2 then r1.roi_vs_rally2_match
      when 3 then r1.roi_vs_rally3_match
      when 4 then r1.roi_vs_rally4_match
    end as p1_roi_vs_p2_rally,

    case p2_net_cluster
      when 1 then r1.roi_vs_net1_match
      when 2 then r1.roi_vs_net2_match
      when 3 then r1.roi_vs_net3_match
    end as p1_roi_vs_p2_net,

    case p2_serve_cluster
      when 1 then r1.roi_vs_serve1_match
      when 2 then r1.roi_vs_serve2_match
      when 3 then r1.roi_vs_serve3_match
      when 4 then r1.roi_vs_serve4_match
    end as p1_roi_vs_p2_serve,

     -- NEW: Overall ROIs for Player 1
    r2.overall_match_win_roi as p2_overall_match_roi,
    r2.overall_plus_handicap_roi as p2_overall_plus_handicap_roi,
    r2.overall_minus_handicap_roi as p2_overall_minus_handicap_roi,

    -- Player 2 ROI against Player 1's clusters
    case p1_rally_cluster
      when 1 then r2.roi_vs_rally1_match
      when 2 then r2.roi_vs_rally2_match
      when 3 then r2.roi_vs_rally3_match
      when 4 then r2.roi_vs_rally4_match
    end as p2_roi_vs_p1_rally,

    case p1_net_cluster
      when 1 then r2.roi_vs_net1_match
      when 2 then r2.roi_vs_net2_match
      when 3 then r2.roi_vs_net3_match
    end as p2_roi_vs_p1_net,

    case p1_serve_cluster
      when 1 then r2.roi_vs_serve1_match
      when 2 then r2.roi_vs_serve2_match
      when 3 then r2.roi_vs_serve3_match
      when 4 then r2.roi_vs_serve4_match
    end as p2_roi_vs_p1_serve,


    -- Special condition ROIs
    r1.grand_slam_match_win_roi as p1_grand_slam_roi,
    r2.grand_slam_match_win_roi as p2_grand_slam_roi,
    r1.home_match_win_roi as p1_home_roi,
    r2.home_match_win_roi as p2_home_roi

  from match_clusters mc
  left join roi_data r1 on mc.p1_name = r1.player_name
  left join roi_data r2 on mc.p2_name = r2.player_name
)

select
--   event_id,
  tournament_round,
  match_start_at,
  p1_name,
  p2_name,
  100*greatest(
        p1_model_prob - p1_implied_prob,
        p2_model_prob - p2_implied_prob
        ) as diff,
  round(p1_pinnacle_odds, 2) as p1_pinnacle_odds,
  round(p2_pinnacle_odds, 2) as p2_pinnacle_odds,
--   p1_implied_prob,
--   p2_implied_prob,
--   p1_model_prob,
--   p2_model_prob,
  round(p1_model_odds, 2) as p1_model_odds,
  round(p2_model_odds, 2) as p2_model_odds,

--   -- Cluster information
--   p1_rally_cluster,
--   p1_net_cluster,
--   p1_serve_cluster,
--   p2_rally_cluster,
--   p2_net_cluster,
--   p2_serve_cluster,

-- NEW: Overall ROI metrics
  round(p1_overall_match_roi, 2) as p1_overall_match_roi,
  round(p1_overall_plus_handicap_roi, 2) as p1_overall_plus_handicap_roi,
  round(p1_overall_minus_handicap_roi, 2) as p1_overall_minus_handicap_roi,
  round(p2_overall_match_roi, 2) as p2_overall_match_roi,
  round(p2_overall_plus_handicap_roi, 2) as p2_overall_plus_handicap_roi,
  round(p2_overall_minus_handicap_roi, 2) as p2_overall_minus_handicap_roi,

  -- NEW: Left-handed metrics
  round(p1_vs_left_handed_roi, 2) as p1_vs_left_handed_roi,
  round(p2_vs_left_handed_roi, 2) as p2_vs_left_handed_roi,

  -- NEW: Surface metrics
  round(p1_surface_roi, 2) as p1_surface_roi,
  round(p2_surface_roi, 2) as p2_surface_roi,


  -- ROI metrics
  round(p1_roi_vs_p2_rally, 2) as p1_roi_vs_p2_rally,
  round(p1_roi_vs_p2_net, 2) as p1_roi_vs_p2_net,
  round(p1_roi_vs_p2_serve, 2) as p1_roi_vs_p2_serve,
  round(p2_roi_vs_p1_rally, 2) as p2_roi_vs_p1_rally,
  round(p2_roi_vs_p1_net, 2) as p2_roi_vs_p1_net,
  round(p2_roi_vs_p1_serve, 2) as p2_roi_vs_p1_serve,

  -- Special condition ROIs
  round(p1_grand_slam_roi, 2) as p1_grand_slam_roi,
  round(p2_grand_slam_roi, 2) as p2_grand_slam_roi,
  round(p1_home_roi, 2) as p1_home_roi,
  round(p2_home_roi, 2) as p2_home_roi

from roi_enhancements