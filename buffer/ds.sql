match_data as (
  select
    match_id,
    tournament_name,
    surface,
    tournament_tier,
    tournament_country,
    p1_name as player_name,
    p1_country as player_country,
    p1_rally_aggression_cluster as player_rally_cluster,
    p1_net_points_cluster as player_net_cluster,
    p1_serve_dependency_cluster as player_serve_cluster,
    p2_name as opponent_name,
    p2_country as opponent_country,
    p2_rally_aggression_cluster as opponent_rally_cluster,
    p2_net_points_cluster as opponent_net_cluster,
    p2_serve_dependency_cluster as opponent_serve_cluster,
    1 as is_win,
    p1_win_match_odds as win_match_odds,
    p1_handicap_line as handicap_line,
    p1_handicap_odds as handicap_odds,
    p1_total_games as games_won,
    p2_total_games as games_against
  from atp_matches

  union all

  select
    match_id,
    tournament_name,
    surface,
    tournament_tier,
    tournament_country,
    p2_name as player_name,
    p2_country as player_country,
    p2_rally_aggression_cluster as player_rally_cluster,
    p2_net_points_cluster as player_net_cluster,
    p2_serve_dependency_cluster as player_serve_cluster,
    p1_name as opponent_name,
    p1_country as opponent_country,
    p1_rally_aggression_cluster as opponent_rally_cluster,
    p1_net_points_cluster as opponent_net_cluster,
    p1_serve_dependency_cluster as opponent_serve_cluster,
    0 as is_win,
    p2_win_match_odds as win_match_odds,
    p2_handicap_line as handicap_line,
    p2_handicap_odds as handicap_odds,
    p2_total_games as games_won,
    p1_total_games as games_against
  from atp_matches
),

bet_calculations as (
  select
    *,
    -- match win roi calculation
    (is_win * win_match_odds) - 1 as match_win_profit,

    -- handicap calculations
    case
      when handicap_line > 0 then  -- plus handicap
        case when (games_won + handicap_line) > games_against then handicap_odds else 0 end - 1
      when handicap_line < 0 then  -- minus handicap
        case when (games_won + handicap_line) > games_against then handicap_odds else 0 end - 1
    end as handicap_profit,

    -- flags for special conditions
    case when tournament_tier = 'Grand Slam' then 1 else 0 end as is_grand_slam,
    case when player_country = tournament_country then 1 else 0 end as is_home_country
  from match_data
),

roi_aggregations as (
  select
    player_name,

    -- overall roi
    safe_divide(sum(match_win_profit), count(*)) * 100 as overall_match_win_roi,
    safe_divide(sum(handicap_profit), sum(sign(abs(handicap_line)))) * 100 as overall_handicap_roi,

    -- cluster-specific rois
    opponent_rally_cluster,
    opponent_net_cluster,
    opponent_serve_cluster,

    -- grand slam roi
    safe_divide(sum(match_win_profit * is_grand_slam), sum(is_grand_slam))) * 100 as grand_slam_match_win_roi,
    safe_divide(sum(handicap_profit * is_grand_slam), sum(is_grand_slam * sign(abs(handicap_line)))) * 100 as grand_slam_handicap_roi,

    -- home country roi
    safe_divide(sum(match_win_profit * is_home_country), sum(is_home_country))) * 100 as home_match_win_roi,
    safe_divide(sum(handicap_profit * is_home_country), sum(is_home_country * sign(abs(handicap_line)))) * 100 as home_handicap_roi
  from bet_calculations
  group by player_name, opponent_rally_cluster, opponent_net_cluster, opponent_serve_cluster
),

pivoted_roi as (
  select
    player_name,
    -- rally aggression cluster rois
    max(case when opponent_rally_cluster = 1 then safe_divide(sum(match_win_profit), count(*)) * 100 end) as roi_vs_rally1_match,
    max(case when opponent_rally_cluster = 1 then safe_divide(sum(handicap_profit), sum(sign(abs(handicap_line)))) * 100 end) as roi_vs_rally1_handicap,
    -- repeat for clusters 2-4...

    -- net points cluster rois
    max(case when opponent_net_cluster = 1 then safe_divide(sum(match_win_profit), count(*)) * 100 end) as roi_vs_net1_match,
    max(case when opponent_net_cluster = 1 then safe_divide(sum(handicap_profit), sum(sign(abs(handicap_line)))) * 100 end) as roi_vs_net1_handicap,
    -- repeat for clusters 2-4...

    -- serve dependency cluster rois
    max(case when opponent_serve_cluster = 1 then safe_divide(sum(match_win_profit), count(*)) * 100 end) as roi_vs_serve1_match,
    max(case when opponent_serve_cluster = 1 then safe_divide(sum(handicap_profit), sum(sign(abs(handicap_line)))) * 100 end) as roi_vs_serve1_handicap,
    -- repeat for clusters 2-5...

    -- special conditions
    avg(grand_slam_match_win_roi) as grand_slam_match_win_roi,
    avg(grand_slam_handicap_roi) as grand_slam_handicap_roi,
    avg(home_match_win_roi) as home_match_win_roi,
    avg(home_handicap_roi) as home_handicap_roi
  from roi_aggregations
  group by player_name
)

select *
from pivoted_roi;