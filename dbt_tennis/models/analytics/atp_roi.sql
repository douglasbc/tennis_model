{{ config(
    materialized = 'table',
    schema = 'analytics',
    partition_by = {
      "field": "player_name",
      "data_type": "string"
    }
)}}

with 

atp_matches as (
  select
    *
  from {{ ref('atp_matches') }}
  where
    regexp_extract(result, r'([a-zA-Z]+)') is null
    and tournament_tier not in ('Laver Cup', 'Next Gen ATP Finals')
    and match_date >= '2021-01-01'
),

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
    p1_win_match_odds as match_win_odds,
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
    p2_win_match_odds as match_win_odds,
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
    (is_win * match_win_odds) - 1 as match_win_profit,

    -- plus handicap calculation (receiving extra games)
    case when handicap_line > 0 then
      case
        when (games_won + handicap_line) = games_against then 0
        when (games_won + handicap_line) > games_against then handicap_odds -1
        else -1 end
      end as plus_handicap_profit,

    -- minus handicap calculation (giving away games)
    case when handicap_line < 0 then
      case
        when (games_won + handicap_line) = games_against then 0
        when (games_won + handicap_line) > games_against then handicap_odds -1
        else -1 end
      end as minus_handicap_profit,

     -- flags for special conditions
    case when tournament_tier = 'Grand Slam' then 1 else 0 end as is_grand_slam,
    case when player_country = tournament_country then 1 else 0 end as is_home_country
  from (select * from match_data where match_win_odds is not null)
),

roi_aggregations as (
  select
    player_name,

    opponent_rally_cluster,
    opponent_net_cluster,
    opponent_serve_cluster,

    -- Match win metrics
    COUNT(*) AS total_matches,
    SUM(match_win_profit) AS total_match_win_profit,

    -- Plus handicap metrics
    COUNTIF(handicap_line > 0) AS plus_handicap_matches,
    SUM(plus_handicap_profit) AS total_plus_handicap_profit,

    -- Minus handicap metrics
    COUNTIF(handicap_line < 0) AS minus_handicap_matches,
    SUM(minus_handicap_profit) AS total_minus_handicap_profit,

    -- Grand Slam metrics
    SUM(is_grand_slam) AS grand_slam_matches,
    SUM(match_win_profit * is_grand_slam) AS grand_slam_match_win_profit,
    SUM(plus_handicap_profit * is_grand_slam) AS grand_slam_plus_handicap_profit,
    SUM(minus_handicap_profit * is_grand_slam) AS grand_slam_minus_handicap_profit,

    -- Home country metrics
    SUM(is_home_country) AS home_matches,
    SUM(match_win_profit * is_home_country) AS home_match_win_profit,
    SUM(plus_handicap_profit * is_home_country) AS home_plus_handicap_profit,
    SUM(minus_handicap_profit * is_home_country) AS home_minus_handicap_profit
  FROM bet_calculations
  GROUP BY player_name, opponent_rally_cluster, opponent_net_cluster, opponent_serve_cluster
),

pivoted_roi AS (
  SELECT
    player_name,

  -- Overall ROIs
  sum(total_match_win_profit) / nullif(sum(total_matches), 0) * 100 as overall_match_win_roi,
  sum(total_plus_handicap_profit) / nullif(sum(plus_handicap_matches), 0) * 100 as overall_plus_handicap_roi,
  sum(total_minus_handicap_profit) / nullif(sum(minus_handicap_matches), 0) * 100 as overall_minus_handicap_roi,

    -- Rally Aggression Clusters (1-4)
    -- Cluster 1
    SUM(IF(opponent_rally_cluster = 1, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 1, total_matches, 0)), 0) * 100 AS roi_vs_rally1_match,
    SUM(IF(opponent_rally_cluster = 1, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 1, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally1_plus_handicap,
    SUM(IF(opponent_rally_cluster = 1, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 1, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally1_minus_handicap,
    SUM(IF(opponent_rally_cluster = 1, total_matches, 0)) AS matches_vs_rally1,
    SUM(IF(opponent_rally_cluster = 1, plus_handicap_matches, 0)) AS plus_handicap_vs_rally1,
    SUM(IF(opponent_rally_cluster = 1, minus_handicap_matches, 0)) AS minus_handicap_vs_rally1,

    -- Cluster 2
    SUM(IF(opponent_rally_cluster = 2, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 2, total_matches, 0)), 0) * 100 AS roi_vs_rally2_match,
    SUM(IF(opponent_rally_cluster = 2, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 2, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally2_plus_handicap,
    SUM(IF(opponent_rally_cluster = 2, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 2, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally2_minus_handicap,
    SUM(IF(opponent_rally_cluster = 2, total_matches, 0)) AS matches_vs_rally2,
    SUM(IF(opponent_rally_cluster = 2, plus_handicap_matches, 0)) AS plus_handicap_vs_rally2,
    SUM(IF(opponent_rally_cluster = 2, minus_handicap_matches, 0)) AS minus_handicap_vs_rally2,

    -- Cluster 3
    SUM(IF(opponent_rally_cluster = 3, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 3, total_matches, 0)), 0) * 100 AS roi_vs_rally3_match,
    SUM(IF(opponent_rally_cluster = 3, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 3, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally3_plus_handicap,
    SUM(IF(opponent_rally_cluster = 3, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 3, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally3_minus_handicap,
    SUM(IF(opponent_rally_cluster = 3, total_matches, 0)) AS matches_vs_rally3,
    SUM(IF(opponent_rally_cluster = 3, plus_handicap_matches, 0)) AS plus_handicap_vs_rally3,
    SUM(IF(opponent_rally_cluster = 3, minus_handicap_matches, 0)) AS minus_handicap_vs_rally3,

    -- Cluster 4
    SUM(IF(opponent_rally_cluster = 4, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 4, total_matches, 0)), 0) * 100 AS roi_vs_rally4_match,
    SUM(IF(opponent_rally_cluster = 4, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 4, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally4_plus_handicap,
    SUM(IF(opponent_rally_cluster = 4, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_rally_cluster = 4, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_rally4_minus_handicap,
    SUM(IF(opponent_rally_cluster = 4, total_matches, 0)) AS matches_vs_rally4,
    SUM(IF(opponent_rally_cluster = 4, plus_handicap_matches, 0)) AS plus_handicap_vs_rally4,
    SUM(IF(opponent_rally_cluster = 4, minus_handicap_matches, 0)) AS minus_handicap_vs_rally4,

    -- Net Points Clusters (1-4)
    -- Cluster 1
    SUM(IF(opponent_net_cluster = 1, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 1, total_matches, 0)), 0) * 100 AS roi_vs_net1_match,
    SUM(IF(opponent_net_cluster = 1, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 1, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_net1_plus_handicap,
    SUM(IF(opponent_net_cluster = 1, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 1, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_net1_minus_handicap,
    SUM(IF(opponent_net_cluster = 1, total_matches, 0)) AS matches_vs_net1,
    SUM(IF(opponent_net_cluster = 1, plus_handicap_matches, 0)) AS plus_handicap_vs_net1,
    SUM(IF(opponent_net_cluster = 1, minus_handicap_matches, 0)) AS minus_handicap_vs_net1,

    -- Cluster 2
    SUM(IF(opponent_net_cluster = 2, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 2, total_matches, 0)), 0) * 100 AS roi_vs_net2_match,
    SUM(IF(opponent_net_cluster = 2, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 2, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_net2_plus_handicap,
    SUM(IF(opponent_net_cluster = 2, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 2, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_net2_minus_handicap,
    SUM(IF(opponent_net_cluster = 2, total_matches, 0)) AS matches_vs_net2,
    SUM(IF(opponent_net_cluster = 2, plus_handicap_matches, 0)) AS plus_handicap_vs_net2,
    SUM(IF(opponent_net_cluster = 2, minus_handicap_matches, 0)) AS minus_handicap_vs_net2,

    -- Cluster 3
    SUM(IF(opponent_net_cluster = 3, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 3, total_matches, 0)), 0) * 100 AS roi_vs_net3_match,
    SUM(IF(opponent_net_cluster = 3, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 3, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_net3_plus_handicap,
    SUM(IF(opponent_net_cluster = 3, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 3, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_net3_minus_handicap,
    SUM(IF(opponent_net_cluster = 3, total_matches, 0)) AS matches_vs_net3,
    SUM(IF(opponent_net_cluster = 3, plus_handicap_matches, 0)) AS plus_handicap_vs_net3,
    SUM(IF(opponent_net_cluster = 3, minus_handicap_matches, 0)) AS minus_handicap_vs_net3,

    -- Cluster 4
    SUM(IF(opponent_net_cluster = 4, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 4, total_matches, 0)), 0) * 100 AS roi_vs_net4_match,
    SUM(IF(opponent_net_cluster = 4, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 4, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_net4_plus_handicap,
    SUM(IF(opponent_net_cluster = 4, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_net_cluster = 4, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_net4_minus_handicap,
    SUM(IF(opponent_net_cluster = 4, total_matches, 0)) AS matches_vs_net4,
    SUM(IF(opponent_net_cluster = 4, plus_handicap_matches, 0)) AS plus_handicap_vs_net4,
    SUM(IF(opponent_net_cluster = 4, minus_handicap_matches, 0)) AS minus_handicap_vs_net4,

    -- Serve Dependency Clusters (1-5)
    -- Cluster 1
    SUM(IF(opponent_serve_cluster = 1, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 1, total_matches, 0)), 0) * 100 AS roi_vs_serve1_match,
    SUM(IF(opponent_serve_cluster = 1, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 1, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve1_plus_handicap,
    SUM(IF(opponent_serve_cluster = 1, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 1, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve1_minus_handicap,
    SUM(IF(opponent_serve_cluster = 1, total_matches, 0)) AS matches_vs_serve1,
    SUM(IF(opponent_serve_cluster = 1, plus_handicap_matches, 0)) AS plus_handicap_vs_serve1,
    SUM(IF(opponent_serve_cluster = 1, minus_handicap_matches, 0)) AS minus_handicap_vs_serve1,

    -- Cluster 2
    SUM(IF(opponent_serve_cluster = 2, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 2, total_matches, 0)), 0) * 100 AS roi_vs_serve2_match,
    SUM(IF(opponent_serve_cluster = 2, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 2, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve2_plus_handicap,
    SUM(IF(opponent_serve_cluster = 2, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 2, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve2_minus_handicap,
    SUM(IF(opponent_serve_cluster = 2, total_matches, 0)) AS matches_vs_serve2,
    SUM(IF(opponent_serve_cluster = 2, plus_handicap_matches, 0)) AS plus_handicap_vs_serve2,
    SUM(IF(opponent_serve_cluster = 2, minus_handicap_matches, 0)) AS minus_handicap_vs_serve2,

    -- Cluster 3
    SUM(IF(opponent_serve_cluster = 3, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 3, total_matches, 0)), 0) * 100 AS roi_vs_serve3_match,
    SUM(IF(opponent_serve_cluster = 3, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 3, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve3_plus_handicap,
    SUM(IF(opponent_serve_cluster = 3, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 3, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve3_minus_handicap,
    SUM(IF(opponent_serve_cluster = 3, total_matches, 0)) AS matches_vs_serve3,
    SUM(IF(opponent_serve_cluster = 3, plus_handicap_matches, 0)) AS plus_handicap_vs_serve3,
    SUM(IF(opponent_serve_cluster = 3, minus_handicap_matches, 0)) AS minus_handicap_vs_serve3,

    -- Cluster 4
    SUM(IF(opponent_serve_cluster = 4, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 4, total_matches, 0)), 0) * 100 AS roi_vs_serve4_match,
    SUM(IF(opponent_serve_cluster = 4, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 4, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve4_plus_handicap,
    SUM(IF(opponent_serve_cluster = 4, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 4, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve4_minus_handicap,
    SUM(IF(opponent_serve_cluster = 4, total_matches, 0)) AS matches_vs_serve4,
    SUM(IF(opponent_serve_cluster = 4, plus_handicap_matches, 0)) AS plus_handicap_vs_serve4,
    SUM(IF(opponent_serve_cluster = 4, minus_handicap_matches, 0)) AS minus_handicap_vs_serve4,

    -- Cluster 5
    SUM(IF(opponent_serve_cluster = 5, total_match_win_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 5, total_matches, 0)), 0) * 100 AS roi_vs_serve5_match,
    SUM(IF(opponent_serve_cluster = 5, total_plus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 5, plus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve5_plus_handicap,
    SUM(IF(opponent_serve_cluster = 5, total_minus_handicap_profit, 0)) / NULLIF(SUM(IF(opponent_serve_cluster = 5, minus_handicap_matches, 0)), 0) * 100 AS roi_vs_serve5_minus_handicap,
    SUM(IF(opponent_serve_cluster = 5, total_matches, 0)) AS matches_vs_serve5,
    SUM(IF(opponent_serve_cluster = 5, plus_handicap_matches, 0)) AS plus_handicap_vs_serve5,
    SUM(IF(opponent_serve_cluster = 5, minus_handicap_matches, 0)) AS minus_handicap_vs_serve5,

    -- Grand Slam Performance
    SUM(grand_slam_match_win_profit) / NULLIF(SUM(grand_slam_matches), 0) * 100 AS grand_slam_match_win_roi,
    SUM(grand_slam_plus_handicap_profit) / NULLIF(SUM(IF(opponent_rally_cluster IS NOT NULL, grand_slam_matches, 0)), 0) * 100 AS grand_slam_plus_handicap_roi,
    SUM(grand_slam_minus_handicap_profit) / NULLIF(SUM(IF(opponent_rally_cluster IS NOT NULL, grand_slam_matches, 0)), 0) * 100 AS grand_slam_minus_handicap_roi,
    SUM(grand_slam_matches) AS grand_slam_matches_count,

    -- Home Country Performance
    SUM(home_match_win_profit) / NULLIF(SUM(home_matches), 0) * 100 AS home_match_win_roi,
    SUM(home_plus_handicap_profit) / NULLIF(SUM(IF(opponent_rally_cluster IS NOT NULL, home_matches, 0)), 0) * 100 AS home_plus_handicap_roi,
    SUM(home_minus_handicap_profit) / NULLIF(SUM(IF(opponent_rally_cluster IS NOT NULL, home_matches, 0)), 0) * 100 AS home_minus_handicap_roi,
    SUM(home_matches) AS home_matches_count

  FROM roi_aggregations
  GROUP BY player_name
)

-- Final selection with all columns
SELECT *
FROM pivoted_roi