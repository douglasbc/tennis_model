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
    and p1_win_match_odds is not null and p2_win_match_odds is not null
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
    p1_is_left_handed,  -- Added player handedness
    p1_rally_aggression_cluster as player_rally_cluster,
    p1_net_points_cluster as player_net_cluster,
    p1_serve_dependency_cluster as player_serve_cluster,
    p2_name as opponent_name,
    p2_country as opponent_country,
    p2_is_left_handed as opponent_is_left_handed,  -- Added opponent handedness
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
    p2_is_left_handed,  -- Added player handedness
    p2_rally_aggression_cluster as player_rally_cluster,
    p2_net_points_cluster as player_net_cluster,
    p2_serve_dependency_cluster as player_serve_cluster,
    p1_name as opponent_name,
    p1_country as opponent_country,
    p1_is_left_handed as opponent_is_left_handed,  -- Added opponent handedness
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

-- CORRECTED overall_roi CTE
overall_roi AS (
  SELECT
    player_name,
    -- Match win metrics
    COUNT(*) AS overall_total_matches,
    SUM(match_win_profit) AS overall_total_match_win_profit,

    -- Plus handicap metrics - ONLY COUNT MATCHES WITH VALID ODDS
    COUNTIF(handicap_line > 0 AND handicap_odds IS NOT NULL) AS overall_plus_handicap_matches,
    SUM(IF(handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS overall_total_plus_handicap_profit,

    -- Minus handicap metrics - ONLY COUNT MATCHES WITH VALID ODDS
    COUNTIF(handicap_line < 0 AND handicap_odds IS NOT NULL) AS overall_minus_handicap_matches,
    SUM(IF(handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS overall_total_minus_handicap_profit,

    -- NEW: Against left-handed opponents
    COUNTIF(opponent_is_left_handed) AS vs_left_handed_matches,
    SUM(IF(opponent_is_left_handed, match_win_profit, 0)) AS vs_left_handed_match_win_profit,
    COUNTIF(opponent_is_left_handed AND handicap_line > 0 AND handicap_odds IS NOT NULL) AS vs_left_handed_plus_handicap_matches,
    SUM(IF(opponent_is_left_handed AND handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS vs_left_handed_plus_handicap_profit,
    COUNTIF(opponent_is_left_handed AND handicap_line < 0 AND handicap_odds IS NOT NULL) AS vs_left_handed_minus_handicap_matches,
    SUM(IF(opponent_is_left_handed AND handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS vs_left_handed_minus_handicap_profit,

    -- Surface-specific metrics
    COUNTIF(surface = 'Clay') AS clay_matches,
    SUM(IF(surface = 'Clay', match_win_profit, 0)) AS clay_match_win_profit,
    COUNTIF(surface = 'Clay' AND handicap_line > 0 AND handicap_odds IS NOT NULL) AS clay_plus_handicap_matches,
    SUM(IF(surface = 'Clay' AND handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS clay_plus_handicap_profit,
    COUNTIF(surface = 'Clay' AND handicap_line < 0 AND handicap_odds IS NOT NULL) AS clay_minus_handicap_matches,
    SUM(IF(surface = 'Clay' AND handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS clay_minus_handicap_profit,

    COUNTIF(surface = 'Grass') AS grass_matches,
    SUM(IF(surface = 'Grass', match_win_profit, 0)) AS grass_match_win_profit,
    COUNTIF(surface = 'Grass' AND handicap_line > 0 AND handicap_odds IS NOT NULL) AS grass_plus_handicap_matches,
    SUM(IF(surface = 'Grass' AND handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS grass_plus_handicap_profit,
    COUNTIF(surface = 'Grass' AND handicap_line < 0 AND handicap_odds IS NOT NULL) AS grass_minus_handicap_matches,
    SUM(IF(surface = 'Grass' AND handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS grass_minus_handicap_profit,

    COUNTIF(surface = 'Hard') AS hard_matches,
    SUM(IF(surface = 'Hard', match_win_profit, 0)) AS hard_match_win_profit,
    COUNTIF(surface = 'Hard' AND handicap_line > 0 AND handicap_odds IS NOT NULL) AS hard_plus_handicap_matches,
    SUM(IF(surface = 'Hard' AND handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS hard_plus_handicap_profit,
    COUNTIF(surface = 'Hard' AND handicap_line < 0 AND handicap_odds IS NOT NULL) AS hard_minus_handicap_matches,
    SUM(IF(surface = 'Hard' AND handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS hard_minus_handicap_profit,

    COUNTIF(surface = 'Indoor Hard') AS indoor_hard_matches,
    SUM(IF(surface = 'Indoor Hard', match_win_profit, 0)) AS indoor_hard_match_win_profit,
    COUNTIF(surface = 'Indoor Hard' AND handicap_line > 0 AND handicap_odds IS NOT NULL) AS indoor_hard_plus_handicap_matches,
    SUM(IF(surface = 'Indoor Hard' AND handicap_line > 0 AND handicap_odds IS NOT NULL, plus_handicap_profit, 0)) AS indoor_hard_plus_handicap_profit,
    COUNTIF(surface = 'Indoor Hard' AND handicap_line < 0 AND handicap_odds IS NOT NULL) AS indoor_hard_minus_handicap_matches,
    SUM(IF(surface = 'Indoor Hard' AND handicap_line < 0 AND handicap_odds IS NOT NULL, minus_handicap_profit, 0)) AS indoor_hard_minus_handicap_profit,

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
  GROUP BY player_name
),

-- Cluster-specific ROI calculations remain grouped
roi_aggregations as (
  SELECT
    player_name,
    opponent_rally_cluster,
    opponent_net_cluster,
    opponent_serve_cluster,

    -- Match win metrics
    COUNT(*) AS total_matches,
    SUM(match_win_profit) AS total_match_win_profit,

    -- Plus handicap metrics
    COUNTIF(handicap_line > 0 AND handicap_odds IS NOT NULL) AS plus_handicap_matches,
    SUM(plus_handicap_profit) AS total_plus_handicap_profit,

    -- Minus handicap metrics
    COUNTIF(handicap_line < 0 AND handicap_odds IS NOT NULL) AS minus_handicap_matches,
    SUM(minus_handicap_profit) AS total_minus_handicap_profit
  FROM bet_calculations
  GROUP BY player_name, opponent_rally_cluster, opponent_net_cluster, opponent_serve_cluster
),

pivoted_roi AS (
  SELECT
    r.player_name,

    -- Overall ROIs
    o.overall_total_match_win_profit / NULLIF(o.overall_total_matches, 0) * 100 AS overall_match_win_roi,
    o.overall_total_plus_handicap_profit / NULLIF(o.overall_plus_handicap_matches, 0) * 100 AS overall_plus_handicap_roi,
    o.overall_total_minus_handicap_profit / NULLIF(o.overall_minus_handicap_matches, 0) * 100 AS overall_minus_handicap_roi,

    -- NEW: Against left-handed opponents
    o.vs_left_handed_match_win_profit / NULLIF(o.vs_left_handed_matches, 0) * 100 AS vs_left_handed_match_roi,
    o.vs_left_handed_plus_handicap_profit / NULLIF(o.vs_left_handed_plus_handicap_matches, 0) * 100 AS vs_left_handed_plus_handicap_roi,
    o.vs_left_handed_minus_handicap_profit / NULLIF(o.vs_left_handed_minus_handicap_matches, 0) * 100 AS vs_left_handed_minus_handicap_roi,

    -- Surface ROIs
    -- Clay
    o.clay_match_win_profit / NULLIF(o.clay_matches, 0) * 100 AS clay_match_win_roi,
    o.clay_plus_handicap_profit / NULLIF(o.clay_plus_handicap_matches, 0) * 100 AS clay_plus_handicap_roi,
    o.clay_minus_handicap_profit / NULLIF(o.clay_minus_handicap_matches, 0) * 100 AS clay_minus_handicap_roi,

    -- Grass
    o.grass_match_win_profit / NULLIF(o.grass_matches, 0) * 100 AS grass_match_win_roi,
    o.grass_plus_handicap_profit / NULLIF(o.grass_plus_handicap_matches, 0) * 100 AS grass_plus_handicap_roi,
    o.grass_minus_handicap_profit / NULLIF(o.grass_minus_handicap_matches, 0) * 100 AS grass_minus_handicap_roi,

    -- Hard
    o.hard_match_win_profit / NULLIF(o.hard_matches, 0) * 100 AS hard_match_win_roi,
    o.hard_plus_handicap_profit / NULLIF(o.hard_plus_handicap_matches, 0) * 100 AS hard_plus_handicap_roi,
    o.hard_minus_handicap_profit / NULLIF(o.hard_minus_handicap_matches, 0) * 100 AS hard_minus_handicap_roi,

    -- Indoor Hard
    o.indoor_hard_match_win_profit / NULLIF(o.indoor_hard_matches, 0) * 100 AS indoor_hard_match_win_roi,
    o.indoor_hard_plus_handicap_profit / NULLIF(o.indoor_hard_plus_handicap_matches, 0) * 100 AS indoor_hard_plus_handicap_roi,
    o.indoor_hard_minus_handicap_profit / NULLIF(o.indoor_hard_minus_handicap_matches, 0) * 100 AS indoor_hard_minus_handicap_roi,

    -- Grand Slam ROIs from separate CTE
    o.grand_slam_match_win_profit / NULLIF(o.grand_slam_matches, 0) * 100 AS grand_slam_match_win_roi,
    o.grand_slam_plus_handicap_profit / NULLIF(o.grand_slam_matches, 0) * 100 AS grand_slam_plus_handicap_roi,
    o.grand_slam_minus_handicap_profit / NULLIF(o.grand_slam_matches, 0) * 100 AS grand_slam_minus_handicap_roi,

    -- Home Country ROIs from separate CTE
    o.home_match_win_profit / NULLIF(o.home_matches, 0) * 100 AS home_match_win_roi,
    o.home_plus_handicap_profit / NULLIF(o.home_matches, 0) * 100 AS home_plus_handicap_roi,
    o.home_minus_handicap_profit / NULLIF(o.home_matches, 0) * 100 AS home_minus_handicap_roi,

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
    SUM(IF(opponent_serve_cluster = 5, minus_handicap_matches, 0)) AS minus_handicap_vs_serve5

FROM roi_aggregations r
  JOIN overall_roi o ON r.player_name = o.player_name
  GROUP BY
    r.player_name,
    o.overall_total_match_win_profit,
    o.overall_total_matches,
    o.overall_total_plus_handicap_profit,
    o.overall_plus_handicap_matches,
    o.overall_total_minus_handicap_profit,
    o.overall_minus_handicap_matches,
    -- Include all new aggregation columns in GROUP BY
    o.vs_left_handed_match_win_profit,
    o.vs_left_handed_matches,
    o.vs_left_handed_plus_handicap_profit,
    o.vs_left_handed_plus_handicap_matches,
    o.vs_left_handed_minus_handicap_profit,
    o.vs_left_handed_minus_handicap_matches,
    o.clay_match_win_profit,
    o.clay_matches,
    o.clay_plus_handicap_profit,
    o.clay_plus_handicap_matches,
    o.clay_minus_handicap_profit,
    o.clay_minus_handicap_matches,

    o.grass_match_win_profit,
    o.grass_matches,
    o.grass_plus_handicap_profit,
    o.grass_plus_handicap_matches,
    o.grass_minus_handicap_profit,
    o.grass_minus_handicap_matches,

    o.hard_match_win_profit,
    o.hard_matches,
    o.hard_plus_handicap_profit,
    o.hard_plus_handicap_matches,
    o.hard_minus_handicap_profit,
    o.hard_minus_handicap_matches,

    o.indoor_hard_match_win_profit,
    o.indoor_hard_matches,
    o.indoor_hard_plus_handicap_profit,
    o.indoor_hard_plus_handicap_matches,
    o.indoor_hard_minus_handicap_profit,
    o.indoor_hard_minus_handicap_matches,

    o.grand_slam_match_win_profit,
    o.grand_slam_matches,
    o.grand_slam_plus_handicap_profit,
    o.grand_slam_minus_handicap_profit,
    o.home_match_win_profit,
    o.home_matches,
    o.home_plus_handicap_profit,
    o.home_minus_handicap_profit
)

-- Final selection with all columns
SELECT *
FROM pivoted_roi