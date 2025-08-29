{{
    config(
        materialized = 'table',
        schema = 'analytics',
        partition_by = {
            "field": "player_name",
            "data_type": "string"
        }
    )
}}

with atp_matches as (
    select *
    from {{ ref('atp_matches') }}
    where
        regexp_extract(result, r'([a-zA-Z]+)') is null
        and tournament_tier not in ('Laver Cup', 'Next Gen ATP Finals')
        and match_date >= '2021-01-01'
        and p1_win_match_odds is not null
        and p2_win_match_odds is not null
        and (p1_name = 'Marin Cilic' or p2_name = 'Marin Cilic')
        and match_date in ('2024-11-03', '2024-11-04', '2024-11-05', '2024-11-06')

),

match_data as (
    -- Player 1 perspective
    select
        match_id,
        tournament_name,
        surface,
        tournament_tier,
        tournament_country,
        p1_name as player_name,
        p1_country as player_country,
        p1_is_left_handed,
        p1_rally_aggression_cluster as player_rally_cluster,
        p1_net_points_cluster as player_net_cluster,
        p1_serve_dependency_cluster as player_serve_cluster,
        p2_name as opponent_name,
        p2_country as opponent_country,
        p2_is_left_handed as opponent_is_left_handed,
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

    -- Player 2 perspective
    select
        match_id,
        tournament_name,
        surface,
        tournament_tier,
        tournament_country,
        p2_name as player_name,
        p2_country as player_country,
        p2_is_left_handed,
        p2_rally_aggression_cluster as player_rally_cluster,
        p2_net_points_cluster as player_net_cluster,
        p2_serve_dependency_cluster as player_serve_cluster,
        p1_name as opponent_name,
        p1_country as opponent_country,
        p1_is_left_handed as opponent_is_left_handed,
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

-- NEW DEDICATED HANDICAP CALCULATION CTE
handicap_calculations as (
    select
        *,
        -- Calculate handicap outcome and profit
        case
            when (games_won + handicap_line) > games_against then handicap_odds - 1  -- Win
            when (games_won + handicap_line) < games_against then -1                 -- Loss
        end as handicap_profit,
        case when tournament_tier = 'Grand Slam' then 1 else 0 end as is_grand_slam,
        case when player_country = tournament_country then 1 else 0 end as is_home_country
    from match_data
    where 
        handicap_line is not null 
        and handicap_odds is not null
        and (games_won + handicap_line) <> 0
),

bet_calculations as (
    select
        *,
        -- Match win ROI calculation
        (is_win * match_win_odds) - 1 as match_win_profit,
        
        -- Flags for special conditions
        case when tournament_tier = 'Grand Slam' then 1 else 0 end as is_grand_slam,
        case when player_country = tournament_country then 1 else 0 end as is_home_country
    from match_data
),

-- SEPARATE CTEs FOR DIFFERENT BET TYPES
match_win_bets as (
    select
        player_name,
        opponent_is_left_handed,
        surface,
        is_grand_slam,
        is_home_country,
        opponent_rally_cluster,
        opponent_net_cluster,
        opponent_serve_cluster,
        match_win_profit as profit
    from bet_calculations
    where match_win_odds is not null
),

-- HANDICAP BETS FROM DEDICATED CTE
handicap_bets as (
    select
        player_name,
        opponent_is_left_handed,
        surface,
        is_grand_slam,
        is_home_country,
        opponent_rally_cluster,
        opponent_net_cluster,
        opponent_serve_cluster,
        handicap_line,
        handicap_profit as profit
    from handicap_calculations
),

-- ROI CALCULATION FOR OVERALL, SURFACE, AND SPECIAL CONDITIONS
roi_calculator as (
    select
        player_name,
        bet_type,

        -- Overall metrics
        count(*) as total_matches,
        sum(profit) as total_profit,

        -- Surface metrics
        countif(surface = 'Clay') as clay_matches,
        sum(if(surface = 'Clay', profit, 0)) as clay_profit,

        countif(surface = 'Grass') as grass_matches,
        sum(if(surface = 'Grass', profit, 0)) as grass_profit,

        countif(surface = 'Hard') as hard_matches,
        sum(if(surface = 'Hard', profit, 0)) as hard_profit,

        countif(surface = 'Indoor Hard') as indoor_hard_matches,
        sum(if(surface = 'Indoor Hard', profit, 0)) as indoor_hard_profit,

        -- Left-handed opponents
        countif(opponent_is_left_handed) as vs_left_handed_matches,
        sum(if(opponent_is_left_handed, profit, 0)) as vs_left_handed_profit,

        -- Grand Slams
        countif(is_grand_slam = 1) as grand_slam_matches,
        sum(if(is_grand_slam = 1, profit, 0)) as grand_slam_profit,

        -- Home country
        countif(is_home_country = 1) as home_matches,
        sum(if(is_home_country = 1, profit, 0)) as home_profit
    from (
        select *, 'match_win' as bet_type from match_win_bets
        union all
        select * except(handicap_line),
            case 
                when handicap_line > 0 then 'plus_handicap' 
                when handicap_line < 0 then 'minus_handicap' 
            end as bet_type 
        from handicap_bets
    )
    group by player_name, bet_type
),

-- CLUSTER-SPECIFIC ROI CALCULATIONS
cluster_roi_calculator as (
    select
        player_name,
        bet_type,
        opponent_rally_cluster,
        opponent_net_cluster,
        opponent_serve_cluster,
        count(*) as total_matches,
        sum(profit) as total_profit
    from (
        select
            player_name,
            opponent_rally_cluster,
            opponent_net_cluster,
            opponent_serve_cluster,
            profit,
            'match_win' as bet_type
        from match_win_bets

        union all

        select
            player_name,
            opponent_rally_cluster,
            opponent_net_cluster,
            opponent_serve_cluster,
            profit,
            case 
                when handicap_line > 0 then 'plus_handicap' 
                when handicap_line < 0 then 'minus_handicap' 
            end as bet_type
        from handicap_bets
    )
    group by player_name, bet_type, opponent_rally_cluster, opponent_net_cluster, opponent_serve_cluster
),

-- PIVOTED ROI FOR OVERALL, SURFACE, AND SPECIAL CONDITIONS
pivoted_roi as (
    select
        player_name,

        -- Match Win ROIs
        max(if(bet_type = 'match_win', total_profit / nullif(total_matches, 0) * 100, null)) as overall_match_win_roi,
        max(if(bet_type = 'match_win', vs_left_handed_profit / nullif(vs_left_handed_matches, 0) * 100, null)) as vs_left_handed_match_roi,
        max(if(bet_type = 'match_win', clay_profit / nullif(clay_matches, 0) * 100, null)) as clay_match_win_roi,
        max(if(bet_type = 'match_win', grass_profit / nullif(grass_matches, 0) * 100, null)) as grass_match_win_roi,
        max(if(bet_type = 'match_win', hard_profit / nullif(hard_matches, 0) * 100, null)) as hard_match_win_roi,
        max(if(bet_type = 'match_win', indoor_hard_profit / nullif(indoor_hard_matches, 0) * 100, null)) as indoor_hard_match_win_roi,
        max(if(bet_type = 'match_win', grand_slam_profit / nullif(grand_slam_matches, 0) * 100, null)) as grand_slam_match_win_roi,
        max(if(bet_type = 'match_win', home_profit / nullif(home_matches, 0) * 100, null)) as home_match_win_roi,

        -- Plus Handicap ROIs
        max(if(bet_type = 'plus_handicap', total_profit / nullif(total_matches, 0) * 100, null)) as overall_plus_handicap_roi,
        max(if(bet_type = 'plus_handicap', vs_left_handed_profit / nullif(vs_left_handed_matches, 0) * 100, null)) as vs_left_handed_plus_handicap_roi,
        max(if(bet_type = 'plus_handicap', clay_profit / nullif(clay_matches, 0) * 100, null)) as clay_plus_handicap_roi,
        max(if(bet_type = 'plus_handicap', grass_profit / nullif(grass_matches, 0) * 100, null)) as grass_plus_handicap_roi,
        max(if(bet_type = 'plus_handicap', hard_profit / nullif(hard_matches, 0) * 100, null)) as hard_plus_handicap_roi,
        max(if(bet_type = 'plus_handicap', indoor_hard_profit / nullif(indoor_hard_matches, 0) * 100, null)) as indoor_hard_plus_handicap_roi,

        -- Minus Handicap ROIs
        max(if(bet_type = 'minus_handicap', total_profit / nullif(total_matches, 0) * 100, null)) as overall_minus_handicap_roi,
        max(if(bet_type = 'minus_handicap', vs_left_handed_profit / nullif(vs_left_handed_matches, 0) * 100, null)) as vs_left_handed_minus_handicap_roi,
        max(if(bet_type = 'minus_handicap', clay_profit / nullif(clay_matches, 0) * 100, null)) as clay_minus_handicap_roi,
        max(if(bet_type = 'minus_handicap', grass_profit / nullif(grass_matches, 0) * 100, null)) as grass_minus_handicap_roi,
        max(if(bet_type = 'minus_handicap', hard_profit / nullif(hard_matches, 0) * 100, null)) as hard_minus_handicap_roi,
        max(if(bet_type = 'minus_handicap', indoor_hard_profit / nullif(indoor_hard_matches, 0) * 100, null)) as indoor_hard_minus_handicap_roi
    from roi_calculator
    group by player_name
),

-- PIVOTED CLUSTER-SPECIFIC ROIS
cluster_pivot as (
    select
        player_name,
        -- Rally Aggression Clusters (1-4)
        -- Cluster 1
        sum(if(opponent_rally_cluster = 1 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 1 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_rally1_match,
        sum(if(opponent_rally_cluster = 1 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 1 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally1_plus_handicap,
        sum(if(opponent_rally_cluster = 1 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 1 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally1_minus_handicap,

        -- Cluster 2
        sum(if(opponent_rally_cluster = 2 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 2 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_rally2_match,
        sum(if(opponent_rally_cluster = 2 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 2 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally2_plus_handicap,
        sum(if(opponent_rally_cluster = 2 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 2 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally2_minus_handicap,

        -- Cluster 3
        sum(if(opponent_rally_cluster = 3 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 3 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_rally3_match,
        sum(if(opponent_rally_cluster = 3 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 3 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally3_plus_handicap,
        sum(if(opponent_rally_cluster = 3 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 3 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally3_minus_handicap,

        -- Cluster 4
        sum(if(opponent_rally_cluster = 4 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 4 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_rally4_match,
        sum(if(opponent_rally_cluster = 4 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 4 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally4_plus_handicap,
        sum(if(opponent_rally_cluster = 4 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_rally_cluster = 4 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_rally4_minus_handicap,

        -- Net Points Clusters (1-4)
        -- Cluster 1
        sum(if(opponent_net_cluster = 1 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 1 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_net1_match,
        sum(if(opponent_net_cluster = 1 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 1 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net1_plus_handicap,
        sum(if(opponent_net_cluster = 1 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 1 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net1_minus_handicap,

        -- Cluster 2
        sum(if(opponent_net_cluster = 2 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 2 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_net2_match,
        sum(if(opponent_net_cluster = 2 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 2 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net2_plus_handicap,
        sum(if(opponent_net_cluster = 2 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 2 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net2_minus_handicap,

        -- Cluster 3
        sum(if(opponent_net_cluster = 3 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 3 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_net3_match,
        sum(if(opponent_net_cluster = 3 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 3 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net3_plus_handicap,
        sum(if(opponent_net_cluster = 3 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 3 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net3_minus_handicap,

        -- Cluster 4
        sum(if(opponent_net_cluster = 4 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 4 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_net4_match,
        sum(if(opponent_net_cluster = 4 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 4 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net4_plus_handicap,
        sum(if(opponent_net_cluster = 4 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_net_cluster = 4 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_net4_minus_handicap,

        -- Serve Dependency Clusters (1-5)
        -- Cluster 1
        sum(if(opponent_serve_cluster = 1 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 1 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_serve1_match,
        sum(if(opponent_serve_cluster = 1 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 1 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve1_plus_handicap,
        sum(if(opponent_serve_cluster = 1 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 1 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve1_minus_handicap,

        -- Cluster 2
        sum(if(opponent_serve_cluster = 2 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 2 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_serve2_match,
        sum(if(opponent_serve_cluster = 2 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 2 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve2_plus_handicap,
        sum(if(opponent_serve_cluster = 2 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 2 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve2_minus_handicap,

        -- Cluster 3
        sum(if(opponent_serve_cluster = 3 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 3 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_serve3_match,
        sum(if(opponent_serve_cluster = 3 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 3 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve3_plus_handicap,
        sum(if(opponent_serve_cluster = 3 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 3 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve3_minus_handicap,

        -- Cluster 4
        sum(if(opponent_serve_cluster = 4 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 4 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_serve4_match,
        sum(if(opponent_serve_cluster = 4 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 4 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve4_plus_handicap,
        sum(if(opponent_serve_cluster = 4 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 4 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve4_minus_handicap,

        -- Cluster 5
        sum(if(opponent_serve_cluster = 5 and bet_type = 'match_win', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 5 and bet_type = 'match_win', total_matches, 0)), 0) * 100 as roi_vs_serve5_match,
        sum(if(opponent_serve_cluster = 5 and bet_type = 'plus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 5 and bet_type = 'plus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve5_plus_handicap,
        sum(if(opponent_serve_cluster = 5 and bet_type = 'minus_handicap', total_profit, 0)) / nullif(sum(if(opponent_serve_cluster = 5 and bet_type = 'minus_handicap', total_matches, 0)), 0) * 100 as roi_vs_serve5_minus_handicap
    from cluster_roi_calculator
    group by player_name
),

-- FINAL COMBINED RESULTS
final_roi as (
    select
        p.*,
        c.roi_vs_rally1_match, c.roi_vs_rally1_plus_handicap, c.roi_vs_rally1_minus_handicap,
        c.roi_vs_rally2_match, c.roi_vs_rally2_plus_handicap, c.roi_vs_rally2_minus_handicap,
        c.roi_vs_rally3_match, c.roi_vs_rally3_plus_handicap, c.roi_vs_rally3_minus_handicap,
        c.roi_vs_rally4_match, c.roi_vs_rally4_plus_handicap, c.roi_vs_rally4_minus_handicap,
        c.roi_vs_net1_match, c.roi_vs_net1_plus_handicap, c.roi_vs_net1_minus_handicap,
        c.roi_vs_net2_match, c.roi_vs_net2_plus_handicap, c.roi_vs_net2_minus_handicap,
        c.roi_vs_net3_match, c.roi_vs_net3_plus_handicap, c.roi_vs_net3_minus_handicap,
        c.roi_vs_net4_match, c.roi_vs_net4_plus_handicap, c.roi_vs_net4_minus_handicap,
        c.roi_vs_serve1_match, c.roi_vs_serve1_plus_handicap, c.roi_vs_serve1_minus_handicap,
        c.roi_vs_serve2_match, c.roi_vs_serve2_plus_handicap, c.roi_vs_serve2_minus_handicap,
        c.roi_vs_serve3_match, c.roi_vs_serve3_plus_handicap, c.roi_vs_serve3_minus_handicap,
        c.roi_vs_serve4_match, c.roi_vs_serve4_plus_handicap, c.roi_vs_serve4_minus_handicap,
        c.roi_vs_serve5_match, c.roi_vs_serve5_plus_handicap, c.roi_vs_serve5_minus_handicap
    from pivoted_roi p
    left join cluster_pivot c
    on p.player_name = c.player_name
)

select * from final_roi