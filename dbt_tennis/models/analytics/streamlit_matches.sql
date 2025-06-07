{{ config(
    materialized = 'table',
    schema = 'analytics',
)}}

with

atp_bets_players as (
    select p1_name as player_name from {{ ref('atp_bets') }}
    union all
    select p2_name as player_name from {{ ref('atp_bets') }}
),

wta_bets_players as (
    select p1_name as player_name from {{ ref('wta_bets') }}
    union all
    select p2_name as player_name from {{ ref('wta_bets') }}
),

atp_matches as (
    select
        'ATP' as tour, match_date, tournament_name, tournament_tier, tournament_level, round, surface, p1_name, p2_name, result as score, p1_win_match_odds, p2_win_match_odds, p1_ranking, p2_ranking
    from {{ ref('atp_matches') }}
    where
        (p1_name in (select * from atp_bets_players)
       or p2_name in (select * from atp_bets_players))
      and match_date >= '2019-12-27'
        ),

wta_matches as (
    select
        'WTA' as tour, match_date, tournament_name, tournament_tier, tournament_level, round, surface, p1_name, p2_name, result as score, p1_win_match_odds, p2_win_match_odds, p1_ranking, p2_ranking
    from {{ ref('wta_matches') }}
    where
        (p1_name in (select * from wta_bets_players)
       or p2_name in (select * from wta_bets_players))
      and match_date >= '2019-12-27'
        )

select * from atp_matches
union all select * from wta_matches

