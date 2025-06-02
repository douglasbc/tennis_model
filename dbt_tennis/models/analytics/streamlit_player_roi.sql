{{
    config(
        materialized = 'table',
        schema = 'analytics',
    )
}}

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
)

select
  'ATP' as tour,
  player_name,
  overall_match_win_roi,
  clay_match_win_roi,
  grass_match_win_roi,
  hard_match_win_roi,
  indoor_hard_match_win_roi,
  grand_slam_match_win_roi,
  vs_left_handed_match_roi,
  roi_vs_rally1_match,
  roi_vs_rally2_match,
  roi_vs_rally3_match,
  roi_vs_net1_match,
  roi_vs_net2_match,
  roi_vs_net3_match,
  roi_vs_serve1_match,
  roi_vs_serve2_match,
  roi_vs_serve3_match
from {{ ref('atp_roi') }}
where player_name in (select * from atp_bets_players)
union all
select
  'WTA' as tour,
  player_name,
  overall_match_win_roi,
  clay_match_win_roi,
  grass_match_win_roi,
  hard_match_win_roi,
  indoor_hard_match_win_roi,
  grand_slam_match_win_roi,
  vs_left_handed_match_roi,
  roi_vs_rally1_match,
  roi_vs_rally2_match,
  roi_vs_rally3_match,
  roi_vs_net1_match,
  roi_vs_net2_match,
  roi_vs_net3_match,
  roi_vs_serve1_match,
  roi_vs_serve2_match,
  roi_vs_serve3_match
from {{ ref('wta_roi') }}
where player_name in (select * from wta_bets_players)