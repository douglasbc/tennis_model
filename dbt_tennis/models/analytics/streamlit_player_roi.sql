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
  *,
from {{ ref('atp_roi') }}
where player_name in (select * from atp_bets_players)
union all
select
  'WTA' as tour,
  *,
  null as roi_vs_net4_match,
  null as roi_vs_net4_plus_handicap,
  null as roi_vs_net4_minus_handicap,
  null as roi_vs_serve5_match,
  null as roi_vs_serve5_plus_handicap,
  null as roi_vs_serve5_minus_handicap
from {{ ref('wta_roi') }}
where player_name in (select * from wta_bets_players)