{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with odds as (
  select * from {{ source('raw_layer', 'odds_wta') }}
),

final as (
    select    
      player_1_id,
      player_2_id,
      tournament_id,
      round_id,
      p1_win_match_odds,
      p2_win_match_odds,
      total_line,
      under_odds,
      over_odds,
      p1_handicap_line,
      p2_handicap_line,
      p1_handicap_odds,
      p2_handicap_odds
    from odds
    where bookie_id = 1
)

select * from final
