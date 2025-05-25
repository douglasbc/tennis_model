{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

odds as (
  select * from {{ source('raw_layer', 'odds_wta') }}
),

pinnacle_odds as (
    select    
      to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
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
      p2_handicap_odds,
      p1_2_0_sets_odds,
      p2_2_0_sets_odds
    from odds
    where bookie_id = 2
),

marathon_odds as (
    select    
      to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
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
      p2_handicap_odds,
      p1_2_0_sets_odds,
      p2_2_0_sets_odds
    from odds
    where bookie_id = 1
),

final as (
    select    
      p.match_id,
      coalesce(p.player_1_id, m.player_1_id) as player_1_id,
      coalesce(p.player_2_id, m.player_2_id) as player_2_id,
      coalesce(p.tournament_id, m.tournament_id) as tournament_id,
      coalesce(p.round_id, m.round_id) as round_id,
      coalesce(p.p1_win_match_odds, m.p1_win_match_odds) as p1_win_match_odds,
      coalesce(p.p2_win_match_odds, m.p2_win_match_odds) as p2_win_match_odds,
      coalesce(p.total_line, m.total_line) as total_line,
      coalesce(p.under_odds, m.under_odds) as under_odds,
      coalesce(p.over_odds, m.over_odds) as over_odds,
      coalesce(p.p1_handicap_line, m.p1_handicap_line) as p1_handicap_line,
      coalesce(p.p2_handicap_line, m.p2_handicap_line) as p2_handicap_line,
      coalesce(p.p1_handicap_odds, m.p1_handicap_odds) as p1_handicap_odds,
      coalesce(p.p2_handicap_odds, m.p2_handicap_odds) as p2_handicap_odds,
      coalesce(p.p1_2_0_sets_odds, m.p1_2_0_sets_odds) as p1_2_0_sets_odds,
      coalesce(p.p2_2_0_sets_odds, m.p2_2_0_sets_odds) as p2_2_0_sets_odds
    from pinnacle_odds as p
    left join marathon_odds as m
      on p.match_id = m.match_id
)

select * from final
