{{ config(
    materialized = 'table',
    schema = 'silver'
)}}

with

odds as (
  select * from {{ source('raw_layer', 'odds_wta') }}
),

pinnacle_odds as (
    select
      to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id_1,
      to_hex(md5(concat(player_2_id, player_1_id, tournament_id, round_id))) as match_id_2,
      player_1_id,
      player_2_id,
      tournament_id,
      round_id,
      p1_win_match_odds,
      p2_win_match_odds,
--       total_line,
--       under_odds,
--       over_odds,
      case
        when p1_handicap_line in (-1.5, 1.5) then null else p1_handicap_line
      end as p1_handicap_line,
      case
        when p2_handicap_line in (-1.5, 1.5) then null else p2_handicap_line
      end as p2_handicap_line,
      case
        when p1_handicap_line in (-1.5, 1.5) then null
        when p1_handicap_odds between 1.7 and 2.25 then p1_handicap_odds
      end as p1_handicap_odds,
      case
        when p2_handicap_line in (-1.5, 1.5) then null
        when p2_handicap_odds between 1.7 and 2.25 then p2_handicap_odds
      end as p2_handicap_odds,
      p1_2_0_sets_odds,
      p2_2_0_sets_odds
    from odds
    where bookie_id = 2
),

marathon_odds as (
    select
      to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id_1,
      to_hex(md5(concat(player_2_id, player_1_id, tournament_id, round_id))) as match_id_2,
      player_1_id,
      player_2_id,
      tournament_id,
      round_id,
      p1_win_match_odds,
      p2_win_match_odds,
--       total_line,
--       under_odds,
--       over_odds,
      p1_handicap_line,
      p2_handicap_line,
      case
        when p1_handicap_odds between 1.7 and 2.25 then p1_handicap_odds
      end as p1_handicap_odds,
      case
        when p2_handicap_odds between 1.7 and 2.25 then p2_handicap_odds
      end as p2_handicap_odds,
      p1_2_0_sets_odds,
      p2_2_0_sets_odds
    from odds
    where bookie_id = 1
),

final as (
    select
      p.match_id_1,
      p.match_id_2,
      coalesce(p.player_1_id, m1.player_1_id, m2.player_2_id) as player_1_id,
      coalesce(p.player_2_id, m1.player_2_id, m2.player_1_id) as player_2_id,
      coalesce(p.round_id, m1.round_id, m2.round_id) as round_id,
      coalesce(p.p1_win_match_odds, m1.p1_win_match_odds, m2.p2_win_match_odds) as p1_win_match_odds,
      coalesce(p.p2_win_match_odds, m1.p2_win_match_odds, m2.p1_win_match_odds) as p2_win_match_odds,
--       coalesce(p.total_line, m1.total_line) as total_line,
--       coalesce(p.under_odds, m1.under_odds) as under_odds,
--       coalesce(p.over_odds, m1.over_odds) as over_odds,
      coalesce(p.p1_handicap_line, m1.p1_handicap_line, m2.p2_handicap_line) as p1_handicap_line,
      coalesce(p.p2_handicap_line, m1.p2_handicap_line, m2.p1_handicap_line) as p2_handicap_line,
      coalesce(p.p1_handicap_odds, m1.p1_handicap_odds, m2.p2_handicap_odds) as p1_handicap_odds,
      coalesce(p.p2_handicap_odds, m1.p2_handicap_odds, m2.p1_handicap_odds) as p2_handicap_odds,
      coalesce(p.p1_2_0_sets_odds, m1.p1_2_0_sets_odds, m2.p2_2_0_sets_odds) as p1_2_0_sets_odds,
      coalesce(p.p2_2_0_sets_odds, m1.p2_2_0_sets_odds, m2.p1_2_0_sets_odds) as p2_2_0_sets_odds
    from pinnacle_odds as p
    left join marathon_odds as m1
      on p.match_id_1 = m1.match_id_1
    left join marathon_odds as m2
      on p.match_id_1 = m2.match_id_2
)

select * from final