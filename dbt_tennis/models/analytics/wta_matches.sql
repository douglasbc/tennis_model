{{ config(
    materialized = 'table',
    schema = 'analytics_layer',
    partition_by = {
      "field": "match_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with matches_wta as (
  select * from {{ source('raw_layer', 'matches_wta') }}
),

wta_players as (
  select * from {{ ref('wta_players') }}
),

wta_tournaments as (
  select * from {{ ref('wta_tournaments') }}
),

wta_entry as (
  select * from {{ ref('wta_entry') }}
),

wta_stats as (
  select * from {{ ref('wta_stats') }}
),

wta_odds_m as (
  select * from {{ ref('wta_odds_m') }}
),

wta_odds_p as (
  select * from {{ ref('wta_odds_p') }}
),

rankings_wta as (
  select * from {{ source('raw_layer', 'rankings_wta') }}
),

rounds as (
  select * from {{ source('raw_layer', 'rounds') }}
),

final as (
    select
      m.match_date,
      p1.player_name as p1_name,
      p2.player_name as p2_name,
      r.round,
      t.tournament_id,
      t.tournament_name,
      t.country as tournament_country,
      case
        when t.surface = 'Carpet' then 'Indoor Hard'
        else t.surface
      end as surface,
      t.tournament_tier,
      p1.country as p1_country,
      p2.country as p2_country,
      if(e1.entry_status is null, 'Direct Acceptance', e1.entry_status) as p1_entry_status,
      e1.seed_number as p1_seed_number,
      if(e2.entry_status is null, 'Direct Acceptance', e2.entry_status) as p2_entry_status,
      e2.seed_number as p2_seed_number,
      r1.ranking_position as p1_ranking,
      r2.ranking_position as p2_ranking,
      m.result,
      s.p1_service_points_played,
      s.p1_service_points_won,
      s.p1_return_points_played,
      s.p1_return_points_won,
      s.p2_service_points_played,
      s.p2_service_points_won,
      s.p2_return_points_played,
      s.p2_return_points_won,
      s.match_duration_minutes,
      coalesce(op.p1_win_match_odds, om.p1_win_match_odds) as p1_win_match_odds,
      coalesce(op.p2_win_match_odds, om.p2_win_match_odds) as p2_win_match_odds,
      coalesce(op.total_line, om.total_line) as total_line,
      coalesce(op.under_odds, om.under_odds) as under_odds,
      coalesce(op.over_odds, om.over_odds) as over_odds,
      coalesce(op.p1_handicap_line, om.p1_handicap_line) as p1_handicap_line,
      coalesce(safe_multiply(op.p1_handicap_line, -1),
               safe_multiply(om.p1_handicap_line, -1)) as p2_handicap_line,
      coalesce(op.p1_handicap_odds, om.p1_handicap_odds) as p1_handicap_odds,
      coalesce(op.p2_handicap_odds, om.p2_handicap_odds) as p2_handicap_odd
    from matches_wta as m
      inner join wta_tournaments as t on m.tournament_id = t.tournament_id
      inner join wta_players as p1 on m.player_1_id = p1.player_id
      inner join wta_players as p2 on m.player_2_id = p2.player_id
      left join wta_entry as e1 on (m.player_1_id = e1.player_id and m.tournament_id = e1.tournament_id)
      left join wta_entry as e2 on (m.player_2_id = e2.player_id and m.tournament_id = e2.tournament_id)
      left join wta_stats as s on (m.player_1_id = s.player_1_id and m.player_2_id = s.player_2_id 
                                  and m.tournament_id = s.tournament_id and m.round_id = s.round_id)
      left join wta_odds_m as om on (m.player_1_id = om.player_1_id and m.player_2_id = om.player_2_id 
                                  and m.tournament_id = om.tournament_id and m.round_id = om.round_id)
      left join wta_odds_p as op on (m.player_1_id = op.player_1_id and m.player_2_id = op.player_2_id 
                                  and m.tournament_id = op.tournament_id and m.round_id = op.round_id)
      left join rankings_wta as r1 on m.player_1_id = r1.player_id
        and m.match_date between r1.ranking_date and date_add(r1.ranking_date, interval 6 day)
      left join rankings_wta as r2 on m.player_2_id = r2.player_id
        and m.match_date between r2.ranking_date and date_add(r2.ranking_date, interval 6 day)
      left join rounds as r on m.round_id = r.round_id
    where extract(year from m.match_date) >= 2015
)

select * from final
