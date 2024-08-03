{{ config(
    materialized = 'table',
    schema = 'analytics_layer',
    partition_by = {
      "field": "match_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with 

matches_wta as (
  select
    to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
    *
  from {{ source('raw_layer', 'matches_wta') }}
),

wta_players as (
  select * from {{ ref('wta_players') }}
),

wta_tournaments as (
  select * from {{ ref('wta_tournaments') }}
),

historical_weather as (
  select * from {{ ref('historical_weather') }}
),

wta_entry as (
  select * from {{ ref('wta_entry') }}
),

wta_stats as (
  select * from {{ ref('wta_stats') }}
),

wta_odds as (
  select * from {{ ref('wta_odds') }}
),

rankings_wta as (
  select * from {{ source('raw_layer', 'rankings_wta') }}
),

rounds as (
  select * from {{ source('raw_layer', 'rounds') }}
),

final as (
    select
      m.match_id,
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
      t.tournament_elevation,
      w.avg_apparent_temperature,
      w.avg_relative_humidity,
      w.avg_wind_speed,
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
      o.p1_win_match_odds,
      o.p2_win_match_odds,
      o.total_line,
      o.under_odds,
      o.over_odds,
      o.p1_handicap_line,
      safe_multiply(o.p1_handicap_line, -1) as p2_handicap_line,
      o.p1_handicap_odds,
      o.p2_handicap_odds
    from matches_wta as m
      inner join wta_players as p1 on m.player_1_id = p1.player_id
      inner join wta_players as p2 on m.player_2_id = p2.player_id
      inner join wta_tournaments as t on m.tournament_id = t.tournament_id
      left join historical_weather as w on (m.match_date = w.local_date and t.geo_id = w.geo_id)
      left join wta_entry as e1 on (m.player_1_id = e1.player_id and m.tournament_id = e1.tournament_id)
      left join wta_entry as e2 on (m.player_2_id = e2.player_id and m.tournament_id = e2.tournament_id)
      left join wta_stats as s on m.match_id = s.match_id
      left join wta_odds as o on m.match_id = o.match_id
      left join rankings_wta as r1 on m.player_1_id = r1.player_id
        and m.match_date between r1.ranking_date and date_add(r1.ranking_date, interval 6 day)
      left join rankings_wta as r2 on m.player_2_id = r2.player_id
        and m.match_date between r2.ranking_date and date_add(r2.ranking_date, interval 6 day)
      left join rounds as r on m.round_id = r.round_id
    where extract(year from m.match_date) >= 2015
)

select * from final
