{{ config(
    materialized = 'table',
    schema = 'silver',
    partition_by = {
      "field": "match_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with 

matches_atp as (
  select
    to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
    *
  from {{ source('raw_layer', 'matches_atp') }}
),

atp_players as (
  select * from {{ ref('atp_players') }}
),

atp_tournaments as (
  select * from {{ ref('atp_tournaments') }}
),

atp_stats as (
  select * from {{ ref('atp_stats') }}
),

atp_odds as (
  select * from {{ ref('atp_odds') }}
),

atp_rankings as (
  select * from {{ ref('atp_rankings') }}
),

rounds as (
  select * from {{ source('raw_layer', 'rounds') }}
),

rankings_atp as (
  select * from {{ source('raw_layer', 'rankings_atp') }}
),

atp_serve_dependency_clusters as (
  select * from {{ source('raw_layer', 'atp_serve_dependency_clusters') }}
),

atp_rally_aggression_clusters as (
  select * from {{ source('raw_layer', 'atp_rally_aggression_clusters') }}
),

atp_net_points_clusters as (
  select * from {{ source('raw_layer', 'atp_net_points_clusters') }}
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
      t.tournament_level,
--       t.tournament_elevation,
--       w.avg_apparent_temperature,
--       w.avg_relative_humidity,
--       w.avg_wind_speed,
      p1.country as p1_country,
      p2.country as p2_country,
      p1.is_left_handed as p1_is_left_handed,
      p2.is_left_handed as p2_is_left_handed,
--       if(e1.entry_status is null, 'Direct Acceptance', e1.entry_status) as p1_entry_status,
--       e1.seed_number as p1_seed_number,
--       if(e2.entry_status is null, 'Direct Acceptance', e2.entry_status) as p2_entry_status,
--       e2.seed_number as p2_seed_number,
      r1.ranking_position as p1_ranking,
      r2.ranking_position as p2_ranking,
      m.result,
      (
        select sum(cast(number as int64))
        from unnest(regexp_extract_all(regexp_replace(m.result, r'\([^)]*\)', ''), r'(\d+)-')) number
      ) as p1_total_games,
      (
        select sum(cast(number as int64))
        from unnest(regexp_extract_all(regexp_replace(m.result, r'\([^)]*\)', ''), r'-(\d+)')) number
      ) as p2_total_games,
      s.p1_service_points_played,
      s.p1_service_points_won,
      s.p1_return_points_played,
      s.p1_return_points_won,
      s.p2_service_points_played,
      s.p2_service_points_won,
      s.p2_return_points_played,
      s.p2_return_points_won,
--       s.match_duration_minutes,
      coalesce(o1.p1_win_match_odds, o2.p2_win_match_odds) as p1_win_match_odds,
      coalesce(o1.p2_win_match_odds, o2.p1_win_match_odds) as p2_win_match_odds,
--       o.total_line,
--       o.under_odds,
--       o.over_odds,
      coalesce(o1.p1_handicap_line, o2.p2_handicap_line) as p1_handicap_line,
--       safe_multiply(o.p1_handicap_line, -1) as p2_handicap_line,
      coalesce(o1.p2_handicap_line, o2.p1_handicap_line) as p2_handicap_line,
      coalesce(o1.p1_handicap_odds, o2.p2_handicap_odds) as p1_handicap_odds,
      coalesce(o1.p2_handicap_odds, o2.p1_handicap_odds) as p2_handicap_odds,
      cs1.best_cluster as p1_serve_dependency_cluster,
      cs2.best_cluster as p2_serve_dependency_cluster,
      cr1.best_cluster as p1_rally_aggression_cluster,
      cr2.best_cluster as p2_rally_aggression_cluster,
      cn1.best_cluster as p1_net_points_cluster,
      cn2.best_cluster as p2_net_points_cluster
    from matches_atp as m
      inner join atp_players as p1 on m.player_1_id = p1.player_id
      inner join atp_players as p2 on m.player_2_id = p2.player_id
      inner join atp_tournaments as t on m.tournament_id = t.tournament_id
--       left join historical_weather as w on (m.match_date = w.local_date and t.geo_id = w.geo_id)
--       left join atp_entry as e1 on (m.player_1_id = e1.player_id and m.tournament_id = e1.tournament_id)
--       left join atp_entry as e2 on (m.player_2_id = e2.player_id and m.tournament_id = e2.tournament_id)
      left join atp_stats as s on m.match_id = s.match_id
--       left join atp_odds as o on m.match_id = o.match_id
      left join atp_odds as o1 on m.match_id = o1.match_id_1
      left join atp_odds as o2 on m.match_id = o2.match_id_2
      left join atp_rankings as r1 on m.player_1_id = r1.player_id
        and m.match_date between r1.ranking_date and date_add(r1.ranking_date, interval 6 day)
      left join atp_rankings as r2 on m.player_2_id = r2.player_id
        and m.match_date between r2.ranking_date and date_add(r2.ranking_date, interval 6 day)
      left join rounds as r on m.round_id = r.round_id
      left join atp_serve_dependency_clusters as cs1 on p1.player_name = cs1.player_name
      left join atp_serve_dependency_clusters as cs2 on p2.player_name = cs2.player_name
      left join atp_rally_aggression_clusters as cr1 on p1.player_name = cr1.player_name
      left join atp_rally_aggression_clusters as cr2 on p2.player_name = cr2.player_name
      left join atp_net_points_clusters as cn1 on p1.player_name = cn1.player_name
      left join atp_net_points_clusters as cn2 on p2.player_name = cn2.player_name

    where extract(year from m.match_date) >= 2015
)

select distinct * from final
