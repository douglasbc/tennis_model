{{ config(
    materialized = 'table',
    schema = 'silver'
)}}

with

matches_wta as (
  select
    to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
    match_date,
    result
  from {{ source('raw_layer', 'matches_wta') }}
),

wta_players as (
  select *
  from {{ ref('wta_players') }}
),

fix_player_names as (
  select *
  from {{ source('raw_layer', 'fix_player_names') }}
),

oncourt_stats as (
  select
    *,
    to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
    p1_total_points + p2_total_points as match_total_points
  from {{ source('raw_layer', 'stats_wta') }}
  where p1_total_points is not null and p2_total_points is not null
),

wta_match_charting_repo_stats as (
  select
    match_date,
    coalesce(fix1.oncourt_name, mc.p1_name) as player_1_name,
    coalesce(fix2.oncourt_name, mc.p2_name) as player_2_name,
    p1_winners,
    p1_unforced as p1_unforced_errors,
    p1_net_pts_won as p1_net_points_won,
    p1_net_pts as p1_net_points_played,
    p2_winners,
    p2_unforced as p2_unforced_errors,
    p2_net_pts_won as p2_net_points_won,
    p2_net_pts as p2_net_points_played
  from {{ source('raw_layer', 'wta_match_charting_repo_stats') }} as mc
  left join fix_player_names as fix1 on fix1.match_charting_project_name = mc.p1_name
  left join fix_player_names as fix2 on fix2.match_charting_project_name = mc.p2_name
),

final as (
  select
    o.match_id,
    m.match_date,
    player_1_id,
    p1.player_name as player_1_name,
    player_2_id,
    p2.player_name as player_2_name,
    tournament_id,
    round_id,
    match_total_points,
    p1_first_serve_attempts as p1_service_points_played,
    (p1_total_points - p1_return_points_won) as p1_service_points_won,
    p2_first_serve_attempts as p1_return_points_played,
    p1_return_points_won,
    p1_aces,
    p1_double_faults,
    COALESCE(o.p1_winners, mc1.p1_winners, mc2.p2_winners) as p1_winners,
    COALESCE(o.p1_unforced_errors, mc1.p1_unforced_errors, mc2.p2_unforced_errors) as p1_unforced_errors,
    COALESCE(o.p1_net_points_won, mc1.p1_net_points_won, mc2.p2_net_points_won) as p1_net_points_won,
    COALESCE(o.p1_net_points_played, mc1.p1_net_points_played, mc2.p2_net_points_played) as p1_net_points_played,
    p2_first_serve_attempts as p2_service_points_played,
    (p2_total_points - p2_return_points_won) as p2_service_points_won,
    p1_first_serve_attempts as p2_return_points_played,
    p2_return_points_won,
    p2_aces,
    p2_double_faults,
    COALESCE(o.p2_winners, mc1.p2_winners, mc2.p1_winners) as p2_winners,
    COALESCE(o.p2_unforced_errors, mc1.p2_unforced_errors, mc2.p1_winners) as p2_unforced_errors,
    COALESCE(o.p2_net_points_won, mc1.p2_net_points_won, mc2.p1_winners) as p2_net_points_won,
    COALESCE(o.p2_net_points_played, mc1.p2_net_points_played, mc2.p1_winners) as p2_net_points_played,
  from (select * from oncourt_stats where match_total_points > 0) as o
  join (select * from matches_wta where regexp_extract(result, r'([a-zA-Z]+)') is null) as m on m.match_id = o.match_id
  left join wta_players as p1 on p1.player_id = o.player_1_id
  left join wta_players as p2 on p2.player_id = o.player_2_id
  left join wta_match_charting_repo_stats as mc1
    on mc1.match_date = m.match_date and mc1.player_1_name = p1.player_name and mc1.player_2_name = p2.player_name
  left join wta_match_charting_repo_stats as mc2
    on mc2.match_date = m.match_date and mc2.player_1_name = p2.player_name and mc2.player_2_name = p1.player_name
)

select * from final where player_1_name is not null and player_2_name is not null
