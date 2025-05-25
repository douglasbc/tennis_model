{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

stats as (
  select * from {{ source('raw_layer', 'stats_wta') }}
),

final as (
    select    
      to_hex(md5(concat(player_1_id, player_2_id, tournament_id, round_id))) as match_id,
      player_1_id,
      player_2_id,
      tournament_id,
      round_id,
      p1_first_serve_attempts as p1_service_points_played,
      (p1_total_points - p1_return_points_won) as p1_service_points_won,
      p2_first_serve_attempts as p1_return_points_played,
      p1_return_points_won,
      p1_aces,
      p1_winners,
      p1_unforced_errors,
      p2_first_serve_attempts as p2_service_points_played,
      (p2_total_points - p2_return_points_won) as p2_service_points_won,
      p1_first_serve_attempts as p2_return_points_played,
      p2_return_points_won,
      p2_aces,
      p2_winners,
      p2_unforced_errors,
      if(match_duration = 'nan', null, div(cast(match_duration as integer), 60)) as match_duration_minutes
    from stats
)

select * from final