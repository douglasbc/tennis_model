{{ config(
    materialized = 'table',
    schema = 'model'
)}}

with 

atp_stats as (
  select
    player_1_id,
    player_1_name,
    p1_winners,
    p1_unforced_errors,
    p1_aces,
    p1_double_faults,
    player_2_id,
    player_2_name,
    p2_winners,
    p2_unforced_errors,
    p2_aces,
    p2_double_faults
  from {{ ref('atp_stats') }}
  where
    player_1_id is not null
    and player_1_name is not null
    and p1_winners is not null
    and p1_unforced_errors is not null
    and p1_aces is not null
    and p1_double_faults is not null
    and player_2_id is not null
    and player_2_name is not null
    and p2_winners is not null
    and p2_unforced_errors is not null
    and p2_aces is not null
    and p2_double_faults is not null
),

stats_union as (
  select
    player_1_id as player_id,
    player_1_name as player_name,
    p1_winners as winners,
    p1_unforced_errors as unforced_errors,
    p1_aces,
    p1_double_faults,


)


atp_career as (
    select
      mcp.player_name,
      mcp.unreturned_pct,
      mcp.rally_agression_score
      -- mcp.second_serve_agression_score,
      -- mcp.returned_pct,
      -- mcp.slice_returns_pct,
      -- mcp.avg_rally_lenght,
      -- mcp.sliced_per_backhand_groundstroke,
      -- mcp.net_points_pct,
      -- mcp.return_agression_score
    from atp_mcp_career as mcp
      left join atp_players as p
        on mcp.player_name = p.player_name
    where p.active_since_2015 is true
),

atp_last_52 as (
    select
      l.player_name,
      l.unreturned_pct,
      l.rally_agression_score
      -- l.second_serve_agression_score,
      -- l.returned_pct,
      -- l.slice_returns_pct,
      -- l.avg_rally_lenght,
      -- l.sliced_per_backhand_groundstroke,
      -- l.net_points_pct,
      -- l.return_agression_score
    from atp_mcp_last_52 as l
      left join atp_career as c
        on l.player_name = c.player_name
    where c.player_name is null
),

final as (
  select * from atp_career
  union all
  select * from atp_last_52
)

select * from final
