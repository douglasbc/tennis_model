{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

atp_mcp_career as (
  select * from {{ ref('atp_mcp_career') }}
),

atp_mcp_last_52 as (
  select * from {{ ref('atp_mcp_last_52') }}
),

atp_players as (
  select * from {{ ref('atp_players') }}
),

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
