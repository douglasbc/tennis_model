{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

wta_mcp_career as (
  select * from {{ ref('wta_mcp_career') }}
),

wta_mcp_last_52 as (
  select * from {{ ref('wta_mcp_last_52') }}
),

wta_players as (
  select * from {{ ref('wta_players') }}
),

wta_career as (
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
    from wta_mcp_career as mcp
      left join wta_players as p
        on mcp.player_name = p.player_name
    where p.active_since_2015 is true
),

wta_last_52 as (
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
    from wta_mcp_last_52 as l
      left join wta_career as c
        on l.player_name = c.player_name
    where c.player_name is null
),

final as (
  select * from wta_career
  union all
  select * from wta_last_52
)

select * from final
