select
    mcp.player_name as mcp_player_name,
    p.player_name as p_player_name,
    p.player_id
from {{ ref('atp_mcp_career') }} as mcp
  left join {{ ref('atp_players') }} as p
    on mcp.player_name = p.player_name
where p.player_name is null

UNION ALL

select
    mcp.player_name as mcp_player_name,
    p.player_name as p_player_name,
    p.player_id
from {{ ref('atp_mcp_last_52') }} as mcp
  left join {{ ref('atp_players') }} as p
    on mcp.player_name = p.player_name
where p.player_name is null

UNION ALL

select
    mcp.player_name as mcp_player_name,
    p.player_name as p_player_name,
    p.player_id
from {{ ref('wta_mcp_career') }} as mcp
  left join {{ ref('wta_players') }} as p
    on mcp.player_name = p.player_name
where p.player_name is null

UNION ALL

select
    mcp.player_name as mcp_player_name,
    p.player_name as p_player_name,
    p.player_id
from {{ ref('wta_mcp_last_52') }} as mcp
  left join {{ ref('wta_players') }} as p
    on mcp.player_name = p.player_name
where p.player_name is null