with

fix_player_names as (
  select * from {{ source('raw_layer', 'fix_player_names') }}
)


select
   mc.player_name as mc_name,
   oc.player_name as oc_name
from (select distinct coalesce(fix.oncourt_name, s.p1_name) as player_name
     from (select * from {{ source('raw_layer', 'atp_match_charting_repo_stats') }} where match_date >= '2015-01-01') as s
     left join fix_player_names as fix
       on s.p1_name = fix.match_charting_project_name
     ) as mc
full outer join {{ ref('atp_players') }} as oc
   on mc.player_name = oc.player_name
where oc.player_name is null