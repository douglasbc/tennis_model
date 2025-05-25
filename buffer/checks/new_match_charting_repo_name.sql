select
   mc.player_name as mc_name,
   oc.player_name as oc_name
from (select distinct coalesce(fix.oncourt_name, s.p2_name) as player_name
     from raw_layer.atp_match_charting_repo_stats as s
     left join raw_layer.fix_player_names as fix
       on s.p2_name = fix.match_charting_project_name
     ) as mc
full outer join treated_layer.atp_players as oc
   on mc.player_name = oc.player_name
where oc.player_name is null