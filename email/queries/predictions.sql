select
  'atp' as circuit,
  *
from `tennis-358702.raw_layer.atp_predictions`
union all
select
  'wta' as circuit,
  *
from `tennis-358702.raw_layer.wta_predictions`

order by circuit, tournament_name
