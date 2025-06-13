with

fix_player_names as (
  select * from {{ source('raw_layer', 'fix_player_names') }}
),

pinnacle_players as (
  select
    coalesce(f1.oncourt_name, p1_name) as p1_name,
    coalesce(f2.oncourt_name, p2_name) as p2_name
  from {{ source('raw_layer', 'pinnacle_odds') }} as p
    left join fix_player_names as f1 on p.p1_name = f1.pinnacle_name
    left join fix_player_names as f2 on p.p2_name = f2.pinnacle_name
  where p.resulting_unit = 'Sets'
    and p.event_type = 'prematch'
    and p.p1_pinnacle_odds is not null
    and p.tournament_round not like '%Doubles%'
),

pinnacle_union as (
  select p1_name as player_name from pinnacle_players
  union all
  select p2_name as player_name from pinnacle_players
),

pinnacle_oncourt_join as (
  select
      pu.player_name as pinnacle_player_name,
      ap.player_name as oncourt_atp_player_name,
      wp.player_name as oncourt_wta_player_name,
  from pinnacle_union as pu
    left join {{ ref('atp_players') }} as ap
      on pu.player_name = ap.player_name
    left join {{ ref('wta_players') }} as wp
      on pu.player_name = wp.player_name
)

select * from pinnacle_oncourt_join
where oncourt_atp_player_name is null and oncourt_wta_player_name is null
