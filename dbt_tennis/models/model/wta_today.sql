{{ config(
    materialized = 'table',
    schema = 'model_layer',
)}}

with today_wta as (
  select * from {{ source('raw_layer', 'today_wta') }}
),

wta_players as (
  select * from {{ ref('wta_players') }}
),

wta_tournaments as (
  select * from {{ ref('wta_tournaments') }}
),

rounds as (
  select * from {{ source('raw_layer', 'rounds') }}
),

final as (
    select
      td.match_date,
      p1.player_name as p1_name,
      p2.player_name as p2_name,
      r.round,
      t.tournament_name,
      case
        when t.surface = 'Carpet' then 'Indoor Hard'
        else t.surface
      end as surface,
      t.tournament_tier
    from today_wta as td
      inner join wta_tournaments as t on td.tournament_id = t.tournament_id
      inner join wta_players as p1 on td.player_1_id = p1.player_id
      inner join wta_players as p2 on td.player_2_id = p2.player_id
      left join rounds as r on td.round_id = r.round_id
    where
      td.player_1_id not in (3699, 3700)
      and td.player_2_id not in (3699, 3700)
      and td.result is null
)

select * from final
