{{ config(
    materialized = 'table',
    schema = 'model_layer',
)}}

with today_atp as (
  select * from {{ source('raw_layer', 'today_atp') }}
),

atp_players as (
  select * from {{ ref('atp_players') }}
),

atp_tournaments as (
  select * from {{ ref('atp_tournaments') }}
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
      t.surface,
      case
      when t.tournament_tier in ('ATP 250', 'ATP 500', 'ATP Cup', 'ATP Finals', 'Masters 1000', 'Grand Slam')
        then 'Main Tour'
      else t.tournament_tier
    end as tournament_level
    from today_atp as td
      inner join atp_tournaments as t on td.tournament_id = t.tournament_id
      inner join atp_players as p1 on td.player_1_id = p1.player_id
      inner join atp_players as p2 on td.player_2_id = p2.player_id
      left join rounds as r on td.round_id = r.round_id
    where
      td.player_1_id not in (3699, 3700)
      and td.player_2_id not in (3699, 3700)
      and td.result is null
      -- and t.tournament_tier in ('ATP 250', 'ATP 500', 'ATP Cup', 'ATP Finals', 'Masters 1000', 'Grand Slam')
)

select * from final
