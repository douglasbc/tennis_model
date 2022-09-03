{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with players_atp as (
  select * from {{ source('raw_layer', 'players_atp') }}
  where country_code is not null
),

categories_atp as (
  select * from {{ source('raw_layer', 'categories_atp')}}
),

country_codes as (
  select * from {{ source('raw_layer', 'country_codes') }}
),

final as (
    select
      p.player_id,
      p.player_name,
      p.birth_date,
      cc.country,
      c.is_left_handed
    from players_atp as p
      left join categories_atp as c
        on p.player_id = c.player_id
      left join country_codes as cc
        on p.country_code = cc.country_code
)

select * from final


