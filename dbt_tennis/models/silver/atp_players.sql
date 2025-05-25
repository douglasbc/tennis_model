{{ config(
    materialized = 'table',
    schema = 'silver'
)}}

with 

players_atp as (
  select * from {{ source('raw_layer', 'players_atp') }}
  where country_code is not null
),

categories_atp as (
  select * from {{ source('raw_layer', 'categories_atp') }}
),

country_codes as (
  select * from {{ source('raw_layer', 'country_codes') }}
),

atp_players_active_status as (
  select * from {{ ref('atp_players_active_status') }}
),

final as (
    select
      p.player_id,
      p.player_name,
      CONCAT(
        REGEXP_EXTRACT(p.player_name, r'^\S+\s(.+)$'),  -- Everything after first name
        ' ',
        SUBSTR(REGEXP_EXTRACT(p.player_name, r'^(\S+)'), 1, 1),  -- First initial
        '.'
      ) AS player_standardized_name,
      p.birth_date,
      cc.country,
      c.is_left_handed,
      pas.last_match_date,
      pas.active_last_2_years,
      pas.active_since_2015
    from players_atp as p
      left join categories_atp as c
        on p.player_id = c.player_id
      left join country_codes as cc
        on p.country_code = cc.country_code
      left join atp_players_active_status as pas
        on p.player_id = pas.player_id
)

select * from final


