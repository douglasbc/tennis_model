{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with tournaments as (
  select * from {{ source('raw_layer', 'tournaments_wta') }}
  where
    extract(year from tournament_date) >= 2015
    and tournament_level <> 6

),

courts as (
  select
    surface_id,
    surface
  from {{ source('raw_layer', 'courts')}}
),

country_codes as (
  select * from {{ source('raw_layer', 'country_codes') }}
),

final as (
  select
    t.tournament_id,
    t.tournament_date,
    t.tournament_name,
    cc.country,
    c.surface,
    case
      when (t.tournament_level = 5 and extract(year from tournament_date) >= 2021)
        then 'Billie Jean King Cup'
      when t.tournament_level = 5
        then 'Fed Cup'
      when t.tournament_level = 4
        then 'Grand Slam'
      when t.tournament_level = 3 and extract(year from tournament_date) >= 2021
        then 'WTA 1000'
      when t.tournament_level = 3
        then 'Premier'
      when t.tournament_level = 2 and extract(year from tournament_date) <= 2020
        then 'WTA International'
      when t.tournament_id = 13243
        then 'WTA International'
      when t.tournament_prize in ('$115K', '$125K', '$125K+H', '$140K', '$150K', '$162K')
        then 'WTA 125'
      when t.tournament_level in (0, 1)
        then SPLIT(t.tournament_prize, '+') [OFFSET(0)]
      else t.tournament_tier
    end as tournament_tier,
    t.tournament_prize
  from tournaments as t
    left join courts as c
      on t.surface_id = c.surface_id
    left join country_codes as cc
      on t.country = cc.country_code
)

select * from final


