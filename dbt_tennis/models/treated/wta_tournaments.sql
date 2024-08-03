{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

tournaments_atp as (
  select * from {{ source('raw_layer', 'tournaments_atp') }}
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

tournament_elevation as (
  select * from {{ ref('tournament_elevation') }}
),

tournaments_geo as (
  select
    tournament_id,
    to_hex(md5(concat(tournament_latitude, tournament_longitude))) as geo_id,
    surface_id,
    tournament_date,
    tournament_name,
    tournament_level,
    tournament_tier,
    tournament_latitude,
    tournament_longitude,
    tournament_prize,
    country
  from tournaments_atp
),

final as (
  select
    t.tournament_id,
    t.geo_id,
    t.tournament_date,
    t.tournament_name,
    cc.country,
    c.surface,
    case
      when t.tournament_name like '%Laver Cup%'
        then 'Laver Cup'
      when t.tournament_level = 5
        then 'Davis Cup'
      when t.tournament_level = 4
        then 'Grand Slam'
      when t.tournament_level = 1
        then 'Challenger'
      when t.tournament_level = 0
        then 'Future'
      when t.tournament_tier like '%ATP World Tour 250%'
        then 'ATP 250'
      when t.tournament_tier like '%ATP World Tour 500%'
        then 'ATP 500'
      when t.tournament_tier like '%Masters 1000%'
        then 'Masters 1000'
      when t.tournament_id in (8912, 8052)
        then 'ATP World Team Cup'
      when t.tournament_tier like '%ATP Cup%'
        then 'ATP Cup'
      when t.tournament_tier like '%ATP World Tour Finals%'
        then 'ATP Finals'
      when t.tournament_tier like '%Finals%'
        then 'Next Gen ATP Finals'
      else t.tournament_tier
    end as tournament_tier,
    t.tournament_prize,
    t.tournament_latitude,
    t.tournament_longitude,
    e.elevation as tournament_elevation
  from tournaments_geo as t
    left join courts as c
      on t.surface_id = c.surface_id
    left join country_codes as cc
      on t.country = cc.country_code
    left join tournament_elevation as e
      on t.geo_id = e.geo_id
)

select * from final
