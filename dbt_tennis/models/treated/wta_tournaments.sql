{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

tournaments_wta as (
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

elevation_clusters as (
  select * from {{ ref('elevation_clusters') }}
),

tournaments_geo as (
  select
    tournament_id,
    to_hex(md5(concat(
      if(tournament_name = 'M15 Quito', -0.13, tournament_latitude),
      if(tournament_name = 'M15 Quito', -78.300003, tournament_longitude)
      ))) as geo_id,
    surface_id,
    tournament_date,
    tournament_name,
    tournament_level,
    tournament_tier,
    if(tournament_name = 'M15 Quito', -0.13, tournament_latitude) as tournament_latitude,
    if(tournament_name = 'M15 Quito', -78.300003, tournament_longitude) as tournament_longitude,
    tournament_prize,
    country
  from tournaments_wta
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
      when t.tournament_tier like '%wta World Tour 250%'
        then 'wta 250'
      when t.tournament_tier like '%wta World Tour 500%'
        then 'wta 500'
      when t.tournament_tier like '%Masters 1000%'
        then 'Masters 1000'
      when t.tournament_id in (8912, 8052)
        then 'wta World Team Cup'
      when t.tournament_tier like '%wta Cup%'
        then 'wta Cup'
      when t.tournament_tier like '%wta World Tour Finals%'
        then 'wta Finals'
      when t.tournament_tier like '%Finals%'
        then 'Next Gen wta Finals'
      else t.tournament_tier
    end as tournament_tier,
    t.tournament_prize,
    t.tournament_latitude,
    t.tournament_longitude,
    ec.elevation as tournament_elevation,
    ec.cluster_label as elevation_cluster
  from tournaments_geo as t
    left join courts as c
      on t.surface_id = c.surface_id
    left join country_codes as cc
      on t.country = cc.country_code
    left join elevation_clusters as ec
      on t.geo_id = ec.geo_id
)

select * from final
