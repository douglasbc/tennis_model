{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

tournaments_atp as (
  select * from {{ source('raw_layer', 'tournaments_atp') }}
  where
    extract(year from tournament_date) >= 2024
    and tournament_level <> 6
),

tournaments_wta as (
  select * from {{ source('raw_layer', 'tournaments_wta') }}
  where
    extract(year from tournament_date) >= 2024
    and tournament_level <> 6
),

atp as (
  select
    if(
      tournament_level = 4,
      date_sub(tournament_date, INTERVAL 8 DAY),
      date_sub(tournament_date, INTERVAL 3 DAY)
    ) as start_date,
    if(
      tournament_level = 4
        or (tournament_level = 3 and tournament_name like '%Indian Wells%')
        or (tournament_level = 3 and tournament_name like '%Miami%'),
      date_add(tournament_date, INTERVAL 15 DAY),
      date_add(tournament_date, INTERVAL 8 DAY)
    ) as end_date,
    tournament_latitude,
    tournament_longitude
  from tournaments_atp
  where
    tournament_date = '2024-07-22'
    and tournament_latitude is not null 
    and tournament_longitude is not null
),

wta as (
  select
    if(
      tournament_level = 4,
      date_sub(tournament_date, INTERVAL 8 DAY),
      date_sub(tournament_date, INTERVAL 3 DAY)
    ) as start_date,
    if(
      tournament_level = 4
        or (tournament_level = 3 and tournament_name like '%Indian Wells%')
        or (tournament_level = 3 and tournament_name like '%Miami%'),
      date_add(tournament_date, INTERVAL 15 DAY),
      date_add(tournament_date, INTERVAL 8 DAY)
    ) as end_date,
    tournament_latitude,
    tournament_longitude
  from tournaments_wta
  where
    tournament_date = '2024-07-22'
    and tournament_latitude is not null 
    and tournament_longitude is not null
),

tournaments_union as (
  select * from atp
  union all
  select * from wta  
)

select distinct
  to_hex(md5(concat(start_date, end_date, tournament_latitude, tournament_longitude))) as unique_key,
  *
from tournaments_union