{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}


with 

historical_weather as (
  select * from {{ ref('historical_weather') }}
),

final as (
  select distinct
    geo_id,
    elevation
  from historical_weather
)

select * from final