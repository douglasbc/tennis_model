{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}


with 

historical_weather as (
  select * from {{ source('raw_layer', 'historical_weather') }}
),

convert_to_local_datetime as (
  select distinct
    DATETIME(timestamp, SAFE_CONVERT_BYTES_TO_STRING(timezone)) as local_datetime,
    right(left(latitude,length(latitude)-2), length(latitude)-3) as latitude,
    longitude,
    temperature,
    relative_humidity,
    apparent_temperature,
    wind_speed,
    elevation    
  from historical_weather
),

final as (
  select
    concat(date(local_datetime), latitude, longitude) as weather_id,
    to_hex(md5(concat(latitude, longitude))) as geo_id,
    date(local_datetime) as local_date,
    latitude,
    longitude,
    cast(elevation as int64) as elevation,
    round(avg(apparent_temperature), 2) as avg_apparent_temperature,
    round(avg(relative_humidity), 2) as avg_relative_humidity,
    round(avg(wind_speed), 2) as avg_wind_speed,
  from convert_to_local_datetime
  where
    extract(hour from local_datetime) between 11 and 22
  group by 1,2,3,4,5,6
)

select * from final