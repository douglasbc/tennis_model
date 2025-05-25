{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}


with 

tournament_elevation as (
  select * from {{ ref('tournament_elevation') }}
),


elevation_clusters as (
  select * from {{ source('raw_layer', 'elevation_clusters') }}
),

centroids as (
  select
    distinct cluster,
    centroid,    
  from elevation_clusters
),

ranked_data as (
  select 
    cluster,
    rank() over (order by centroid) AS rank
  from centroids
),

label as (
  select 
    cluster,
    case 
      when rank = 1 then 'no altitude'
      when rank = 2 then 'low altitude'
      when rank = 3 then 'medium altitude'
      when rank = 4 then 'high altitude'
    end as cluster_label
  from ranked_data
),

final as (
  select
    te.geo_id,
    ec.elevation,
    ec.cluster,
    ec.centroid,
    l.cluster_label
  from elevation_clusters as ec
  left join tournament_elevation as te
    on ec.elevation = te.elevation
  left join label as l
    on ec.cluster = l.cluster
)

select * from final