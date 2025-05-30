{{ config(
    materialized = 'table',
    schema = 'model'
)}}

with 

wta_stats as (
  select
    player_1_id,
    player_1_name,
    match_total_points,
    p1_winners,
    p1_unforced_errors,
    p1_aces,
    p1_double_faults,
    p1_net_points_played,
    p1_service_points_played,
    p1_service_points_won,
    p1_return_points_played,
    p1_return_points_won,
    player_2_id,
    player_2_name,
    p2_winners,
    p2_unforced_errors,
    p2_aces,
    p2_double_faults,
    p2_net_points_played,
    p2_service_points_played,
    p2_service_points_won,
    p2_return_points_played,
    p2_return_points_won
  from {{ ref('wta_stats') }}
),

stats_union as (
  select
    player_1_id as player_id,
    player_1_name as player_name,
    match_total_points,
    p1_winners - p1_aces as rally_winners,
    p1_unforced_errors - p1_double_faults as rally_unforced_errors,
    p1_service_points_played as service_points_played,
    p1_service_points_won as service_points_won,
    p1_return_points_played as return_points_played,
    p1_return_points_won as return_points_won,
    p1_net_points_played as net_points_played
  from wta_stats
  union all
  select
    player_2_id as player_id,
    player_2_name as player_name,
    match_total_points,
    p2_winners - p2_aces as rally_winners,
    p2_unforced_errors - p2_double_faults as rally_unforced_errors,
    p2_service_points_played as service_points_played,
    p2_service_points_won as service_points_won,
    p2_return_points_played as return_points_played,
    p2_return_points_won as return_points_won,
    p2_net_points_played as net_points_played
  from wta_stats
),

rally_aggression as (
  select
    player_id,
    player_name,
    count(1) as player_data_points,
    (sum(rally_winners) + sum(rally_unforced_errors)) / sum(match_total_points) as rally_aggression_score
  from stats_union
  where rally_winners > 0 and rally_unforced_errors > 0 and match_total_points > (rally_winners + rally_unforced_errors)
  group by player_id, player_name
),

serve_dependence as (
  select
    player_id,
    player_name,
    count(1) as player_data_points,
    ((sum(service_points_won) / sum(service_points_played)) / (sum(return_points_won) / sum(return_points_played))
    ) as serve_dependency_score
  from stats_union
  where service_points_played is not null and service_points_won is not null
    and return_points_played is not null and return_points_won is not null
  group by player_id, player_name
),

net_points as (
  select
    player_id,
    player_name,
    count(1) as player_data_points,
    sum(net_points_played) / sum(match_total_points) as net_points_ratio
  from stats_union
  where net_points_played is not null and match_total_points > net_points_played
  group by player_id, player_name
),

final as (
  select
    s.player_id,
    s.player_name,
    r.rally_aggression_score,
    s.serve_dependency_score,
    n.net_points_ratio
  from (select player_id, player_name, serve_dependency_score from serve_dependence where player_data_points >= 10) as s
  left join (select player_id, rally_aggression_score from rally_aggression where player_data_points >= 10) as r
    on s.player_id = r.player_id
  left join (select player_id, net_points_ratio from net_points where player_data_points >= 10) as n
    on s.player_id = n.player_id
)

select * from final