{{ config(
    materialized = 'table',
    schema = 'analytics_layer',
)}}

with 

matches_atp as (
  select
    *
  from {{ ref('matches_atp') }}
  where extract(year from match_date) >= 2022
),

atp_tournaments as (
  select
    *
  from {{ ref('atp_tournaments') }}
),

atp_players_clusters as (
  select * from {{ source('raw_layer', 'atp_players_clusters') }}
),

matches_clusters as (
    select distinct
      m.p1_name,
      m.p2_name,
      m.tournament_country,
      m.surface,
      t.elevation_cluster,
      m.avg_apparent_temperature,
      m.p1_country,
      m.p2_country,
      c1.cluster as p1_cluster,
      c2.cluster as p2_cluster,
      m.p1_win_match_odds,
      m.p2_win_match_odds,
      m.p1_handicap_odds,
      m.p2_handicap_odds,
      case
        when m.p1_total_games - m.p2_total_games + m.p1_handicap_line > 0
          then 'won'
        when m.p1_total_games - m.p2_total_games + m.p1_handicap_line < 0
          then 'lost'
        when m.p1_total_games - m.p2_total_games + m.p1_handicap_line = 0
          then 'refunded'
        else null
      end as p1_handicap_result,
      case
        when m.p2_total_games - m.p1_total_games + m.p2_handicap_line > 0
          then 'won'
        when m.p2_total_games - m.p1_total_games + m.p2_handicap_line < 0
          then 'lost'
        when m.p2_total_games - m.p1_total_games + m.p2_handicap_line = 0
          then 'refunded'
        else null
      end as p2_handicap_result
    from matches_atp as m
      left join atp_tournaments as t on m.tournament_id = t.tournament_id
      left join atp_players_clusters as c1 on m.p1_name = c1.player_name
      left join atp_players_clusters as c2 on m.p2_name = c2.player_name
),

player_cluster_matches as (
  select
    p1_name as player_name,
    p2_cluster as opponent_cluster,
    count(1) as matches,
    sum(p1_win_match_odds) as units
  from matches_clusters
  -- where p2_cluster is not null
  group by 1,2

  union all

  select
    p2_name as player_name,
    p1_cluster as opponent_cluster,
    count(1) as matches,
    count(1) * -1 as units
  from matches_clusters
  -- where p1_cluster is not null
  group by 1,2
),

player_handicap_p1_won as (
   select
    p1_name as player_name,
    p2_cluster as opponent_cluster,
    count(1) as matches,
    sum(p1_handicap_odds) as units
  from matches_clusters
  where p1_handicap_result = 'won'
  group by 1,2
),

player_handicap_p2_won as (
  select
    p2_name as player_name,
    p1_cluster as opponent_cluster,
    count(1) as matches,
    sum(p2_handicap_odds) as units
  from matches_clusters
  where p2_handicap_result = 'won'
  group by 1,2
),

player_handicap_p1_lost as (
  select
    p1_name as player_name,
    p2_cluster as opponent_cluster,
    count(1) as matches,
  from matches_clusters
  where p1_handicap_result = 'lost'
  group by 1,2
),

player_handicap_p2_lost as (
  select
    p2_name as player_name,
    p1_cluster as opponent_cluster,
    count(1) as matches,
  from matches_clusters
  where p2_handicap_result = 'lost'
  group by 1,2
),

players as (
  select distinct
    player_name,
    opponent_cluster
  from (
    select
      player_name,
      opponent_cluster
    from player_handicap_p1_won
    union all
      select
      player_name,
      opponent_cluster
    from player_handicap_p2_won
      union all  
      select
      player_name,
      opponent_cluster
    from player_handicap_p1_lost
      union all  
      select
      player_name,
      opponent_cluster
    from player_handicap_p2_lost
  )
),

player_handicap_roi as (
  select
    p.player_name,
    p.opponent_cluster,
    p1_won.matches + p2_won.matches + p1_lost.matches + p2_lost.matches as handicap_matches,
    p1_won.units + p2_won.units - p1_lost.matches - p2_lost.matches as handicap_net_units,
    round(SAFE_DIVIDE(
      p1_won.matches + p2_won.matches + p1_lost.matches + p2_lost.matches,
      p1_won.units + p2_won.units - p1_lost.matches - p2_lost.matches
      ), 2) * 100 as handicap_net_roi
  from players as p
    left join player_handicap_p1_won as p1_won
      on p.player_name = p1_won.player_name and p.opponent_cluster = p1_won.opponent_cluster
    left join player_handicap_p2_won as p2_won
      on p.player_name = p2_won.player_name and p.opponent_cluster = p2_won.opponent_cluster
    left join player_handicap_p1_lost as p1_lost
      on p.player_name = p1_lost.player_name and p.opponent_cluster = p1_lost.opponent_cluster
    left join player_handicap_p2_lost as p2_lost
      on p.player_name = p2_lost.player_name and p.opponent_cluster = p2_lost.opponent_cluster
),

player_match_roi as (
  select
    player_name,
    opponent_cluster,
    sum(matches) as matches,
    round(sum(units), 2) as net_units,
    round(SAFE_DIVIDE(sum(units),sum(matches)), 2) * 100 as net_roi
  from player_cluster_matches
  group by 1,2
),

final as (
  select
    m.player_name,
    m.opponent_cluster,
    m.matches,
    m.net_units,
    m.net_roi,
    h.handicap_matches,
    h.handicap_net_units,
    h.handicap_net_roi
  from player_match_roi as m
    left join player_handicap_roi as h
      on m.player_name = h.player_name and m.opponent_cluster = h.opponent_cluster
)

select * from final