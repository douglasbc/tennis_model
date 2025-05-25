{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with 

atp_last_52_serve as (
  select * from {{ source('raw_layer', 'atp_last_52_serve') }}
),

atp_last_52_return as (
  select * from {{ source('raw_layer', 'atp_last_52_return') }}
),

atp_last_52_rally as (
  select * from {{ source('raw_layer', 'atp_last_52_rally') }}
),

atp_last_52_tactics as (
  select * from {{ source('raw_layer', 'atp_last_52_tactics') }}
),

fix_player_names as (
  select * from {{ source('raw_layer', 'fix_player_names') }}
),

final as (
    select
      coalesce(fix.oncourt_name, s.player_name) as player_name,
      s.matches_count,
      cast(left(s.unreturned_pct, length(s.unreturned_pct)-1) as float64) as unreturned_pct,
      cast(left(s.sp_won_serve_or_second_shot_pct, length(s.sp_won_serve_or_second_shot_pct)-1) as float64) as sp_won_serve_or_second_shot_pct,
      cast(left(s.sp_won_when_returned_pct, length(s.sp_won_when_returned_pct)-1) as float64) as sp_won_when_returned_pct,
      cast(left(s.serve_impact, length(s.serve_impact)-1) as float64) as serve_impact,
      cast(left(s.first_serve_unreturned_pct, length(s.first_serve_unreturned_pct)-1) as float64) as first_serve_unreturned_pct,
      cast(left(s.first_sp_won_serve_or_second_shot_pct, length(s.first_sp_won_serve_or_second_shot_pct)-1) as float64) as first_sp_won_serve_or_second_shot_pct,
      cast(left(s.first_sp_won_when_returned_pct, length(s.first_sp_won_when_returned_pct)-1) as float64) as first_sp_won_when_returned_pct,
      cast(left(s.first_serve_impact, length(s.first_serve_impact)-1) as float64) as first_serve_impact,
      cast(left(s.wide_deuce_court_first_serve_pct, length(s.wide_deuce_court_first_serve_pct)-1) as float64) as wide_deuce_court_first_serve_pct,
      cast(left(s.wide_ad_court_first_serve_pct, length(s.wide_ad_court_first_serve_pct)-1) as float64) as wide_ad_court_first_serve_pct,
      cast(left(s.wide_ad_court_break_point_first_serve_pct, length(s.wide_ad_court_break_point_first_serve_pct)-1) as float64) as wide_ad_court_break_point_first_serve_pct,
      cast(left(s.second_serve_unreturned_pct, length(s.second_serve_unreturned_pct)-1) as float64) as second_serve_unreturned_pct,
      cast(left(s.second_sp_won_serve_or_second_shot_pct, length(s.second_sp_won_serve_or_second_shot_pct)-1) as float64) as second_sp_won_serve_or_second_shot_pct,
      cast(left(s.second_sp_won_when_returned_pct, length(s.second_sp_won_when_returned_pct)-1) as float64) as second_sp_won_when_returned_pct,
      cast(left(s.wide_deuce_court_second_serve_pct, length(s.wide_deuce_court_second_serve_pct)-1) as float64) as wide_deuce_court_second_serve_pct,
      cast(left(s.wide_ad_court_second_serve_pct, length(s.wide_ad_court_second_serve_pct)-1) as float64) as wide_ad_court_second_serve_pct,
      cast(left(s.wide_ad_court_break_point_second_serve_pct, length(s.wide_ad_court_break_point_second_serve_pct)-1) as float64) as wide_ad_court_break_point_second_serve_pct,
      s.second_serve_agression_score,
      cast(left(ret.returned_pct, length(ret.returned_pct)-1) as float64) as returned_pct,
      cast(left(ret.points_won_returned_pct, length(ret.points_won_returned_pct)-1) as float64) as points_won_returned_pct,
      cast(left(ret.winners_pct, length(ret.winners_pct)-1) as float64) as winners_pct,
      cast(left(ret.fh_winners_pct, length(ret.fh_winners_pct)-1) as float64) as fh_winners_pct,
      ret.return_depth_index,
      cast(left(ret.slice_returns_pct, length(ret.slice_returns_pct)-1) as float64) as slice_returns_pct,
      cast(left(ret.first_serve_returned_pct, length(ret.first_serve_returned_pct)-1) as float64) as first_serve_returned_pct,
      cast(left(ret.first_serve_points_won_returned_pct, length(ret.first_serve_points_won_returned_pct)-1) as float64) as first_serve_points_won_returned_pct,
      cast(left(ret.first_serve_winners_pct, length(ret.first_serve_winners_pct)-1) as float64) as first_serve_winners_pct,
      ret.first_serve_return_depth_index,
      cast(left(ret.first_serve_slice_returns_pct, length(ret.first_serve_slice_returns_pct)-1) as float64) as first_serve_slice_returns_pct,
      cast(left(ret.second_serve_returned_pct, length(ret.second_serve_returned_pct)-1) as float64) as second_serve_returned_pct,
      cast(left(ret.second_serve_points_won_returned_pct, length(ret.second_serve_points_won_returned_pct)-1) as float64) as second_serve_points_won_returned_pct,
      cast(left(ret.second_serve_winners_pct, length(ret.second_serve_winners_pct)-1) as float64) as second_serve_winners_pct,
      ret.second_serve_return_depth_index,
      cast(left(ret.second_serve_slice_returns_pct, length(ret.second_serve_slice_returns_pct)-1) as float64) as second_serve_slice_returns_pct,
      rally.avg_rally_lenght,
      rally.avg_rally_lenght_serve,
      rally.avg_rally_lenght_return,
      cast(left(rally.points_btw_1_3_shots_win_pct, length(rally.points_btw_1_3_shots_win_pct)-1) as float64) as points_btw_1_3_shots_win_pct,
      cast(left(rally.points_btw_4_6_shots_win_pct, length(rally.points_btw_4_6_shots_win_pct)-1) as float64) as points_btw_4_6_shots_win_pct,
      cast(left(rally.points_btw_7_9_shots_win_pct, length(rally.points_btw_7_9_shots_win_pct)-1) as float64) as points_btw_7_9_shots_win_pct,
      cast(left(rally.points_10_plus_shots_win_pct, length(rally.points_10_plus_shots_win_pct)-1) as float64) as points_10_plus_shots_win_pct,
      cast(left(rally.forehands_per_groundstroke, length(rally.forehands_per_groundstroke)-1) as float64) as forehands_per_groundstroke,
      cast(left(rally.sliced_per_backhand_groundstroke, length(rally.sliced_per_backhand_groundstroke)-1) as float64) as sliced_per_backhand_groundstroke,
      rally.forehand_potency_per_100_forehands,
      rally.forehand_potency_per_match,
      rally.backhand_potency_per_match,
      rally.backhand_potency_per_100_backhands,
      cast(left(t.serve_and_volley_pct, length(t.serve_and_volley_pct)-1) as float64) as serve_and_volley_pct,
      cast(left(t.points_won_per_serve_and_volley, length(t.points_won_per_serve_and_volley)-1) as float64) as points_won_per_serve_and_volley,
      cast(left(t.net_points_pct, length(t.net_points_pct)-1) as float64) as net_points_pct,
      cast(left(t.points_won_per_net_point, length(t.points_won_per_net_point)-1) as float64) as points_won_per_net_point,
      cast(left(t.winners_per_forehand, length(t.winners_per_forehand)-1) as float64) as winners_per_forehand,
      cast(left(t.winners_per_forehand_down_the_line, length(t.winners_per_forehand_down_the_line)-1) as float64) as winners_per_forehand_down_the_line,
      cast(left(t.winners_per_forehand_inside_out, length(t.winners_per_forehand_inside_out)-1) as float64) as winners_per_forehand_inside_out,
      cast(left(t.winners_per_backhand, length(t.winners_per_backhand)-1) as float64) as winners_per_backhand,
      cast(left(t.winners_per_backhand_down_the_line, length(t.winners_per_backhand_down_the_line)-1) as float64) as winners_per_backhand_down_the_line,
      cast(left(t.dropshot_pct, length(t.dropshot_pct)-1) as float64) as dropshot_pct,
      cast(left(t.points_won_per_dropshot, length(t.points_won_per_dropshot)-1) as float64) as points_won_per_dropshot,
      t.rally_agression_score,
      t.return_agression_score
    from atp_last_52_serve as s
      left join atp_last_52_return as ret
        on s.player_name = ret.player_name
      left join atp_last_52_rally as rally
        on s.player_name = rally.player_name
      left join atp_last_52_tactics as t
        on s.player_name = t.player_name
      left join fix_player_names as fix
        on s.player_name = fix.match_charting_project_name
    where s.player_name not in (
      'John Mcenroe',
      'Sergi Bruguera'
    )
)

select * from final
