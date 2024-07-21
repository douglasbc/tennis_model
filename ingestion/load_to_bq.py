import os
from typing import List

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from bq_client import bigquery_client
from scrape_mcp_to_df import scrape_mcp


client = bigquery_client()
df_dict = scrape_mcp()


class TennisTable():

    BACKUP_PATH = os.path.join('data', 'backup')
    CSV_PATH = os.path.join('data', 'csv')
    DATASET = 'raw_layer'
    PROJECT_ID = 'tennis-358702'

    table_names = [
        'categories', 'courts', 'games', 'odds',
        'players', 'ratings', 'rounds', 'seed',
        'stat', 'today', 'tours',
        'atp_last_52_serve',
        'atp_last_52_return',
        'atp_last_52_rally',
        'atp_last_52_tactics',
        'atp_career_serve',
        'atp_career_return',
        'atp_career_rally',
        'atp_career_tactics',
        'wta_last_52_serve',
        'wta_last_52_return',
        'wta_last_52_rally',
        'wta_last_52_tactics',
        'wta_career_serve',
        'wta_career_return',
        'wta_career_rally',
        'wta_career_tactics'
        ]

    def __init__(self, name: str, schema: List[bigquery.SchemaField],
            rename: List[str], tour=None, mcp=None, incremental=False
            ):
        if mcp:
            self.df = df_dict[name]
        elif tour:
            self.df = pd.read_csv(os.path.join(TennisTable.CSV_PATH, f'{name}_{tour}.csv'))
        else:
            self.df = pd.read_csv(os.path.join(TennisTable.CSV_PATH, f'{name}.csv'))
        
        self.name = name
        self.schema = schema
        self.rename = rename
        self.tour = tour
        self.incremental = incremental


    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if name not in TennisTable.table_names:
            raise ValueError('''Invalid name. Valid names are:
                            categories, courts, games, odds,
                            players, ratings, rounds, seed,
                            stat, today, tours.'''
                            )
        self._name = name
    

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema


    @property
    def rename(self):
        return self._rename

    @rename.setter
    def rename(self, rename):
        self._rename = rename


    @property
    def tour(self):
        return self._tour

    @tour.setter
    def tour(self, tour):
        if tour and tour not in ('atp', 'wta'):
            raise ValueError('Invalid tour. Valid tours are atp and wta.')
        self._tour = tour

    
    @property
    def incremental(self):
        return self._incremental

    @incremental.setter
    def incremental(self, incremental):
        if incremental not in (True, False):
            raise ValueError('Invalid incremental flag. Valid flags are the booleans True and False.')
        self._incremental = incremental


    def keep_columns(self, columns: List[str]):
        self.df = self.df[columns]


    def drop_columns(self, columns: List[str]):
        self.df = self.df.drop(columns=columns)


    def rename_columns(self):
        assert len(self.df.columns) == len(self.rename)
        self.df.columns = self.rename

    def cast_column_to_date(self, column):
        self.df[column] = pd.to_datetime(self.df[column], format='%m/%d/%y %H:%M:%S').dt.date


    def incremental_control():

        table = f'{self.name}_{self.tour}'
        csv_path = os.path.join(TennisTable.BACKUP_PATH, f'{table}.csv')

        with open(csv_path) as f:
            row_number = sum(1 for row in f)

        return row_number


    def load_to_bq(self, bq_table_name, write_disposition='WRITE_TRUNCATE',
                        partition_field=None, require_partition_filter=False):

        if self.tour:
            table_name = f'{bq_table_name}_{self.tour}'
        else:
            table_name = bq_table_name

        table_id = f"{TennisTable.PROJECT_ID}.{TennisTable.DATASET}.{table_name}"

        if partition_field is not None:
            time_partitioning = bigquery.TimePartitioning(
                field=partition_field,
                type_=bigquery.TimePartitioningType.MONTH,
                require_partition_filter=require_partition_filter
            )
        else:
            time_partitioning = None

        assert len(self.df.columns) == len(self.schema)

        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            time_partitioning=time_partitioning,
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.CSV
        )

        if self.incremental:
            i = incremental_control()
            self.df = self.df[i-1:]

        job = client.load_table_from_dataframe(self.df, table_id, job_config=job_config)
        job.result()

        print(f"Loaded table {table_id}")


def load_categories():

    keep = ['ID_P', 'CAT1']
    rename = ['player_id', 'is_left_handed']
    schema = [
    bigquery.SchemaField('player_id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('is_left_handed', 'BOOLEAN', mode='REQUIRED')
]
    categories_atp = TennisTable('categories', schema, rename, 'atp')
    categories_wta = TennisTable('categories', schema, rename, 'wta')

    for table in [categories_atp, categories_wta]:
        table.keep_columns(keep)
        table.rename_columns()
        table.load_to_bq('categories')


def load_courts():

    rename = ['surface_id', 'surface']
    schema = [
        bigquery.SchemaField('surface_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('surface', 'STRING')
    ]
    courts = TennisTable('courts', schema, rename)

    courts.rename_columns()
    courts.load_to_bq('courts')


def load_matches(incremental=False):

    rename = ['player_1_id', 'player_2_id', 'tournament_id', 'round_id', 'result', 'match_date']
    schema = [
        bigquery.SchemaField('player_1_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_2_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('round_id', 'INT64'),
        bigquery.SchemaField('result', 'STRING'),
        bigquery.SchemaField('match_date', 'DATE')
        ]
    matches_atp = TennisTable('games', schema, rename, 'atp', incremental)
    # filter matches since 2015
    matches_atp.df = matches_atp.df[505609:]

    matches_wta = TennisTable('games', schema, rename, 'wta', incremental)
    # filter matches since 2015
    matches_wta.df = matches_wta.df[319104:]



    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [matches_atp, matches_wta]:
        table.rename_columns()
        table.cast_column_to_date('match_date')
        table.load_to_bq('matches', write_disposition, partition_field='match_date')


def load_odds(incremental=False):

    rename = ['bookie_id', 'player_1_id', 'player_2_id', 'tournament_id',
                    'round_id', 'p1_win_match_odds', 'p2_win_match_odds',
                    'total_line', 'under_odds', 'over_odds',
                    'p1_handicap_line', 'p2_handicap_line',
                    'p1_handicap_odds', 'p2_handicap_odds',
                    'p1_2_0_sets_odds', 'p1_2_1_sets_odds',
                    'p2_2_1_sets_odds', 'p2_2_0_sets_odds',
                    'p1_3_0_sets_odds', 'p1_3_1_sets_odds',
                    'p1_3_2_sets_odds', 'p2_3_2_sets_odds',
                    'p2_3_1_sets_odds', 'p2_3_0_sets_odds',
                    ] 
    schema = [
        bigquery.SchemaField('bookie_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_1_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_2_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('round_id', 'INT64'),
        bigquery.SchemaField('p1_win_match_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_win_match_odds', 'FLOAT64'),
        bigquery.SchemaField('total_line', 'FLOAT64'),
        bigquery.SchemaField('under_odds', 'FLOAT64'),
        bigquery.SchemaField('over_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_handicap_line', 'FLOAT64'),
        bigquery.SchemaField('p2_handicap_line', 'FLOAT64'),
        bigquery.SchemaField('p1_handicap_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_handicap_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_2_0_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_2_1_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_2_1_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_2_0_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_3_0_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_3_1_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p1_3_2_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_3_2_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_3_1_sets_odds', 'FLOAT64'),
        bigquery.SchemaField('p2_3_0_sets_odds', 'FLOAT64')      
]
    odds_atp = TennisTable('odds', schema, rename, 'atp', incremental)
    odds_atp.df = odds_atp.df[116833:]

    odds_wta = TennisTable('odds', schema, rename, 'wta', incremental)
    odds_wta.df = odds_wta.df[81560:]

    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [odds_atp, odds_wta]:
        table.rename_columns()
        table.load_to_bq('odds', write_disposition)


def load_players():

    keep = ['ID_P', 'NAME_P', 'DATE_P', 'COUNTRY_P']
    rename = ['player_id', 'player_name', 'birth_date', 'country_code']
    schema = [
        bigquery.SchemaField('player_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_name', 'STRING'),
        bigquery.SchemaField('birth_date', 'DATE'),
        bigquery.SchemaField('country_code', 'STRING')
]
    players_atp = TennisTable('players', schema, rename, 'atp')
    players_wta = TennisTable('players', schema, rename, 'wta')

    for table in [players_atp, players_wta]:
        table.keep_columns(keep)
        table.rename_columns()
        table.cast_column_to_date('birth_date')
        table.load_to_bq('players')


def load_rankings(incremental=False):

    rename = ['ranking_date', 'player_id', 'ranking_points', 'ranking_position']
    schema = [
        bigquery.SchemaField('ranking_date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('player_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('ranking_points', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('ranking_position', 'INT64', mode='REQUIRED')
]
    rankings_atp = TennisTable('ratings', schema, rename, 'atp', incremental)
    rankings_wta = TennisTable('ratings', schema, rename, 'wta', incremental)

    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [rankings_atp, rankings_wta]:
        table.rename_columns()
        table.cast_column_to_date('ranking_date')
        table.load_to_bq('rankings', write_disposition, partition_field='ranking_date')


def load_rounds():

    rename = ['round_id', 'round']
    schema = [
        bigquery.SchemaField('round_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('round', 'STRING')
]
    rounds = TennisTable('rounds', schema, rename)

    rounds.rename_columns()
    rounds.load_to_bq('rounds')


def load_seeds(incremental=False):

    rename = ['player_id', 'tournament_id', 'seeding']
    schema = [
        bigquery.SchemaField('player_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('seeding', 'STRING', mode='REQUIRED')
]
    seeds_atp = TennisTable('seed', schema, rename, 'atp', incremental)
    seeds_wta = TennisTable('seed', schema, rename, 'wta', incremental)

    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [seeds_atp, seeds_wta]:
        table.rename_columns()
        table.load_to_bq('seeds', write_disposition)


def load_stats(incremental=False):

    drop = ['W1SOF_1', 'W1SOF_2', 'RPWOF_1', 'RPWOF_2']
    rename = ['player_1_id', 'player_2_id', 'tournament_id', 'round_id',
                'p1_first_serve_in', 'p1_first_serve_attempts', 'p1_aces',
                'p1_double_faults', 'p1_unforced_errors',
                'p1_first_serve_won', 'p1_second_serve_won',
                'p1_second_serve_attempts', 'p1_winners',
                'p1_break_points_won', 'p1_break_points_played',
                'p1_net_points_won', 'p1_net_points_played',
                'p1_total_points', 'p1_fastest_serve',
                'p1_avg_first_serve_speed', 'p1_avg_second_serve_speed',
                'p2_first_serve_in', 'p2_first_serve_attempts',
                'p2_aces', 'p2_double_faults', 'p2_unforced_errors',
                'p2_first_serve_won', 'p2_second_serve_won',
                'p2_second_serve_attempts', 'p2_winners',
                'p2_break_points_won', 'p2_break_points_played',
                'p2_net_points_won', 'p2_net_points_played',
                'p2_total_points', 'p2_fastest_serve',
                'p2_avg_first_serve_speed', 'p2_avg_second_serve_speed',
                'p1_return_points_won', 'p2_return_points_won', 'match_duration'
                 ]
    schema = [
        bigquery.SchemaField('player_1_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_2_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('round_id', 'INT64'),
        bigquery.SchemaField('p1_first_serve_in', 'INT64'),
        bigquery.SchemaField('p1_first_serve_attempts', 'INT64'),
        bigquery.SchemaField('p1_aces', 'INT64'),
        bigquery.SchemaField('p1_double_faults', 'INT64'),
        bigquery.SchemaField('p1_unforced_errors', 'INT64'),
        bigquery.SchemaField('p1_first_serve_won', 'INT64'),
        bigquery.SchemaField('p1_second_serve_won', 'INT64'),
        bigquery.SchemaField('p1_second_serve_attempts', 'INT64'),
        bigquery.SchemaField('p1_winners', 'INT64'),
        bigquery.SchemaField('p1_break_points_won', 'INT64'),
        bigquery.SchemaField('p1_break_points_played', 'INT64'),
        bigquery.SchemaField('p1_net_points_won', 'INT64'),
        bigquery.SchemaField('p1_net_points_played', 'INT64'),
        bigquery.SchemaField('p1_total_points', 'INT64'),
        bigquery.SchemaField('p1_fastest_serve', 'INT64'),
        bigquery.SchemaField('p1_avg_first_serve_speed', 'INT64'),
        bigquery.SchemaField('p1_avg_second_serve_speed', 'INT64'),
        bigquery.SchemaField('p2_first_serve_in', 'INT64'),
        bigquery.SchemaField('p2_first_serve_attempts', 'INT64'),
        bigquery.SchemaField('p2_aces', 'INT64'),
        bigquery.SchemaField('p2_double_faults', 'INT64'),
        bigquery.SchemaField('p2_unforced_errors', 'INT64'),
        bigquery.SchemaField('p2_first_serve_won', 'INT64'),
        bigquery.SchemaField('p2_second_serve_won', 'INT64'),
        bigquery.SchemaField('p2_second_serve_attempts', 'INT64'),
        bigquery.SchemaField('p2_winners', 'INT64'),
        bigquery.SchemaField('p2_break_points_won', 'INT64'),
        bigquery.SchemaField('p2_break_points_played', 'INT64'),
        bigquery.SchemaField('p2_net_points_won', 'INT64'),
        bigquery.SchemaField('p2_net_points_played', 'INT64'),
        bigquery.SchemaField('p2_total_points', 'INT64'),
        bigquery.SchemaField('p2_fastest_serve', 'INT64'),
        bigquery.SchemaField('p2_avg_first_serve_speed', 'INT64'),
        bigquery.SchemaField('p2_avg_second_serve_speed', 'INT64'),
        bigquery.SchemaField('p1_return_points_won', 'INT64'),
        bigquery.SchemaField('p2_return_points_won', 'INT64'),
        bigquery.SchemaField('match_duration', 'STRING'),     
]
    stats_atp = TennisTable('stat', schema, rename, 'atp', incremental)
    stats_atp.df = stats_atp.df[76351:]

    stats_wta = TennisTable('stat', schema, rename, 'wta', incremental)
    stats_wta.df = stats_wta.df[39631:]

    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [stats_atp, stats_wta]:
        table.drop_columns(drop)
        table.rename_columns()

        table.df['match_duration'] = table.df['match_duration'].str.split(n=1, expand=True)[1]
        table.df['match_duration'] = pd.to_timedelta(table.df['match_duration']).dt.total_seconds().astype(str)
        table.df['match_duration'] = table.df['match_duration'].str.split('.', n=1, expand=True)[0]

        table.load_to_bq('stats', write_disposition)


def load_today():

    keep = ['TOUR', 'DATE_GAME', 'ID1', 'ID2', 'ROUND', 'RESULT']
    rename = ['tournament_id', 'match_date', 'player_1_id', 'player_2_id', 'round_id', 'result']
    schema = [
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('match_date', 'DATETIME'),
        bigquery.SchemaField('player_1_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('player_2_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('round_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('result', 'STRING')
]
    today_atp = TennisTable('today', schema, rename, 'atp')
    today_wta = TennisTable('today', schema, rename, 'wta')

    for table in [today_atp, today_wta]:
        table.keep_columns(keep)
        table.rename_columns()
        table.cast_column_to_date('match_date')
        table.load_to_bq('today')


def load_tournaments(incremental=False):

    drop = ['LINK_T', 'SITE_T', 'RACE_T', 'ENTRY_T',
                'SINGLES_T', 'DOUBLES_T','RESERVE_INT_T',
                'RESERVE_CHAR_T', 'LIVE_T', 'RESULT_T'
                ]
    rename = ['tournament_id', 'tournament_name', 'surface_id',
                 'tournament_date', 'tournament_level', 'country',
                 'tournament_prize', 'tournament_sub_level', 'tournament_website',
                 'tournament_latitude', 'tournament_longitude', 'tournament_tier'
                ]
    schema = [
        bigquery.SchemaField('tournament_id', 'INT64', mode='REQUIRED'),
        bigquery.SchemaField('tournament_name', 'STRING'),
        bigquery.SchemaField('surface_id', 'INT64'),
        bigquery.SchemaField('tournament_date', 'DATE'),
        bigquery.SchemaField('tournament_level', 'INT64'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('tournament_prize', 'STRING'),
        bigquery.SchemaField('tournament_sub_level', 'INT64'),
        bigquery.SchemaField('tournament_website', 'STRING'),
        bigquery.SchemaField('tournament_latitude', 'FLOAT64'),
        bigquery.SchemaField('tournament_longitude', 'FLOAT64'),
        bigquery.SchemaField('tournament_tier', 'STRING'),
]
    tournaments_atp = TennisTable('tours', schema, rename, 'atp', incremental)
    tournaments_atp.df = tournaments_atp.df[11342:]

    tournaments_wta = TennisTable('tours', schema, rename, 'wta', incremental)
    tournaments_wta.df = tournaments_wta.df[6485:]

    if incremental:
        write_disposition = 'WRITE_APPEND'
    else : write_disposition = 'WRITE_TRUNCATE'

    for table in [tournaments_atp, tournaments_wta]:
        table.drop_columns(drop)
        table.rename_columns()
        table.cast_column_to_date('tournament_date')

        table.load_to_bq('tournaments', write_disposition, partition_field='tournament_date')


def load_match_charting_project():
 
    rename_serve_atp = ['player_name', 'matches_count', 'unreturned_pct',
                            'sp_won_serve_or_second_shot_pct', 'sp_won_when_returned_pct',
                            'serve_impact', 'first_serve_unreturned_pct', 'first_sp_won_serve_or_second_shot_pct',
                            'first_sp_won_when_returned_pct', 'first_serve_impact', 'wide_deuce_court_first_serve_pct',
                            'wide_ad_court_first_serve_pct', 'wide_ad_court_break_point_first_serve_pct',
                            'second_serve_unreturned_pct', 'second_sp_won_serve_or_second_shot_pct',
                            'second_sp_won_when_returned_pct', 'wide_deuce_court_second_serve_pct',
                            'wide_ad_court_second_serve_pct', 'wide_ad_court_break_point_second_serve_pct',
                            'second_serve_agression_score'                        
                            ]
    schema_serve_atp = [
        bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('matches_count', 'INT64'),
        bigquery.SchemaField('unreturned_pct', 'STRING'),
        bigquery.SchemaField('sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('serve_impact', 'STRING'),
        bigquery.SchemaField('first_serve_unreturned_pct', 'STRING'),
        bigquery.SchemaField('first_sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('first_sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('first_serve_impact', 'STRING'),
        bigquery.SchemaField('wide_deuce_court_first_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_first_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_break_point_first_serve_pct', 'STRING'),
        bigquery.SchemaField('second_serve_unreturned_pct', 'STRING'),
        bigquery.SchemaField('second_sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('second_sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('wide_deuce_court_second_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_second_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_break_point_second_serve_pct', 'STRING'),
        bigquery.SchemaField('second_serve_agression_score', 'INT64')
]
    rename_serve_wta = ['player_name', 'matches_count', 'unreturned_pct',
                            'sp_won_serve_or_second_shot_pct', 'sp_won_when_returned_pct',
                            'first_serve_unreturned_pct', 'first_sp_won_serve_or_second_shot_pct',
                            'first_sp_won_when_returned_pct', 'wide_deuce_court_first_serve_pct',
                            'wide_ad_court_first_serve_pct', 'wide_ad_court_break_point_first_serve_pct',
                            'second_serve_unreturned_pct', 'second_sp_won_serve_or_second_shot_pct',
                            'second_sp_won_when_returned_pct', 'wide_deuce_court_second_serve_pct',
                            'wide_ad_court_second_serve_pct', 'wide_ad_court_break_point_second_serve_pct',
                            'second_serve_agression_score'                        
                            ]
    schema_serve_wta = [
        bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('matches_count', 'INT64'),
        bigquery.SchemaField('unreturned_pct', 'STRING'),
        bigquery.SchemaField('sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('first_serve_unreturned_pct', 'STRING'),
        bigquery.SchemaField('first_sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('first_sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('wide_deuce_court_first_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_first_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_break_point_first_serve_pct', 'STRING'),
        bigquery.SchemaField('second_serve_unreturned_pct', 'STRING'),
        bigquery.SchemaField('second_sp_won_serve_or_second_shot_pct', 'STRING'),
        bigquery.SchemaField('second_sp_won_when_returned_pct', 'STRING'),
        bigquery.SchemaField('wide_deuce_court_second_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_second_serve_pct', 'STRING'),
        bigquery.SchemaField('wide_ad_court_break_point_second_serve_pct', 'STRING'),
        bigquery.SchemaField('second_serve_agression_score', 'INT64')
]
    
    rename_return = ['player_name', 'matches_count', 'returned_pct',
                            'points_won_returned_pct', 'winners_pct',
                            'fh_winners_pct', 'return_depth_index',
                            'slice_returns_pct', 'first_serve_returned_pct',
                            'first_serve_points_won_returned_pct', 'first_serve_winners_pct',
                            'first_serve_return_depth_index', 'first_serve_slice_returns_pct',
                            'second_serve_returned_pct', 'second_serve_points_won_returned_pct',
                            'second_serve_winners_pct', 'second_serve_return_depth_index',
                            'second_serve_slice_returns_pct'
                            ]

    schema_return = [
        bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('matches_count', 'INT64'),
        bigquery.SchemaField('returned_pct', 'STRING'),
        bigquery.SchemaField('points_won_returned_pct', 'STRING'),
        bigquery.SchemaField('winners_pct', 'STRING'),
        bigquery.SchemaField('fh_winners_pct', 'STRING'),
        bigquery.SchemaField('return_depth_index', 'FLOAT64'),
        bigquery.SchemaField('slice_returns_pct', 'STRING'),
        bigquery.SchemaField('first_serve_returned_pct', 'STRING'),
        bigquery.SchemaField('first_serve_points_won_returned_pct', 'STRING'),
        bigquery.SchemaField('first_serve_winners_pct', 'STRING'),
        bigquery.SchemaField('first_serve_return_depth_index', 'FLOAT64'),
        bigquery.SchemaField('first_serve_slice_returns_pct', 'STRING'),
        bigquery.SchemaField('second_serve_returned_pct', 'STRING'),
        bigquery.SchemaField('second_serve_points_won_returned_pct', 'STRING'),
        bigquery.SchemaField('second_serve_winners_pct', 'STRING'),
        bigquery.SchemaField('second_serve_return_depth_index', 'FLOAT64'),
        bigquery.SchemaField('second_serve_slice_returns_pct', 'STRING')
]
    
    rename_rally = ['player_name', 'matches_count',
                            'avg_rally_lenght', 'avg_rally_lenght_serve',
                            'avg_rally_lenght_return', 'points_btw_1_3_shots_win_pct',
                            'points_btw_4_6_shots_win_pct', 'points_btw_7_9_shots_win_pct',
                            'points_10_plus_shots_win_pct', 'forehands_per_groundstroke',
                            'sliced_per_backhand_groundstroke', 'forehand_potency_per_100_forehands',
                            'forehand_potency_per_match', 'backhand_potency_per_match',
                            'backhand_potency_per_100_backhands'
                            ]

    schema_rally = [
        bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('matches_count', 'INT64'),
        bigquery.SchemaField('avg_rally_lenght', 'FLOAT64'),
        bigquery.SchemaField('avg_rally_lenght_serve', 'FLOAT64'),
        bigquery.SchemaField('avg_rally_lenght_return', 'FLOAT64'),
        bigquery.SchemaField('points_btw_1_3_shots_win_pct', 'STRING'),
        bigquery.SchemaField('points_btw_4_6_shots_win_pct', 'STRING'),
        bigquery.SchemaField('points_btw_7_9_shots_win_pct', 'STRING'),
        bigquery.SchemaField('points_10_plus_shots_win_pct', 'STRING'),
        bigquery.SchemaField('forehands_per_groundstroke', 'STRING'),
        bigquery.SchemaField('sliced_per_backhand_groundstroke', 'STRING'),
        bigquery.SchemaField('forehand_potency_per_100_forehands', 'FLOAT64'),
        bigquery.SchemaField('forehand_potency_per_match', 'FLOAT64'),
        bigquery.SchemaField('backhand_potency_per_match', 'FLOAT64'),
        bigquery.SchemaField('backhand_potency_per_100_backhands', 'FLOAT64')
]    

    rename_tactics = ['player_name', 'matches_count',
                            'serve_and_volley_pct', 'points_won_per_serve_and_volley',
                            'net_points_pct', 'points_won_per_net_point',
                            'winners_per_forehand', 'winners_per_forehand_down_the_line',
                            'winners_per_forehand_inside_out', 'winners_per_backhand',
                            'winners_per_backhand_down_the_line', 'dropshot_pct',
                            'points_won_per_dropshot', 'rally_agression_score',
                            'return_agression_score'
                            ]

    schema_tactics = [
        bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('matches_count', 'INT64'),
        bigquery.SchemaField('serve_and_volley_pct', 'STRING'),
        bigquery.SchemaField('points_won_per_serve_and_volley', 'STRING'),
        bigquery.SchemaField('net_points_pct', 'STRING'),
        bigquery.SchemaField('points_won_per_net_point', 'STRING'),
        bigquery.SchemaField('winners_per_forehand', 'STRING'),
        bigquery.SchemaField('winners_per_forehand_down_the_line', 'STRING'),
        bigquery.SchemaField('winners_per_forehand_inside_out', 'STRING'),
        bigquery.SchemaField('winners_per_backhand', 'STRING'),
        bigquery.SchemaField('winners_per_backhand_down_the_line', 'STRING'),
        bigquery.SchemaField('dropshot_pct', 'STRING'),
        bigquery.SchemaField('points_won_per_dropshot', 'STRING'),
        bigquery.SchemaField('rally_agression_score', 'INT64'),
        bigquery.SchemaField('return_agression_score', 'INT64')
]
    
    table_dict = {
        'atp_last_52_serve' : TennisTable('atp_last_52_serve', schema_serve_atp, rename_serve_atp, mcp='mcp'),
        'atp_last_52_return' : TennisTable('atp_last_52_return', schema_return, rename_return, mcp='mcp'),
        'atp_last_52_rally' : TennisTable('atp_last_52_rally', schema_rally, rename_rally, mcp='mcp'),
        'atp_last_52_tactics' : TennisTable('atp_last_52_tactics', schema_tactics, rename_tactics, mcp='mcp'),
        'atp_career_serve' : TennisTable('atp_career_serve', schema_serve_atp, rename_serve_atp, mcp='mcp'),
        'atp_career_return' : TennisTable('atp_career_return', schema_return, rename_return, mcp='mcp'),
        'atp_career_rally' : TennisTable('atp_career_rally', schema_rally, rename_rally, mcp='mcp'),
        'atp_career_tactics' : TennisTable('atp_career_tactics', schema_tactics, rename_tactics, mcp='mcp'),
        'wta_last_52_serve' : TennisTable('wta_last_52_serve', schema_serve_wta, rename_serve_wta, mcp='mcp'),
        'wta_last_52_return' : TennisTable('wta_last_52_return', schema_return, rename_return, mcp='mcp'),
        'wta_last_52_rally' : TennisTable('wta_last_52_rally', schema_rally, rename_rally, mcp='mcp'),
        'wta_last_52_tactics' : TennisTable('wta_last_52_tactics', schema_tactics, rename_tactics, mcp='mcp'),
        'wta_career_serve' : TennisTable('wta_career_serve', schema_serve_wta, rename_serve_wta, mcp='mcp'),
        'wta_career_return' : TennisTable('wta_career_return', schema_return, rename_return, mcp='mcp'),
        'wta_career_rally' : TennisTable('wta_career_rally', schema_rally, rename_rally, mcp='mcp'),
        'wta_career_tactics' : TennisTable('wta_career_tactics', schema_tactics, rename_tactics, mcp='mcp')
    }

    for key, value in table_dict.items():
        value.rename_columns()
        value.load_to_bq(key)


def increment_to_bq():    

    load_matches(incremental = True)
    load_odds(incremental = True)
    load_rankings(incremental = True)
    load_seeds(incremental = True)
    load_stats(incremental = True)
    load_tournaments(incremental = True)    


def full_refresh_to_bq():    

    load_categories()
    # load_courts()
    load_matches()
    load_odds()
    load_players()
    load_rankings()
    # load_rounds()
    load_seeds()
    load_stats()
    load_today()
    load_tournaments()
    load_match_charting_project()