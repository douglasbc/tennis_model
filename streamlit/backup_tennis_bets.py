import streamlit as st
import pandas as pd
import numpy as np
import os
import matplotlib.colors as mcolors
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# --------------------------
# CACHE CONFIGURATION
# --------------------------
CACHE_DIR = "tennis_data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Cache file paths
BETS_CACHE = f"{CACHE_DIR}/live_bets.parquet"
ROI_CACHE = f"{CACHE_DIR}/player_roi.parquet"
HISTORY_CACHE = f"{CACHE_DIR}/matches_history.parquet"
H2H_CACHE = f"{CACHE_DIR}/head_to_head.parquet"

# Initialize cache
def init_cache():
    if not os.path.exists(BETS_CACHE):
        pd.DataFrame().to_parquet(BETS_CACHE)
    if not os.path.exists(ROI_CACHE):
        pd.DataFrame().to_parquet(ROI_CACHE)
    if not os.path.exists(HISTORY_CACHE):
        pd.DataFrame().to_parquet(HISTORY_CACHE)
    if not os.path.exists(H2H_CACHE):
        pd.DataFrame().to_parquet(H2H_CACHE)

# --------------------------
# BIGQUERY CLIENT
# --------------------------
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials)

# --------------------------
# DATA LOADING FUNCTIONS
# --------------------------

def load_live_bets(_client):
    # Check if cache exists and is fresh (last 5 minutes)
    if os.path.exists(BETS_CACHE):
        cache_time = datetime.fromtimestamp(os.path.getmtime(BETS_CACHE))
        if datetime.now() - cache_time < timedelta(minutes=5):
            cached = pd.read_parquet(BETS_CACHE)
            # Check if required columns exist
            required_cols = ['tour', 'tournament_round', 'match_start_at', 'p1_name', 'p2_name',
                           'surface', 'p1_pinnacle_odds', 'p2_pinnacle_odds', 'p1_model_odds',
                           'p2_model_odds', 'diff', 'p1_is_left_handed', 'p2_is_left_handed',
                           'p1_rally_cluster', 'p1_net_cluster', 'p1_serve_cluster',
                           'p2_rally_cluster', 'p2_net_cluster', 'p2_serve_cluster']
            if all(col in cached.columns for col in required_cols):
                return cached.sort_values('match_start_at')  # Order by match time

    # Query fresh data with ordering
    query = """
    SELECT 
        tour,
        tournament_round,
        match_start_at,
        p1_name,
        p2_name,
        surface,
        p1_pinnacle_odds,
        p2_pinnacle_odds,
        p1_model_odds,
        p2_model_odds,
        diff,
        p1_is_left_handed,
        p2_is_left_handed,
        p1_rally_cluster,
        p1_net_cluster,
        p1_serve_cluster,
        p2_rally_cluster,
        p2_net_cluster,
        p2_serve_cluster
    FROM `tennis-358702.analytics.streamlit_bets`
    ORDER BY match_start_at ASC
    """
    df = _client.query(query).to_dataframe()
    df.to_parquet(BETS_CACHE)
    return df

def load_player_roi(_client):
    # Check if cache exists and is fresh (last 24 hours)
    if os.path.exists(ROI_CACHE):
        cache_time = datetime.fromtimestamp(os.path.getmtime(ROI_CACHE))
        if datetime.now() - cache_time < timedelta(hours=24):
            cached = pd.read_parquet(ROI_CACHE)
            # Check if required columns exist
            required_cols = ['player_name', 'tour', 'overall_match_win_roi',
                           'hard_match_win_roi', 'clay_match_win_roi', 'grass_match_win_roi',
                           'indoor_hard_match_win_roi', 'vs_left_handed_match_roi',
                           'roi_vs_rally1_match', 'roi_vs_rally2_match', 'roi_vs_rally3_match',
                           'roi_vs_net1_match', 'roi_vs_net2_match', 'roi_vs_net3_match',
                           'roi_vs_serve1_match', 'roi_vs_serve2_match', 'roi_vs_serve3_match']
            if all(col in cached.columns for col in required_cols):
                return cached

    query = """
    SELECT * 
    FROM `tennis-358702.analytics.streamlit_player_roi`
    """
    df = _client.query(query).to_dataframe()
    df.to_parquet(ROI_CACHE)
    return df

def load_matches_history(_client):
    # Check if cache exists and is fresh (last 24 hours)
    if os.path.exists(HISTORY_CACHE):
        cache_time = datetime.fromtimestamp(os.path.getmtime(HISTORY_CACHE))
        if datetime.now() - cache_time < timedelta(hours=24):
            cached = pd.read_parquet(HISTORY_CACHE)
            # Check if required columns exist
            required_cols = ['match_date', 'tournament_name', 'tournament_level', 'round', 'surface',
                           'p1_name', 'p2_name', 'win', 'score', 'p1_win_match_odds', 'p2_win_match_odds',
                           'player_ranking', 'opponent_ranking', 'tour']
            if all(col in cached.columns for col in required_cols):
                return cached

    # Updated query to include both odds
    query = """
    SELECT 
        match_date,
        tournament_name,
        tournament_level,
        round,
        surface,
        player_name,
        opponent,
        p1_name,
        p2_name,
        win,
        score,
        p1_win_match_odds,
        p2_win_match_odds,
        CASE 
            WHEN p1_name = player_name THEN p1_ranking 
            ELSE p2_ranking 
        END AS player_ranking,
        CASE 
            WHEN p1_name = player_name THEN p2_ranking 
            ELSE p1_ranking 
        END AS opponent_ranking,
        tour
    FROM (
        SELECT *,
            p1_name AS player_name,  -- First get wins
            p2_name as opponent,
            1 as win
        FROM `tennis-358702.analytics.streamlit_matches`
        UNION ALL
        
        SELECT *,
            p2_name AS player_name,  -- Then get losses
            p1_name as opponent,
            0 as win
        FROM `tennis-358702.analytics.streamlit_matches`
    )
    """
    df = _client.query(query).to_dataframe()
    df.to_parquet(HISTORY_CACHE)
    return df

def load_head_to_head(_client, player1, player2, tour):
    # Create a safe cache key
    cache_key = f"{player1}_{player2}_{tour}".replace(" ", "_").replace("/", "_")
    cache_file = f"{CACHE_DIR}/h2h_{cache_key}.parquet"

    # Check if cache exists and is fresh (last 24 hours)
    if os.path.exists(cache_file):
        cache_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.now() - cache_time < timedelta(hours=24):
            return pd.read_parquet(cache_file)

    # Query BigQuery if cache is stale or missing
    query = f"""
    SELECT 
        match_date,
        tournament_name,
        tournament_level,
        round,
        surface,
        p1_name AS winner,
        p2_name AS loser,
        p1_ranking AS winner_ranking,
        p2_ranking AS loser_ranking,
        score,
        p1_win_match_odds AS winner_odds,
        p2_win_match_odds AS loser_odds
    FROM `tennis-358702.analytics.streamlit_matches`
    WHERE 
        ((p1_name = '{player1}' AND p2_name = '{player2}')
        OR (p1_name = '{player2}' AND p2_name = '{player1}'))
        AND tour = '{tour}'
    ORDER BY match_date DESC
    """
    df = _client.query(query).to_dataframe()

    # Save to cache
    df.to_parquet(cache_file)
    return df

# --------------------------
# DATA UTILITY FUNCTIONS
# --------------------------

def get_player_history(history_df, player_name, tour):
    return history_df[
        (history_df['player_name'] == player_name) &
        (history_df['tour'] == tour)
    ].sort_values('match_date', ascending=False)

def get_player_roi(roi_df, player_name, tour):
    return roi_df[
        (roi_df['player_name'] == player_name) &
        (roi_df['tour'] == tour)
    ]

def calculate_cluster_roi(roi_data, clusters):
    """Calculate sum of ROI against opponent's clusters"""
    if roi_data.empty or not clusters:
        return "N/A"

    rally_cluster, net_cluster, serve_cluster = clusters
    total_roi = 0
    valid_clusters = 0

    # Rally cluster
    if not pd.isna(rally_cluster):
        col_name = f'roi_vs_rally{int(rally_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            total_roi += roi_data[col_name].iloc[0]
            valid_clusters += 1

    # Net cluster
    if not pd.isna(net_cluster):
        col_name = f'roi_vs_net{int(net_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            total_roi += roi_data[col_name].iloc[0]
            valid_clusters += 1

    # Serve cluster - FIXED
    if not pd.isna(serve_cluster):
        col_name = f'roi_vs_serve{int(serve_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            total_roi += roi_data[col_name].iloc[0]
            valid_clusters += 1

    if valid_clusters == 0:
        return "N/A"

    return total_roi

def format_roi_with_color(roi_value):
    """Format ROI with color coding"""
    if pd.isna(roi_value) or roi_value is None or roi_value == "N/A":
        return "N/A"
    try:
        roi_float = float(roi_value)
        color = "green" if roi_float > 0 else "red" if roi_float < 0 else "gray"
        return f"<span style='color:{color}'>{roi_float:.1f}%</span>"
    except:
        return "N/A"

def get_surface_roi_column(surface):
    surface_map = {
        "Clay": "clay",
        "Grass": "grass",
        "Hard": "hard",
        "Carpet": "indoor_hard",
        "Indoor Hard": "indoor_hard"
    }
    base = surface_map.get(surface, "hard")
    return f"{base}_match_win_roi"

# --------------------------
# UI COMPONENTS
# --------------------------

def style_history_row(row):
    # Win/loss coloring
    base_color = '#d4edda' if row['Result'] == 'Win' else '#f8d7da'

    # Tournament tier coloring
    tier_colors = {
        'Grand Slam': '#FFD700',  # Gold
        'ATP': '#87CEEB',         # Sky Blue
        'WTA': '#FFB6C1',         # Light Pink
        'Challenger': '#98FB98',  # Pale Green
        'Future': '#D3D3D3',      # Light Gray
        'Masters 1000': '#FFA07A' # Light Salmon
    }

    # Apply colors with black text
    styles = [f'background-color: {base_color}; color: black'] * len(row)
    tier_idx = list(row.index).index('Tier')
    tier_color = tier_colors.get(row['Tier'], base_color)
    styles[tier_idx] = f'background-color: {tier_color}; color: black'

    return styles

def render_player_history(history, player_name):
    st.subheader(f"{player_name}'s Match History")

    # Create filters in expander - only surface filter with tick boxes
    with st.expander("Filter History", expanded=False):
        # Surface filter - all unchecked by default
        all_surfaces = history['surface'].unique().tolist()
        surface_colors = {
            'Clay': '#F4A460',    # RGB: 244, 164, 96
            'Grass': '#66CDAA',   # RGB: 102, 205, 170
            'Hard': '#1E90FF',    # RGB: 30, 144, 255
            'Indoor Hard': '#87CEEB'  # RGB: 135, 206, 235
        }

        # Get available surfaces (in case some are missing)
        available_surfaces = [s for s in surface_colors.keys() if s in all_surfaces]

        # Create checkboxes (all unchecked by default)
        selected_surfaces = st.multiselect(
            "Filter by Surface:",
            options=available_surfaces,
            default=[],
            key=f"surfaces_{player_name}"
        )

    # Apply filters only if surfaces are selected
    filtered = history.copy()
    if selected_surfaces:
        filtered = filtered[filtered['surface'].isin(selected_surfaces)]

    # Create dynamic columns
    filtered['Player1'] = np.where(filtered['win'] == 1, filtered['player_name'], filtered['opponent'])
    filtered['Player2'] = np.where(filtered['win'] == 1, filtered['opponent'], filtered['player_name'])
    filtered['R1'] = np.where(filtered['win'] == 1, filtered['player_ranking'], filtered['opponent_ranking'])
    filtered['R2'] = np.where(filtered['win'] == 1, filtered['opponent_ranking'], filtered['player_ranking'])
    filtered['Odds1'] = np.where(filtered['win'] == 1, filtered['p1_win_match_odds'], filtered['p2_win_match_odds'])
    filtered['Odds2'] = np.where(filtered['win'] == 1, filtered['p2_win_match_odds'], filtered['p1_win_match_odds'])

    # Format columns
    filtered['R1'] = filtered['R1'].astype('Int64').astype(str).replace('<NA>', '')
    filtered['R2'] = filtered['R2'].astype('Int64').astype(str).replace('<NA>', '')
    filtered['Odds1'] = filtered['Odds1'].round(2)
    filtered['Odds2'] = filtered['Odds2'].round(2)

    # Format columns - keep all original columns but only show relevant ones
    display_cols = [
        'Player1', 'R1', 'Player2', 'R2',
        'tournament_name', 'match_date', 'round', 'surface',
        'score', 'Odds1', 'Odds2', 'tournament_level', 'win'
    ]

    # Create styled display
    if not filtered.empty:
        filtered['match_date'] = pd.to_datetime(filtered['match_date']).dt.strftime('%Y-%m-%d')

        # Create display DataFrame with renamed columns
        display_df = filtered[display_cols].rename(columns={
            'Player1': 'Player 1',
            'R1': 'R1',
            'Player2': 'Player 2',
            'R2': 'R2',
            'tournament_name': 'Tournament',
            'match_date': 'Date',
            'round': 'Round',
            'surface': 'Surface',
            'score': 'Score',
            'Odds1': 'Odds 1',
            'Odds2': 'Odds 2',
            'tournament_level': 'Level',
            'win': 'Result'
        })

        # Apply styling with colors
        def style_history_row(row):
            # Initialize all styles to white background, black text
            styles = ['background-color: white; color: black'] * len(row)

            # Get indices of player columns
            player1_idx = list(row.index).index('Player 1')
            player2_idx = list(row.index).index('Player 2')
            r1_idx = list(row.index).index('R1')
            r2_idx = list(row.index).index('R2')

            # Apply win/loss coloring to player/ranking columns
            if row['Result'] == 1:  # Player won
                styles[player1_idx] = 'background-color: #d4edda; color: black'  # Light green
                styles[r1_idx] = 'background-color: #d4edda; color: black'      # Light green
                styles[player2_idx] = 'background-color: white; color: black'
                styles[r2_idx] = 'background-color: white; color: black'
            else:  # Player lost
                styles[player2_idx] = 'background-color: #f8d7da; color: black'  # Light red
                styles[r2_idx] = 'background-color: #f8d7da; color: black'        # Light red
                styles[player1_idx] = 'background-color: white; color: black'
                styles[r1_idx] = 'background-color: white; color: black'

            # Surface coloring
            surface_colors = {
                'Clay': '#F4A460',    # Sandy Brown
                'Grass': '#66CDAA',   # Medium Aquamarine
                'Hard': '#1E90FF',    # Dodger Blue
                'Indoor Hard': '#87CEEB'  # Sky Blue
            }

            # Tournament level coloring
            tournament_level_colors = {
                0: '#f5f5f5',  # RGB: 245, 245, 245
                1: '#f0fff0',  # RGB: 240, 255, 240
                2: '#7fffd4',  # RGB: 127, 255, 212
                3: '#00ced1',  # RGB: 0, 206, 209
                4: '#dda0dd',  # RGB: 221, 160, 221
                5: '#ffe7ce',  # RGB: 255, 231, 206
                6: '#87cefa'   # RGB: 135, 206, 250
            }

            # Round coloring
            round_colors = {
                'Bronze Match': '#daafff',  # RGB: 218, 177, 255
                'Final': '#ff8d4b',         # RGB: 255, 141, 75
                'First Round': '#fff8d7',    # RGB: 255, 248, 215
                'Fourth Round': '#ffd500',   # RGB: 255, 213, 0
                'Pre Qualifying': '#f5f5f5', # RGB: 245, 245, 245
                'Qualifying Final Round': '#c0c0c0', # RGB: 192, 192, 192
                'Qualifying First Round': '#f5f5f5', # RGB: 245, 245, 245
                'Qualifying Second Round': '#dcdcdc', # RGB: 220, 220, 220
                'Quarter-Finals': '#ffdeca', # RGB: 255, 222, 203
                'Round Robin': '#bff6ff',    # RGB: 191, 246, 255
                'Rubber 1': '#ffdeca',
                'Rubber 2': '#ffdeca',
                'Rubber 3': '#ffdeca',
                'Rubber 4': '#ffdeca',
                'Rubber 5': '#ffdeca',
                'Second Round': '#fff2b1',   # RGB: 255, 242, 177
                'Semi-Finals': '#ffbd97',    # RGB: 255, 189, 151
                'Third Round': '#ffe97f'     # RGB: 255, 233, 127
            }

            # Apply surface color
            surface_idx = list(row.index).index('Surface')
            if row['Surface'] in surface_colors:
                styles[surface_idx] = f'background-color: {surface_colors[row["Surface"]]}; color: black'

            # Apply tournament level color
            tournament_idx = list(row.index).index('Tournament')
            level = row['Level']
            if level in tournament_level_colors:
                styles[tournament_idx] = f'background-color: {tournament_level_colors[level]}; color: black'

            # Apply round color
            round_idx = list(row.index).index('Round')
            round_value = row['Round']
            if round_value in round_colors:
                styles[round_idx] = f'background-color: {round_colors[round_value]}; color: black'

            # Hide Level and Result columns by making width 0
            level_idx = list(row.index).index('Level')
            styles[level_idx] = 'width: 0px; padding: 0px; margin: 0px; border: 0px;'

            result_idx = list(row.index).index('Result')
            styles[result_idx] = 'width: 0px; padding: 0px; margin: 0px; border: 0px;'

            return styles

        # Reorder columns as requested
        display_df = display_df[['Player 1', 'R1', 'Player 2', 'R2', 'Tournament', 'Date',
                                'Round', 'Surface', 'Score', 'Odds 1', 'Odds 2', 'Level', 'Result']]

        styled_df = display_df.style.apply(style_history_row, axis=1)
        st.dataframe(styled_df, height=400)
    else:
        st.info("No matches found with current filters")

def render_player_roi(roi_data, player_name, surface, is_left_handed_opponent, opponent_clusters):
    if roi_data.empty:
        st.warning(f"No ROI data available for {player_name}")
        return

    # Display all ROIs with color coding
    st.markdown(f"**Overall ROI:** {format_roi_with_color(roi_data['overall_match_win_roi'].iloc[0])}",
                unsafe_allow_html=True)

    # Surface ROI
    surface_col = get_surface_roi_column(surface)
    st.markdown(f"**{surface} ROI:** {format_roi_with_color(roi_data[surface_col].iloc[0])}",
                unsafe_allow_html=True)

    # Left-handed ROI if applicable
    if is_left_handed_opponent:
        st.markdown(f"**vs Left-Handed ROI:** {format_roi_with_color(roi_data['vs_left_handed_match_roi'].iloc[0])}",
                    unsafe_allow_html=True)

    # Cluster ROIs section
    st.markdown("**Cluster ROIs vs Opponent:**", unsafe_allow_html=True)

    rally_cluster, net_cluster, serve_cluster = opponent_clusters
    total_cluster_roi = 0
    valid_clusters = 0

    # Always show all 3 cluster types, even if some are missing
    cluster_display = []

    # Rally cluster
    if not pd.isna(rally_cluster):
        col_name = f'roi_vs_rally{int(rally_cluster)}_match'
        if col_name in roi_data.columns:
            roi_value = roi_data[col_name].iloc[0]
            if not pd.isna(roi_value):
                cluster_display.append(f"- Rally Cluster {int(rally_cluster)}: {format_roi_with_color(roi_value)}")
                total_cluster_roi += roi_value
                valid_clusters += 1
            else:
                cluster_display.append(f"- Rally Cluster {int(rally_cluster)}: No data")
        else:
            cluster_display.append(f"- Rally Cluster {int(rally_cluster)}: Column missing")
    else:
        cluster_display.append("- Rally Cluster: Not available")

    # Net cluster
    if not pd.isna(net_cluster):
        col_name = f'roi_vs_net{int(net_cluster)}_match'
        if col_name in roi_data.columns:
            roi_value = roi_data[col_name].iloc[0]
            if not pd.isna(roi_value):
                cluster_display.append(f"- Net Cluster {int(net_cluster)}: {format_roi_with_color(roi_value)}")
                total_cluster_roi += roi_value
                valid_clusters += 1
            else:
                cluster_display.append(f"- Net Cluster {int(net_cluster)}: No data")
        else:
            cluster_display.append(f"- Net Cluster {int(net_cluster)}: Column missing")
    else:
        cluster_display.append("- Net Cluster: Not available")

    # Serve cluster
    if not pd.isna(serve_cluster):
        col_name = f'roi_vs_serve{int(serve_cluster)}_match'
        if col_name in roi_data.columns:
            roi_value = roi_data[col_name].iloc[0]
            if not pd.isna(roi_value):
                cluster_display.append(f"- Serve Cluster {int(serve_cluster)}: {format_roi_with_color(roi_value)}")
                total_cluster_roi += roi_value
                valid_clusters += 1
            else:
                cluster_display.append(f"- Serve Cluster {int(serve_cluster)}: No data")
        else:
            cluster_display.append(f"- Serve Cluster {int(serve_cluster)}: Column missing")
    else:
        cluster_display.append("- Serve Cluster: Not available")

    # Display all cluster information
    for line in cluster_display:
        st.markdown(line, unsafe_allow_html=True)

    # Display total cluster ROI if we have any valid clusters
    if valid_clusters > 0:
        st.markdown(f"**Total Cluster ROI:** {format_roi_with_color(total_cluster_roi)}",
                    unsafe_allow_html=True)
    else:
        st.markdown("**Total Cluster ROI:** No valid cluster data available")

def render_head_to_head(h2h_df, player1, player2):
    if h2h_df.empty:
        st.info("No previous matches found between these players.")
        return

    # Calculate statistics
    player1_wins = h2h_df[h2h_df['winner'] == player1].shape[0]
    player2_wins = h2h_df[h2h_df['winner'] == player2].shape[0]
    total_matches = len(h2h_df)

    # Display stats
    col1, col2, col3 = st.columns(3)
    col1.metric(f"{player1} Wins", player1_wins, f"{player1_wins/total_matches*100:.1f}%")
    col2.metric(f"{player2} Wins", player2_wins, f"{player2_wins/total_matches*100:.1f}%")
    col3.metric("Total Matches", total_matches)

    # Create Player 1 (winner) and Player 2 (loser)
    h2h_df['Player 1'] = h2h_df['winner']
    h2h_df['Player 2'] = h2h_df['loser']
    h2h_df['R1'] = h2h_df['winner_ranking'].astype('Int64').astype(str).replace('<NA>', '')
    h2h_df['R2'] = h2h_df['loser_ranking'].astype('Int64').astype(str).replace('<NA>', '')
    h2h_df['Odds 1'] = h2h_df['winner_odds'].round(2)
    h2h_df['Odds 2'] = h2h_df['loser_odds'].round(2)
    h2h_df['Date'] = pd.to_datetime(h2h_df['match_date']).dt.strftime('%Y-%m-%d')

    # Reorder columns
    display_df = h2h_df[['Player 1', 'R1', 'Player 2', 'R2', 'tournament_name', 'Date',
                         'round', 'surface', 'score', 'Odds 1', 'Odds 2']]
    display_df.columns = ['Player 1', 'R1', 'Player 2', 'R2', 'Tournament', 'Date',
                          'Round', 'Surface', 'Score', 'Odds 1', 'Odds 2']

    # Style the DataFrame
    def style_h2h_row(row):
        # Initialize all styles to white background, black text
        styles = ['background-color: white; color: black'] * len(row)

        # Color Player 1 (winner) green
        player1_idx = list(row.index).index('Player 1')
        r1_idx = list(row.index).index('R1')
        styles[player1_idx] = 'background-color: #d4edda; color: black'  # Light green
        styles[r1_idx] = 'background-color: #d4edda; color: black'      # Light green

        # Color Player 2 (loser) red
        player2_idx = list(row.index).index('Player 2')
        r2_idx = list(row.index).index('R2')
        styles[player2_idx] = 'background-color: #f8d7da; color: black'  # Light red
        styles[r2_idx] = 'background-color: #f8d7da; color: black'      # Light red

        # Tournament level coloring
        tournament_level_colors = {
            0: '#f5f5f5',  # RGB: 245, 245, 245
            1: '#f0fff0',  # RGB: 240, 255, 240
            2: '#7fffd4',  # RGB: 127, 255, 212
            3: '#00ced1',  # RGB: 0, 206, 209
            4: '#dda0dd',  # RGB: 221, 160, 221
            5: '#ffe7ce',  # RGB: 255, 231, 206
            6: '#87cefa'   # RGB: 135, 206, 250
        }

        # Round coloring
        round_colors = {
            'Bronze Match': '#daafff',  # RGB: 218, 177, 255
            'Final': '#ff8d4b',         # RGB: 255, 141, 75
            'First Round': '#fff8d7',    # RGB: 255, 248, 215
            'Fourth Round': '#ffd500',   # RGB: 255, 213, 0
            'Pre Qualifying': '#f5f5f5', # RGB: 245, 245, 245
            'Qualifying Final Round': '#c0c0c0', # RGB: 192, 192, 192
            'Qualifying First Round': '#f5f5f5', # RGB: 245, 245, 245
            'Qualifying Second Round': '#dcdcdc', # RGB: 220, 220, 220
            'Quarter-Finals': '#ffdeca', # RGB: 255, 222, 203
            'Round Robin': '#bff6ff',    # RGB: 191, 246, 255
            'Rubber 1': '#ffdeca',
            'Rubber 2': '#ffdeca',
            'Rubber 3': '#ffdeca',
            'Rubber 4': '#ffdeca',
            'Rubber 5': '#ffdeca',
            'Second Round': '#fff2b1',   # RGB: 255, 242, 177
            'Semi-Finals': '#ffbd97',    # RGB: 255, 189, 151
            'Third Round': '#ffe97f'     # RGB: 255, 233, 127
        }

        # Surface coloring
        surface_colors = {
            'Clay': '#F4A460',    # Sandy Brown
            'Grass': '#66CDAA',   # Medium Aquamarine
            'Hard': '#1E90FF',    # Dodger Blue
            'Indoor Hard': '#87CEEB'  # Sky Blue
        }

        # Apply tournament level color
        tournament_idx = list(row.index).index('Tournament')
        # Since we don't have tournament_level in display, we skip this coloring
        # If you want to add it, you'll need to include it in the DataFrame

        # Apply round color
        round_idx = list(row.index).index('Round')
        round_value = row['Round']
        if round_value in round_colors:
            styles[round_idx] = f'background-color: {round_colors[round_value]}; color: black'

        # Apply surface color
        surface_idx = list(row.index).index('Surface')
        if row['Surface'] in surface_colors:
            styles[surface_idx] = f'background-color: {surface_colors[row["Surface"]]}; color: black'

        return styles

    styled_df = display_df.style.apply(style_h2h_row, axis=1)
    st.dataframe(styled_df)

def player_view(player_name, player_history, player_roi, surface, is_left_handed_opponent, opponent_clusters):
    st.subheader(player_name)
    render_player_roi(player_roi, player_name, surface, is_left_handed_opponent, opponent_clusters)
    render_player_history(player_history, player_name)

# --------------------------
# MAIN APPLICATION
# --------------------------

def main():
    st.set_page_config(layout="wide", page_title="Tennis Betting Assistant", page_icon="🎾")
    init_cache()
    client = get_bq_client()

    # Preload all data
    with st.spinner("Loading data..."):
        bets_df = load_live_bets(client)
        roi_df = load_player_roi(client)
        history_df = load_matches_history(client)

    # Sidebar controls
    st.sidebar.title("🎾 Tennis Betting Assistant")

    # Date filter
    date_options = ["Today", "Tomorrow", "Next 3 Days", "All Upcoming"]
    date_filter = st.sidebar.selectbox("Date Range", date_options, index=0)

    # Tournament round filter
    all_rounds = bets_df['tournament_round'].unique().tolist()
    selected_rounds = st.sidebar.multiselect(
        "Tournament Rounds",
        options=all_rounds,
        default=all_rounds
    )

    # Surface filter
    all_surfaces = bets_df['surface'].unique().tolist()
    selected_surfaces = st.sidebar.multiselect(
        "Surfaces",
        options=all_surfaces,
        default=all_surfaces
    )

    # Cluster data filter
    show_cluster_matches = st.sidebar.checkbox("Only matches with cluster data", value=False)

    # Filter bets data
    today = datetime.now().date()
    if date_filter == "Today":
        filtered_bets = bets_df[pd.to_datetime(bets_df['match_start_at']).dt.date == today]
    elif date_filter == "Tomorrow":
        filtered_bets = bets_df[pd.to_datetime(bets_df['match_start_at']).dt.date == today + timedelta(days=1)]
    elif date_filter == "Next 3 Days":
        filtered_bets = bets_df[pd.to_datetime(bets_df['match_start_at']).dt.date <= today + timedelta(days=3)]
    else:
        filtered_bets = bets_df.copy()

    # Apply additional filters
    filtered_bets = filtered_bets[filtered_bets['tournament_round'].isin(selected_rounds)]
    filtered_bets = filtered_bets[filtered_bets['surface'].isin(selected_surfaces)]

    # Apply cluster filter if enabled
    if show_cluster_matches:
        filtered_bets = filtered_bets.dropna(subset=['p1_rally_cluster', 'p2_rally_cluster'])

    # Order by match_start_at ascending
    filtered_bets = filtered_bets.sort_values('match_start_at')

    # Display matches
    st.title("🎾 Tennis Betting Assistant")

    # Display match count
    st.subheader(f"Upcoming Matches ({len(filtered_bets)})")

    # Create formatted display
    display_df = filtered_bets.copy()
    display_df['Date'] = pd.to_datetime(display_df['match_start_at']).dt.strftime('%Y-%m-%d %H:%M')
    display_df['Diff'] = display_df['diff'].apply(lambda x: f"{x:.2f}↑" if x > 0 else f"{abs(x):.2f}↓" if x < 0 else "0.00")

    # Display matches in a table
    for _, row in display_df.iterrows():
        # Get ROI data for both players
        p1_roi = get_player_roi(roi_df, row['p1_name'], row['tour'])
        p2_roi = get_player_roi(roi_df, row['p2_name'], row['tour'])

        # Get clusters
        p1_clusters = (row['p1_rally_cluster'], row['p1_net_cluster'], row['p1_serve_cluster'])
        p2_clusters = (row['p2_rally_cluster'], row['p2_net_cluster'], row['p2_serve_cluster'])

        # Calculate cluster ROIs with 2 decimal places
        p1_cluster_roi = calculate_cluster_roi(p1_roi, p2_clusters)
        p2_cluster_roi = calculate_cluster_roi(p2_roi, p1_clusters)

        # Format for display
        p1_cluster_display = f"{p1_cluster_roi:.2f}%" if isinstance(p1_cluster_roi, (int, float)) else p1_cluster_roi
        p2_cluster_display = f"{p2_cluster_roi:.2f}%" if isinstance(p2_cluster_roi, (int, float)) else p2_cluster_roi

        # Create columns for match display
        cols = st.columns([0.8, 1.5, 1.2, 2, 1, 1, 1, 1, 1, 1, 1, 1])
        cols[0].write(f"**{row['tour']}**")
        cols[1].write(f"**{row['Date']}**")
        cols[2].write(f"**{row['tournament_round']}**")
        cols[3].write(f"**{row['p1_name']}** vs **{row['p2_name']}**")
        cols[4].write(f"{row['surface']}")

        # Helper function to format value and determine color
        def format_value_with_color(value):
            try:
                # Handle string values with % sign or arrows
                if isinstance(value, str):
                    clean_value = value.replace('%', '').replace('↑', '').replace('↓', '').strip()
                    numeric_value = float(clean_value)
                else:
                    numeric_value = float(value)

                # Format as percentage with 1 decimal place
                formatted_value = f"{numeric_value:.1f}%"

                # Determine color
                color = "green" if numeric_value > 0 else "red" if numeric_value < 0 else "black"

                return formatted_value, color
            except (ValueError, TypeError):
                return str(value), "black"

        # Format Diff column (convert to percentage)
        diff_value, diff_color = format_value_with_color(row['Diff'])
        cols[5].markdown(f"<span style='color:{diff_color}'>{diff_value}</span>", unsafe_allow_html=True)

        # Player 1 odds and ROI
        cols[6].write(f"P1 P: {row['p1_pinnacle_odds']:.2f}")
        cols[7].write(f"P1 M: {row['p1_model_odds']:.2f}")

        # Format p1_cluster_display
        p1_value, p1_color = format_value_with_color(p1_cluster_display)
        cols[8].markdown(f"<span style='color:{p1_color}'>P1 ROI: {p1_value}</span>", unsafe_allow_html=True)

        # Player 2 odds and ROI
        cols[9].write(f"P2 P: {row['p2_pinnacle_odds']:.2f}")
        cols[10].write(f"P2 M: {row['p2_model_odds']:.2f}")

        # Format p2_cluster_display
        p2_value, p2_color = format_value_with_color(p2_cluster_display)
        cols[11].markdown(f"<span style='color:{p2_color}'>P2 ROI: {p2_value}</span>", unsafe_allow_html=True)


        if cols[0].button("Analyze", key=f"analyze_{row['p1_name']}_{row['p2_name']}_{row['match_start_at']}"):
            st.session_state.selected_match = row

    # Match detail view
    if 'selected_match' in st.session_state:
        match = st.session_state.selected_match
        st.divider()
        st.subheader(f"Detailed Analysis: {match['p1_name']} vs {match['p2_name']}")
        st.caption(f"{match['Date']} | {match['tour']} | {match['tournament_round']} | {match['surface']}")

        # Get player ROI
        p1_roi = get_player_roi(roi_df, match['p1_name'], match['tour'])
        p2_roi = get_player_roi(roi_df, match['p2_name'], match['tour'])

        # Get player history
        p1_history = get_player_history(history_df, match['p1_name'], match['tour'])
        p2_history = get_player_history(history_df, match['p2_name'], match['tour'])

        # Get cluster data
        p1_clusters = (match['p1_rally_cluster'], match['p1_net_cluster'], match['p1_serve_cluster'])
        p2_clusters = (match['p2_rally_cluster'], match['p2_net_cluster'], match['p2_serve_cluster'])

        # Head-to-head section
        st.subheader("Head-to-Head History")
        h2h_df = load_head_to_head(client, match['p1_name'], match['p2_name'], match['tour'])
        render_head_to_head(h2h_df, match['p1_name'], match['p2_name'])

        # Player view columns
        st.subheader("Player Analysis")
        col1, col2 = st.columns(2)

        with col1:
            player_view(
                match['p1_name'], p1_history, p1_roi,
                match['surface'], match['p2_is_left_handed'], p2_clusters
            )

        with col2:
            player_view(
                match['p2_name'], p2_history, p2_roi,
                match['surface'], match['p1_is_left_handed'], p1_clusters
            )

        # Value analysis
        st.subheader("Value Analysis")
        col1, col2 = st.columns(2)

        with col1:
            if not p1_roi.empty:
                model_prob = 1 / match['p1_model_odds']
                implied_prob = 1 / match['p1_pinnacle_odds']
                value = model_prob - implied_prob
                st.metric(f"{match['p1_name']} Value",
                         f"{value*100:.1f}%",
                         delta="Positive Value" if value > 0 else "Negative Value",
                         delta_color="normal")

        with col2:
            if not p2_roi.empty:
                model_prob = 1 / match['p2_model_odds']
                implied_prob = 1 / match['p2_pinnacle_odds']
                value = model_prob - implied_prob
                st.metric(f"{match['p2_name']} Value",
                         f"{value*100:.1f}%",
                         delta="Positive Value" if value > 0 else "Negative Value",
                         delta_color="normal")

        # Bet recommendation
        st.subheader("Betting Recommendation")
        if not p1_roi.empty and not p2_roi.empty:
            p1_value = (1 / match['p1_model_odds'] - 1/match['p1_pinnacle_odds']) * 100
            p2_value = (1 / match['p2_model_odds'] - 1/match['p2_pinnacle_odds']) * 100

            if p1_value > 0 and p1_value > p2_value:
                st.success(f"✅ Recommended Bet: **{match['p1_name']}** (Value: {p1_value:.1f}%)")
            elif p2_value > 0 and p2_value > p1_value:
                st.success(f"✅ Recommended Bet: **{match['p2_name']}** (Value: {p2_value:.1f}%)")
            else:
                st.warning("⚠️ No Clear Value Bet - Both players show negative or neutral value")
        else:
            st.info("Insufficient data for betting recommendation")

        if st.button("← Back to Matches"):
            del st.session_state.selected_match
            st.rerun()

if __name__ == "__main__":
    main()