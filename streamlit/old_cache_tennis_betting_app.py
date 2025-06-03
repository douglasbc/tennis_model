import streamlit as st
import pandas as pd
import numpy as np
import os
import json
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

# Initialize cache
def init_cache():
    if not os.path.exists(BETS_CACHE):
        pd.DataFrame().to_parquet(BETS_CACHE)
    if not os.path.exists(ROI_CACHE):
        pd.DataFrame().to_parquet(ROI_CACHE)
    if not os.path.exists(HISTORY_CACHE):
        pd.DataFrame().to_parquet(HISTORY_CACHE)

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
                return cached

    # Query fresh data
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
            required_cols = ['match_date', 'tournament_name', 'tournament_tier', 'surface',
                           'player_name', 'opponent', 'win', 'score', 'odds',
                           'player_ranking', 'opponent_ranking', 'tour']
            if all(col in cached.columns for col in required_cols):
                return cached

    query = """
    SELECT 
        match_date,
        tournament_name,
        tournament_tier,
        surface,
        player_name,
        opponent,
        win,
        score,
        odds,
        player_ranking,
        opponent_ranking,
        tour
    FROM `tennis-358702.analytics.streamlit_matches`
    """
    df = _client.query(query).to_dataframe()
    df.to_parquet(HISTORY_CACHE)
    return df

# --------------------------
# DATA UTILITY FUNCTIONS
# --------------------------

def get_player_history(history_df, player_name, tour):
    """Get unique matches from player's perspective"""
    return history_df[
        (history_df['player_name'] == player_name) &
        (history_df['tour'] == tour)
    ].sort_values('match_date', ascending=False).drop_duplicates(subset=['match_date', 'opponent'])

def get_player_roi(roi_df, player_name, tour):
    return roi_df[
        (roi_df['player_name'] == player_name) &
        (roi_df['tour'] == tour)
    ]

def format_roi(roi_value):
    if pd.isna(roi_value) or roi_value is None:
        return "N/A"
    try:
        roi_float = float(roi_value)
        color = "green" if roi_float > 0 else "red" if roi_float < 0 else "gray"
        return f"{roi_float:.1f}%"
    except:
        return "N/A"

def format_diff(diff_value):
    if pd.isna(diff_value) or diff_value is None:
        return "0.00"
    try:
        diff_float = float(diff_value)
        return f"{diff_float:.2f}↑" if diff_float > 0 else f"{abs(diff_float):.2f}↓" if diff_float < 0 else "0.00"
    except:
        return "0.00"

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

    # Serve cluster
    if not pd.isna(serve_cluster):
        col_name = f'roi_vs_serve{int(serve_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            total_roi += roi_data[col_name].iloc[0]
            valid_clusters += 1

    if valid_clusters == 0:
        return "N/A"

    return total_roi

# --------------------------
# UI COMPONENTS
# --------------------------


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

    # Apply colors
    styles = [f'background-color: {base_color}'] * len(row)
    tier_idx = list(row.index).index('Tier')
    tier_color = tier_colors.get(row['Tier'], base_color)
    styles[tier_idx] = f'background-color: {tier_color}'

    return styles

def render_player_history(history, player_name):
    st.subheader(f"{player_name}'s Match History")

    # Create filters in expander
    with st.expander("Filter History", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            # Opponent ranking filter
            min_rank, max_rank = st.slider(
                "Opponent Ranking Range",
                min_value=1,
                max_value=500,
                value=(1, 500),
                key=f"rank_{player_name}"
            )

            # Tournament tier filter
            all_tiers = history['tournament_tier'].unique().tolist()
            selected_tiers = st.multiselect(
                "Tournament Tiers",
                options=all_tiers,
                default=all_tiers,
                key=f"tiers_{player_name}"
            )

        with col2:
            # Odds filter
            min_odds, max_odds = st.slider(
                "Match Odds Range",
                min_value=float(history['odds'].min()) if not history.empty else 1.0,
                max_value=float(history['odds'].max()) if not history.empty else 10.0,
                value=(1.0, 10.0),
                key=f"odds_{player_name}"
            )

            # Surface filter
            all_surfaces = history['surface'].unique().tolist()
            selected_surfaces = st.multiselect(
                "Surfaces",
                options=all_surfaces,
                default=all_surfaces,
                key=f"surfaces_{player_name}"
            )

    # Apply filters
    filtered = history.copy()
    filtered = filtered[filtered['opponent_ranking'].between(min_rank, max_rank)]
    filtered = filtered[filtered['tournament_tier'].isin(selected_tiers)]
    filtered = filtered[filtered['odds'].between(min_odds, max_odds)]
    filtered = filtered[filtered['surface'].isin(selected_surfaces)]

    # Format columns
    display_cols = [
        'match_date', 'tournament_name', 'tournament_tier', 'surface',
        'opponent', 'player_ranking',
        'opponent_ranking', 'odds', 'win', 'score'
    ]

    # Create styled display
    if not filtered.empty:
        filtered['match_date'] = pd.to_datetime(filtered['match_date']).dt.strftime('%Y-%m-%d')
        filtered['win'] = filtered['win'].apply(lambda x: 'Win' if x == 1 else 'Loss')

        # Convert rankings to string, handling null values properly
        filtered['player_ranking'] = filtered['player_ranking'].astype('Int64').astype(str).replace('<NA>', '')
        filtered['opponent_ranking'] = filtered['opponent_ranking'].astype('Int64').astype(str).replace('<NA>', '')

        styled_df = (
            filtered[display_cols]
            .rename(columns={
                'match_date': 'Date',
                'tournament_name': 'Tournament',
                'tournament_tier': 'Tier',
                'surface': 'Surface',
                'opponent': 'Opponent',
                'player_ranking': 'Ranking',
                'opponent_ranking': 'Opp Ranking',
                'odds': 'Odds',
                'win': 'Result',
                'score': 'Score'
            })
            .style.apply(style_history_row, axis=1)
        )

        st.dataframe(styled_df, height=400)
    else:
        st.info("No matches found with current filters")


def render_player_roi(roi_data, player_name, surface, is_left_handed_opponent, opponent_clusters):
    if roi_data.empty:
        st.warning(f"No ROI data available for {player_name}")
        return

    # Calculate cluster sum
    cluster_sum = calculate_cluster_roi(roi_data, opponent_clusters)

    # Display all ROIs with color coding
    st.markdown(f"**Overall ROI:** {format_roi_with_color(roi_data['overall_match_win_roi'].iloc[0])}",
                unsafe_allow_html=True)

    surface_col = get_surface_roi_column(surface)
    st.markdown(f"**{surface} ROI:** {format_roi_with_color(roi_data[surface_col].iloc[0])}",
                unsafe_allow_html=True)

    if is_left_handed_opponent:
        st.markdown(f"**vs Left-Handed ROI:** {format_roi_with_color(roi_data['vs_left_handed_match_roi'].iloc[0])}",
                    unsafe_allow_html=True)

    st.markdown("**Cluster ROIs vs Opponent:**", unsafe_allow_html=True)
    rally_cluster, net_cluster, serve_cluster = opponent_clusters

    # Display all available cluster ROIs
    cluster_shown = False

    # Rally cluster
    if not pd.isna(rally_cluster):
        col_name = f'roi_vs_rally{int(rally_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            st.markdown(f"- Rally Cluster {int(rally_cluster)}: {format_roi_with_color(roi_data[col_name].iloc[0])}",
                        unsafe_allow_html=True)
            cluster_shown = True

    # Net cluster
    if not pd.isna(net_cluster):
        col_name = f'roi_vs_net{int(net_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            st.markdown(f"- Net Cluster {int(net_cluster)}: {format_roi_with_color(roi_data[col_name].iloc[0])}",
                        unsafe_allow_html=True)
            cluster_shown = True

    # Serve cluster - FIXED: Now properly checking if column exists and has value
    if not pd.isna(serve_cluster):
        col_name = f'roi_vs_serve{int(serve_cluster)}_match'
        if col_name in roi_data.columns and not pd.isna(roi_data[col_name].iloc[0]):
            st.markdown(f"- Serve Cluster {int(serve_cluster)}: {format_roi_with_color(roi_data[col_name].iloc[0])}",
                        unsafe_allow_html=True)
            cluster_shown = True

    if not cluster_shown:
        st.markdown("- No cluster data available")

    # Display total cluster ROI with color and 2 decimal places
    if cluster_sum != "N/A":
        st.markdown(f"**Total Cluster ROI:** {format_roi_with_color(round(cluster_sum, 2))}",
                    unsafe_allow_html=True)
    else:
        st.markdown("**Total Cluster ROI:** N/A")

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

    # Tournament tier filter
    all_tiers = bets_df['tournament_round'].unique().tolist()
    selected_tiers = st.sidebar.multiselect(
        "Tournament Rounds",
        options=all_tiers,
        default=all_tiers
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
    filtered_bets = filtered_bets[filtered_bets['tournament_round'].isin(selected_tiers)]
    filtered_bets = filtered_bets[filtered_bets['surface'].isin(selected_surfaces)]

    # Apply cluster filter if enabled
    if show_cluster_matches:
        filtered_bets = filtered_bets.dropna(subset=['p1_rally_cluster', 'p2_rally_cluster'])

    # Display matches
    st.title("🎾 Tennis Betting Assistant")

    # Display match count
    st.subheader(f"Upcoming Matches ({len(filtered_bets)})")

    # Create formatted display
    display_df = filtered_bets.copy()
    display_df['Date'] = pd.to_datetime(display_df['match_start_at']).dt.strftime('%Y-%m-%d %H:%M')
    display_df['Diff'] = display_df['diff'].apply(format_diff)

    # Display matches in a table
    for _, row in display_df.iterrows():
        # Get ROI data for both players
        p1_roi = get_player_roi(roi_df, row['p1_name'], row['tour'])
        p2_roi = get_player_roi(roi_df, row['p2_name'], row['tour'])

        # Get clusters
        p1_clusters = (row['p1_rally_cluster'], row['p1_net_cluster'], row['p1_serve_cluster'])
        p2_clusters = (row['p2_rally_cluster'], row['p2_net_cluster'], row['p2_serve_cluster'])

        # Calculate cluster ROIs with 2 decimal places
        p1_cluster_roi = round(float(calculate_cluster_roi(p1_roi, p2_clusters)), 2) if calculate_cluster_roi(p1_roi, p2_clusters) != "N/A" else "N/A"
        p2_cluster_roi = round(float(calculate_cluster_roi(p2_roi, p1_clusters)), 2) if calculate_cluster_roi(p2_roi, p1_clusters) != "N/A" else "N/A"

        # Create columns for match display
        cols = st.columns([0.8, 1.2, 1.5, 2, 1, 1, 1, 1, 1, 1, 1, 1])
        cols[0].write(f"**{row['tour']}**")
        cols[1].write(f"**{row['Date']}**")
        cols[2].write(f"**{row['tournament_round']}**")
        cols[3].write(f"**{row['p1_name']}** vs **{row['p2_name']}**")
        cols[4].write(f"{row['surface']}")

        # Player 1 odds and ROI (formatted to 2 decimal places)
        cols[5].write(f"P1 P: {row['p1_pinnacle_odds']:.2f}")
        cols[6].write(f"P1 M: {row['p1_model_odds']:.2f}")
        cols[7].write(f"P1 ROI: {p1_cluster_roi if isinstance(p1_cluster_roi, str) else f'{p1_cluster_roi:.2f}%'}")

        # Player 2 odds and ROI (formatted to 2 decimal places)
        cols[8].write(f"P2 P: {row['p2_pinnacle_odds']:.2f}")
        cols[9].write(f"P2 M: {row['p2_model_odds']:.2f}")
        cols[10].write(f"P2 ROI: {p2_cluster_roi if isinstance(p2_cluster_roi, str) else f'{p2_cluster_roi:.2f}%'}")

        # Diff column
        cols[11].write(f"Diff: {row['Diff']}")

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

        # Player view columns
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