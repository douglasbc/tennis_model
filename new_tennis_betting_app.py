import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# Initialize BigQuery client
def init_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    return client

# Get unique tournament tiers from data
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_unique_tiers():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)

    query = """
    SELECT DISTINCT tournament_tier
    FROM `tennis-358702.analytics.atp_bets`
    UNION DISTINCT
    SELECT DISTINCT tournament_tier
    FROM `tennis-358702.analytics.wta_bets`
    ORDER BY tournament_tier
    """
    results = client.query(query).result()
    return [row.tournament_tier for row in results]

# Get matches data
def get_matches_data(client, tours, tiers):
    query = f"""
    SELECT 
        'ATP' as tour, 
        tournament_tier,
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
    FROM `tennis-358702.analytics.atp_bets`
    WHERE tournament_tier IN UNNEST(@tiers)
    UNION ALL
    SELECT 
        'WTA' as tour, 
        tournament_tier,
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
    FROM `tennis-358702.analytics.wta_bets`
    WHERE tournament_tier IN UNNEST(@tiers)
    ORDER BY match_start_at DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tiers", "STRING", tiers)
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()

# Get player ROI data
def get_player_roi(client, player_name, tour):
    table = "atp_roi" if tour == "ATP" else "wta_roi"
    query = f"""
    SELECT * 
    FROM `tennis-358702.analytics.{table}`
    WHERE player_name = @player_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_name", "STRING", player_name)
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()

# Get head-to-head data
def get_head_to_head(client, player1, player2, tour):
    table = "atp_matches" if tour == "ATP" else "wta_matches"
    query = f"""
    SELECT match_date, tournament_name, surface, p1_name as winner_name, p2_name as loser_name, result as score,
           p1_win_match_odds, p2_win_match_odds
    FROM `tennis-358702.silver.{table}`
    WHERE (p1_name = @player1 AND p2_name = @player2)
       OR (p1_name = @player2 AND p2_name = @player1)
    ORDER BY match_date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player1", "STRING", player1),
            bigquery.ScalarQueryParameter("player2", "STRING", player2)
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()

# Get player match history (updated for 2024 onwards)
def get_player_history(client, player_name, tour):
    table = "atp_matches" if tour == "ATP" else "wta_matches"
    query = f"""
    SELECT match_date, tournament_name, surface, 
           CASE WHEN p1_name = @player_name THEN p2_name ELSE p1_name END AS opponent,
           CASE WHEN p1_name = @player_name THEN 1 ELSE 0 END AS win,
           result as score, 
           CASE WHEN p1_name = @player_name THEN p1_win_match_odds ELSE p2_win_match_odds END AS odds
    FROM `tennis-358702.silver.{table}`
    WHERE (p1_name = @player_name OR p2_name = @player_name)
      AND match_date >= '2024-01-01'
    ORDER BY match_date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_name", "STRING", player_name)
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()

# Format ROI display
def format_roi(roi_value):
    if pd.isna(roi_value) or roi_value is None:
        return "N/A"
    try:
        roi_float = float(roi_value)
        color = "green" if roi_float > 0 else "red" if roi_float < 0 else "gray"
        return f"{roi_float:.1f}%"
    except:
        return "N/A"

# Format difference display
def format_diff(diff_value):
    if pd.isna(diff_value) or diff_value is None:
        return "N/A"
    try:
        diff_float = float(diff_value)
        arrow = "↓" if diff_float < 0 else "↑" if diff_float > 0 else ""
        return f"{diff_float:.2f}{arrow}"
    except:
        return "N/A"

# Format odds display
def format_odds(odds_value):
    if pd.isna(odds_value) or odds_value is None:
        return "N/A"
    try:
        return f"{odds_value:.2f}"
    except:
        return "N/A"

# Get surface-specific ROI column name
def get_surface_roi_column(surface, bet_type="match"):
    surface_map = {
        "Clay": "clay",
        "Grass": "grass",
        "Hard": "hard",
        "Carpet": "indoor_hard",
        "Indoor Hard": "indoor_hard"
    }

    base = surface_map.get(surface, "hard")
    return f"{base}_match_win_roi"

# Format match history with color coding
def format_history_row(row):
    if row['Win'] == 1:
        return ['background-color: #d4edda'] * len(row)
    else:
        return ['background-color: #f8d7da'] * len(row)

# Calculate cluster ROI sum for a player - FIXED SYNTAX
def calculate_cluster_roi_sum(roi_df, rally_cluster, net_cluster, serve_cluster):
    if roi_df.empty:
        return "N/A"

    total = 0
    count = 0

    if not pd.isna(rally_cluster):
        col_name = f'roi_vs_rally{rally_cluster}_match'
        if col_name in roi_df.columns and not pd.isna(roi_df[col_name].iloc[0]):
            total += float(roi_df[col_name].iloc[0])
            count += 1

    if not pd.isna(net_cluster):
        col_name = f'roi_vs_net{net_cluster}_match'
        if col_name in roi_df.columns and not pd.isna(roi_df[col_name].iloc[0]):
            total += float(roi_df[col_name].iloc[0])
            count += 1

    if not pd.isna(serve_cluster):
        col_name = f'roi_vs_serve{serve_cluster}_match'
        if col_name in roi_df.columns and not pd.isna(roi_df[col_name].iloc[0]):
            total += float(roi_df[col_name].iloc[0])
            count += 1

    if count == 0:
        return "N/A"

    avg = total / count
    return f"{avg:.1f}%"

# Main Streamlit app
def main():
    st.set_page_config(layout="wide", page_title="Tennis Betting Assistant", page_icon="🎾")

    # Initialize BigQuery client
    client = init_bigquery_client()

    # Sidebar filters
    st.sidebar.title("🎾 Tennis Betting Assistant")
    st.sidebar.subheader("Match Filters")

    tours = st.sidebar.multiselect("Select Tours", ["ATP", "WTA"], default=["ATP", "WTA"])

    # Get unique tournament tiers dynamically
    with st.spinner("Fetching tournament tiers..."):
        unique_tiers = get_unique_tiers()

    # Default to including most common tiers
    default_tiers = [t for t in unique_tiers if t in ["Grand Slam", "ATP", "WTA", "Challenger", "Future"]]
    if not default_tiers:
        default_tiers = unique_tiers[:3] if unique_tiers else []

    tiers = st.sidebar.multiselect("Tournament Tier",
                                  unique_tiers,
                                  default=default_tiers)

    # Rally cluster filter
    rally_cluster_filter = st.sidebar.checkbox("Only matches with rally cluster data")

    # Date filter with new options
    date_filter = st.sidebar.radio("Date Range", ["Today", "Tomorrow", "2+ Days"])

    # Load matches data
    if tiers:
        with st.spinner("Loading matches..."):
            all_matches = get_matches_data(client, tours, tiers)
    else:
        st.warning("Please select at least one tournament tier")
        st.stop()

    # Apply rally cluster filter
    if rally_cluster_filter:
        all_matches = all_matches.dropna(subset=['p1_rally_cluster', 'p2_rally_cluster'])

    # Apply date filter
    if not all_matches.empty:
        today = datetime.now().date()
        if date_filter == "Today":
            all_matches = all_matches[all_matches['match_start_at'].dt.date == today]
        elif date_filter == "Tomorrow":
            tomorrow = today + timedelta(days=1)
            all_matches = all_matches[all_matches['match_start_at'].dt.date == tomorrow]
        elif date_filter == "2+ Days":
            day_after_tomorrow = today + timedelta(days=2)
            all_matches = all_matches[all_matches['match_start_at'].dt.date >= day_after_tomorrow]

    # Main page
    st.title("🎾 Tennis Betting Assistant")

    if not all_matches.empty:
        # Add match ID for selection
        all_matches["match_id"] = all_matches.apply(
            lambda row: f"{row['tour']}|{row['p1_name']}|{row['p2_name']}|{row['match_start_at']}",
            axis=1
        )

        # Create formatted display columns - ordered by ascending match_start_at
        display_df = all_matches.copy()
        display_df = display_df.sort_values('match_start_at')  # Order by ascending match time
        display_df['Date'] = display_df['match_start_at'].dt.strftime('%Y-%m-%d %H:%M')
        display_df['Diff'] = display_df['diff'].apply(format_diff)
        display_df['P1 Pinnacle'] = display_df['p1_pinnacle_odds'].apply(format_odds)
        display_df['P1 Model'] = display_df['p1_model_odds'].apply(format_odds)
        display_df['P2 Pinnacle'] = display_df['p2_pinnacle_odds'].apply(format_odds)
        display_df['P2 Model'] = display_df['p2_model_odds'].apply(format_odds)

        # Create selection state
        if 'selected_match' not in st.session_state:
            st.session_state.selected_match = None

        # Display matches in a table with clickable rows
        st.subheader(f"Upcoming Matches ({len(all_matches)} found)")
        st.write("Click on a match to view detailed analysis")

        # Create a container for the table
        table_container = st.container()

        # Pre-cache ROI data for all players to avoid repeated queries
        all_players = set(display_df['p1_name'].tolist() + display_df['p2_name'].tolist())
        roi_cache = {}
        with st.spinner("Loading player ROI data..."):
            for player in all_players:
                # Determine tour based on which dataset the player appears in
                tour = "ATP" if player in display_df[display_df['tour'] == "ATP"]['p1_name'].values or \
                               player in display_df[display_df['tour'] == "ATP"]['p2_name'].values else "WTA"
                roi_cache[player] = get_player_roi(client, player, tour)

        # Display the table with clickable rows
        with table_container:
            # Create a form for each row
            for i, row in display_df.iterrows():
                # Calculate cluster ROI sums using cached data
                p1_roi = roi_cache.get(row['p1_name'], pd.DataFrame())
                p2_roi = roi_cache.get(row['p2_name'], pd.DataFrame())

                p1_cluster_roi = calculate_cluster_roi_sum(p1_roi, row['p2_rally_cluster'], row['p2_net_cluster'], row['p2_serve_cluster'])
                p2_cluster_roi = calculate_cluster_roi_sum(p2_roi, row['p1_rally_cluster'], row['p1_net_cluster'], row['p1_serve_cluster'])

                # Create columns for the match info - 15 columns total
                cols = st.columns([0.7, 1.5, 1.0, 0.8, 1.5, 0.5, 1.5, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8])

                # Display match info in columns
                cols[0].write(f"**{i+1}**")
                cols[1].write(f"**{row['Date']}**")
                cols[2].write(f"**{row['tournament_tier']}**")
                cols[3].write(f"**{row['tournament_round']}**")
                cols[4].write(f"**{row['p1_name']}**")
                cols[5].write(f"**vs**")
                cols[6].write(f"**{row['p2_name']}**")
                cols[7].write(f"{row['surface']}")
                cols[8].write(row['Diff'])
                cols[9].write(f"P: {row['P1 Pinnacle']}")
                cols[10].write(f"M: {row['P1 Model']}")
                cols[11].write(f"P: {row['P2 Pinnacle']}")
                cols[12].write(f"M: {row['P2 Model']}")
                cols[13].write(f"{p1_cluster_roi}")
                cols[14].write(f"{p2_cluster_roi}")

                # Add a button to select the match
                if cols[0].button("Select", key=f"select_{i}"):
                    st.session_state.selected_match = row["match_id"]
                    st.rerun()

                # Add divider
                st.divider()

        # Check if a match is selected
        if st.session_state.selected_match:
            # Parse selected match
            tour, p1_name, p2_name, match_time = st.session_state.selected_match.split("|")
            match_time = datetime.strptime(match_time, "%Y-%m-%d %H:%M:%S")

            # Get match details
            match_details = all_matches[all_matches["match_id"] == st.session_state.selected_match].iloc[0]
            surface = match_details["surface"]
            p1_odds = match_details["p1_pinnacle_odds"]
            p2_odds = match_details["p2_pinnacle_odds"]
            p1_left = match_details["p1_is_left_handed"]
            p2_left = match_details["p2_is_left_handed"]
            p1_rally = match_details["p1_rally_cluster"]
            p1_net = match_details["p1_net_cluster"]
            p1_serve = match_details["p1_serve_cluster"]
            p2_rally = match_details["p2_rally_cluster"]
            p2_net = match_details["p2_net_cluster"]
            p2_serve = match_details["p2_serve_cluster"]
            is_grand_slam = match_details["tournament_tier"] == "Grand Slam"
            tournament_round = match_details["tournament_round"]

            # Display detailed match analysis
            st.divider()
            st.subheader(f"{p1_name} vs {p2_name} - {surface}")
            st.caption(f"{match_time.strftime('%Y-%m-%d %H:%M')} | {tour} | {match_details['tournament_tier']} | {tournament_round}")

            # Add button to go back to match list
            if st.button("← Back to Match List"):
                st.session_state.selected_match = None
                st.rerun()

            # Get ROI data for both players
            with st.spinner(f"Loading data for {p1_name}..."):
                p1_roi = get_player_roi(client, p1_name, tour)
            with st.spinner(f"Loading data for {p2_name}..."):
                p2_roi = get_player_roi(client, p2_name, tour)

            # Head-to-head section
            st.subheader("Head-to-Head History")
            h2h = get_head_to_head(client, p1_name, p2_name, tour)

            if not h2h.empty:
                # Calculate win percentages
                p1_wins = h2h[h2h['winner_name'] == p1_name].shape[0]
                p2_wins = h2h[h2h['winner_name'] == p2_name].shape[0]
                total_matches = len(h2h)

                # Display win stats
                col1, col2, col3 = st.columns(3)
                col1.metric(f"{p1_name} Wins", p1_wins, f"{p1_wins/total_matches*100:.1f}%")
                col2.metric(f"{p2_name} Wins", p2_wins, f"{p2_wins/total_matches*100:.1f}%")
                col3.metric("Total Matches", total_matches)

                # Display match history
                st.dataframe(h2h)
            else:
                st.info("No previous matches found between these players.")

            # Player ROI analysis section
            st.subheader("Player ROI Analysis")

            # Create columns for player comparison
            col1, col2 = st.columns(2)

            # Player 1 analysis
            with col1:
                st.markdown(f"### {p1_name}")

                if not p1_roi.empty:
                    # Always show overall ROI
                    st.markdown(f"**Overall Match ROI:** {format_roi(p1_roi['overall_match_win_roi'].iloc[0])}")

                    # Show surface-specific ROI
                    surface_roi_col = get_surface_roi_column(surface)
                    st.markdown(f"**{surface} ROI:** {format_roi(p1_roi[surface_roi_col].iloc[0])}")

                    # Show Grand Slam ROI if applicable
                    if is_grand_slam and 'grand_slam_match_win_roi' in p1_roi.columns:
                        st.markdown(f"**Grand Slam ROI:** {format_roi(p1_roi['grand_slam_match_win_roi'].iloc[0])}")

                    # Show ROI against left-handed opponents if applicable
                    if p2_left and 'vs_left_handed_match_roi' in p1_roi.columns:
                        st.markdown(f"**vs Left-Handed ROI:** {format_roi(p1_roi['vs_left_handed_match_roi'].iloc[0])}")

                    # Show cluster-specific ROIs if available
                    st.markdown("**Cluster ROIs vs Opponent:**")
                    cluster_types = ['rally', 'net', 'serve']
                    cluster_values = [p2_rally, p2_net, p2_serve]

                    for c_type, c_value in zip(cluster_types, cluster_values):
                        # Skip if cluster value is missing
                        if pd.isna(c_value):
                            continue

                        col_name = f'roi_vs_{c_type}{c_value}_match'
                        if col_name in p1_roi.columns:
                            roi_value = p1_roi[col_name].iloc[0]
                            if not pd.isna(roi_value):
                                st.markdown(f"- {c_type.capitalize()} Cluster {c_value}: {format_roi(roi_value)}")

                else:
                    st.warning("No ROI data available for this player")

                # Automatically show player history
                st.subheader(f"{p1_name}'s Match History (2024)")
                history = get_player_history(client, p1_name, tour).head(25)

                if not history.empty:
                    # Display win rate stats
                    wins = history[history['win'] == 1].shape[0]
                    total = history.shape[0]
                    win_rate = wins / total * 100 if total > 0 else 0

                    col1, col2 = st.columns(2)
                    col1.metric("Total Matches", total)
                    col2.metric("Win Rate", f"{win_rate:.1f}%")

                    # Display match history with color coding
                    history_display = history.copy()
                    history_display['Date'] = history_display['match_date'].dt.strftime('%Y-%m-%d')
                    history_display = history_display[['Date', 'tournament_name', 'surface', 'opponent', 'win', 'odds', 'score']]
                    history_display.columns = ['Date', 'Tournament', 'Surface', 'Opponent', 'Win', 'Odds', 'Score']

                    # Apply color coding
                    st.dataframe(history_display.style.apply(format_history_row, axis=1))
                else:
                    st.info("No match history found for this player")

            # Player 2 analysis
            with col2:
                st.markdown(f"### {p2_name}")

                if not p2_roi.empty:
                    # Always show overall ROI
                    st.markdown(f"**Overall Match ROI:** {format_roi(p2_roi['overall_match_win_roi'].iloc[0])}")

                    # Show surface-specific ROI
                    surface_roi_col = get_surface_roi_column(surface)
                    st.markdown(f"**{surface} ROI:** {format_roi(p2_roi[surface_roi_col].iloc[0])}")

                    # Show Grand Slam ROI if applicable
                    if is_grand_slam and 'grand_slam_match_win_roi' in p2_roi.columns:
                        st.markdown(f"**Grand Slam ROI:** {format_roi(p2_roi['grand_slam_match_win_roi'].iloc[0])}")

                    # Show ROI against left-handed opponents if applicable
                    if p1_left and 'vs_left_handed_match_roi' in p2_roi.columns:
                        st.markdown(f"**vs Left-Handed ROI:** {format_roi(p2_roi['vs_left_handed_match_roi'].iloc[0])}")

                    # Show cluster-specific ROIs if available
                    st.markdown("**Cluster ROIs vs Opponent:**")
                    cluster_types = ['rally', 'net', 'serve']
                    cluster_values = [p1_rally, p1_net, p1_serve]

                    for c_type, c_value in zip(cluster_types, cluster_values):
                        # Skip if cluster value is missing
                        if pd.isna(c_value):
                            continue

                        col_name = f'roi_vs_{c_type}{c_value}_match'
                        if col_name in p2_roi.columns:
                            roi_value = p2_roi[col_name].iloc[0]
                            if not pd.isna(roi_value):
                                st.markdown(f"- {c_type.capitalize()} Cluster {c_value}: {format_roi(roi_value)}")

                else:
                    st.warning("No ROI data available for this player")

                # Automatically show player history
                st.subheader(f"{p2_name}'s Match History (2024)")
                history = get_player_history(client, p2_name, tour).head(25)

                if not history.empty:
                    # Display win rate stats
                    wins = history[history['win'] == 1].shape[0]
                    total = history.shape[0]
                    win_rate = wins / total * 100 if total > 0 else 0

                    col1, col2 = st.columns(2)
                    col1.metric("Total Matches", total)
                    col2.metric("Win Rate", f"{win_rate:.1f}%")

                    # Display match history with color coding
                    history_display = history.copy()
                    history_display['Date'] = history_display['match_date'].dt.strftime('%Y-%m-%d')
                    history_display = history_display[['Date', 'tournament_name', 'surface', 'opponent', 'win', 'odds', 'score']]
                    history_display.columns = ['Date', 'Tournament', 'Surface', 'Opponent', 'Win', 'Odds', 'Score']

                    # Apply color coding
                    st.dataframe(history_display.style.apply(format_history_row, axis=1))
                else:
                    st.info("No match history found for this player")

            # Value analysis section
            st.subheader("Value Analysis")
            col1, col2 = st.columns(2)

            with col1:
                if not p1_roi.empty:
                    model_prob = 1 / p1_roi['p1_model_odds'].iloc[0] if 'p1_model_odds' in p1_roi else None
                    implied_prob = 1 / p1_odds if p1_odds else None

                    if model_prob and implied_prob:
                        value = model_prob - implied_prob
                        st.metric(f"{p1_name} Value",
                                f"{value*100:.1f}%",
                                delta="Positive Value" if value > 0 else "Negative Value",
                                delta_color="normal")
                    else:
                        st.warning("Odds data not available for value calculation")

            with col2:
                if not p2_roi.empty:
                    model_prob = 1 / p2_roi['p2_model_odds'].iloc[0] if 'p2_model_odds' in p2_roi else None
                    implied_prob = 1 / p2_odds if p2_odds else None

                    if model_prob and implied_prob:
                        value = model_prob - implied_prob
                        st.metric(f"{p2_name} Value",
                                f"{value*100:.1f}%",
                                delta="Positive Value" if value > 0 else "Negative Value",
                                delta_color="normal")
                    else:
                        st.warning("Odds data not available for value calculation")

            # Bet recommendation
            st.subheader("Betting Recommendation")
            if not p1_roi.empty and not p2_roi.empty:
                # Calculate value for both players
                p1_value = (1 / p1_roi['p1_model_odds'].iloc[0] - 1/p1_odds) * 100
                p2_value = (1 / p2_roi['p2_model_odds'].iloc[0] - 1/p2_odds) * 100

                if p1_value > 0 and p1_value > p2_value:
                    st.success(f"✅ Recommended Bet: **{p1_name}** (Value: {p1_value:.1f}%)")
                    st.markdown(f"**Rationale:** {p1_name} shows positive value based on model vs. market odds")
                elif p2_value > 0 and p2_value > p1_value:
                    st.success(f"✅ Recommended Bet: **{p2_name}** (Value: {p2_value:.1f}%)")
                    st.markdown(f"**Rationale:** {p2_name} shows positive value based on model vs. market odds")
                else:
                    st.warning("⚠️ No Clear Value Bet - Both players show negative or neutral value")
            else:
                st.info("Insufficient data for betting recommendation")

    else:
        st.warning("No matches found with the selected filters")

if __name__ == "__main__":
    main()