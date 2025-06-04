from bs4 import BeautifulSoup, Tag
import pandas as pd
import requests
import shutil
import time
from datetime import datetime
import os
import re

# Configuration
BASE_URL = "https://www.flashscore.mobi"
CSV_FILE = "data_collection/flashscore/flashscore_atp_stats.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}
TOURNAMENT_PREFIXES = ['ATP - SINGLES']
REQUEST_DELAY = 2

def get_player_profile(url):
    """Extract player details from profile page"""
    try:
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.content, "html.parser")
        full_name = soup.find('h1', class_='player-header__name').text.strip()
        return {'full_name': full_name}
    except Exception as e:
        print(f"Error getting player profile: {str(e)}")
        return None

def verify_stats_page(url):
    """Direct content verification with retries"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return False
        return 'stat__row' in response.text
    except Exception as e:
        print(f"Verification failed for {url}: {str(e)}")
        return False
    
def get_match_stats(match_url):
    """Scrape match statistics from the provided HTML structure"""
    try:
        print(f"\nProcessing: {match_url}")
        response = requests.get(match_url, headers=HEADERS)
        soup = BeautifulSoup(response.content, "html.parser")

        # Extract timestamp from the detail div
        timestamp_div = soup.find('div', class_='detail', string=re.compile(r'\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}'))
        if not timestamp_div:
            print(" - Timestamp not found")
            return None
            
        try:
            date_str = timestamp_div.text.strip()
            match_dt = datetime.strptime(date_str, "%d.%m.%Y %H:%M")
            match_timestamp = int(match_dt.timestamp())
        except Exception as e:
            print(f" - Error parsing timestamp: {str(e)}")
            return None

        # Extract player names from h3 links
        try:
            player_links = soup.select('h3 a.web-link-external')
            if len(player_links) < 2:
                print(" - Player links not found")
                return None
                
            players = {
                'p1': player_links[0].text.strip(),
                'p2': player_links[1].text.strip()
            }
        except Exception as e:
            print(f" - Error getting player names: {str(e)}")
            return None

        # Initialize statistics dictionary
        stats_data = {
            'match_id': re.search(r'match/([^/]+)/', match_url).group(1),
            'match_timestamp': match_timestamp,
            'p1_name': players['p1'],
            'p2_name': players['p2'],
            # Initialize all stats fields with empty values
            'p1_avg_1st_sv_speed': '',
            'p2_avg_1st_sv_speed': '',
            'p1_avg_2nd_sv_speed': '',
            'p2_avg_2nd_sv_speed': '',
            'p1_winners': '',
            'p2_winners': '',
            'p1_unforced_errors': '',
            'p2_unforced_errors': '',
            'p1_net_points_won': '',
            'p2_net_points_won': '',
            'p1_net_points_played': '',
            'p2_net_points_played': ''
        }

        # Extract statistics from the wcl rows
        for row in soup.select('div.wcl-row_OFViZ'):
            try:
                category_elem = row.select_one('div.wcl-category_7qsgP strong')
                home_value = row.select_one('div.wcl-homeValue_-iJBW strong').text.strip()
                away_value = row.select_one('div.wcl-awayValue_rQvxs strong').text.strip()
                
                if not category_elem:
                    continue
                    
                category = category_elem.text.strip()
                
                # Map categories to our data structure
                if "Average 1st Serve Speed" in category:
                    stats_data['p1_avg_1st_sv_speed'] = home_value.replace(' km/h', '')
                    stats_data['p2_avg_1st_sv_speed'] = away_value.replace(' km/h', '')
                elif "Average 2nd Serve Speed" in category:
                    stats_data['p1_avg_2nd_sv_speed'] = home_value.replace(' km/h', '')
                    stats_data['p2_avg_2nd_sv_speed'] = away_value.replace(' km/h', '')
                elif "Winners" in category:
                    stats_data['p1_winners'] = home_value
                    stats_data['p2_winners'] = away_value
                elif "Unforced Errors" in category:
                    stats_data['p1_unforced_errors'] = home_value
                    stats_data['p2_unforced_errors'] = away_value
                elif "Net Points Won" in category:
                    # Handle values like "67% (2/3)"
                    try:
                        # Extract numbers from parentheses using regex
                        home_net = re.search(r'\((\d+)/(\d+)\)', home_value)
                        away_net = re.search(r'\((\d+)/(\d+)\)', away_value)
                        
                        if home_net and away_net:
                            stats_data['p1_net_points_won'] = home_net.group(1)
                            stats_data['p1_net_points_played'] = home_net.group(2)
                            stats_data['p2_net_points_won'] = away_net.group(1)
                            stats_data['p2_net_points_played'] = away_net.group(2)
                        else:
                            print(f" - Invalid net points format: {home_value} | {away_value}")
                    except Exception as e:
                        print(f" - Net points processing error: {str(e)}")
                    
            except Exception as e:
                print(f" - Error processing row: {str(e)}")
                continue

        return stats_data

    except Exception as e:
        print(f" - Critical error: {str(e)}")
        return None

def get_daily_matches(days_ago=0):
    """Robust match discovery with direct HTML pattern matching"""
    params = {'d': f'-{days_ago}'} if days_ago > 0 else {}
    print(f"\n=== Checking day {days_ago} days ago ===")
    
    try:
        response = requests.get(
            f"{BASE_URL}/tennis/",
            params=params,
            headers=HEADERS
        )
        response.raise_for_status()
    except Exception as e:
        print(f"Request failed: {str(e)}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    
    # # Save raw HTML for debugging
    # with open(f"debug_{days_ago}.html", "w", encoding="utf-8") as f:
    #     f.write(response.text)
    
    matches = []
    
    # Find all tournament headers
    tournaments = soup.find_all(['h3', 'h4'], string=lambda text: any(p in text for p in TOURNAMENT_PREFIXES))
    print(f"Found {len(tournaments)} ATP tournaments")
    
    for tournament in tournaments:
        print(f"\nProcessing: {tournament.text.strip()}")
        
        # Find all match links in this tournament section
        tournament_section = []
        current = tournament.next_sibling
        
        while current and current.name not in ['h3', 'h4']:
            tournament_section.append(current)
            current = current.next_sibling
        
        # Extract match links from section
        for element in tournament_section:
            if isinstance(element, Tag) and element.name == 'a' and 'fin' in element.get('class', []):
                match_path = element['href']
                base_url = f"{BASE_URL}{match_path}"
                matches.append(f"{base_url}&t=match-statistics")
                print(f" - Found match: {base_url}")
    
    print(f"\nTotal matches found: {len(matches)}")
    return matches

def update_stats_dataset(days_ago=0):
    """Main function to update dataset with backup capability"""
    print(f"\n{'='*50}")
    print(f"Processing day {days_ago} days ago")
    
    # Create backup before making any changes
    if os.path.exists(CSV_FILE):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{CSV_FILE.split('.')[0]}_{timestamp}.csv"
        shutil.copy2(CSV_FILE, backup_file)
        print(f"Created backup: {backup_file}")
        
        # Keep only the 2 most recent backups
        backup_files = sorted([f for f in os.listdir(os.path.dirname(CSV_FILE)) 
                             if f.startswith(os.path.basename(CSV_FILE).split('.')[0]) 
                             and f.endswith('.csv') 
                             and f != os.path.basename(CSV_FILE)])
        for old_backup in backup_files[:-2]:  # Keep last 2 backups
            os.remove(os.path.join(os.path.dirname(CSV_FILE), old_backup))
            print(f"Removed old backup: {old_backup}")

    # Load existing data
    if os.path.exists(CSV_FILE):
        existing_df = pd.read_csv(CSV_FILE)
        print(f"Existing matches: {len(existing_df)}")
        
        # Add timestamp column if it doesn't exist (for backward compatibility)
        if 'data_added_timestamp' not in existing_df.columns:
            existing_df['data_added_timestamp'] = datetime.now().timestamp()
            print("Added timestamp column to existing data")
    else:
        existing_df = pd.DataFrame()
        print("No existing data found")

    # Get daily matches
    match_urls = get_daily_matches(days_ago)
    updated_count = 0
    new_data = []
    
    for url in match_urls:
        match_id = re.search(r'match/([^/]+)/', url).group(1)
        print(f"\nProcessing match: {match_id}")
        
        # Always fetch fresh data
        match_data = get_match_stats(url)
        if match_data:
            # Add current timestamp to the new data
            match_data['data_added_timestamp'] = datetime.now().timestamp()
            new_data.append(match_data)
            updated_count += 1
        else:
            print(f"Failed to collect data for match {match_id}")
        time.sleep(REQUEST_DELAY)

    # Combine old and new data
    if new_data:
        new_df = pd.DataFrame(new_data)
        
        # If we have existing data, combine and handle duplicates
        if not existing_df.empty:
            # Combine old and new data
            combined_df = pd.concat([existing_df, new_df])
            
            # Sort by timestamp (newest first) and remove duplicates, keeping first (newest)
            combined_df = combined_df.sort_values('data_added_timestamp', ascending=False)
            combined_df = combined_df.drop_duplicates(subset=['match_id'], keep='first')
            
            print(f"\nRemoved {len(existing_df) + len(new_df) - len(combined_df)} duplicate matches")
        else:
            combined_df = new_df
        
        # Ensure column order (put timestamp at the end)
        columns = [
            'match_id', 'match_timestamp', 'p1_name', 'p2_name',
            'p1_avg_1st_sv_speed', 'p2_avg_1st_sv_speed',
            'p1_avg_2nd_sv_speed', 'p2_avg_2nd_sv_speed',
            'p1_winners', 'p2_winners',
            'p1_unforced_errors', 'p2_unforced_errors',
            'p1_net_points_won', 'p2_net_points_won',
            'p1_net_points_played', 'p2_net_points_played',
            'data_added_timestamp'
        ]
        combined_df = combined_df[columns]
        combined_df.to_csv(CSV_FILE, index=False)
        
        print(f"\nSaved {len(combined_df)} matches to {CSV_FILE}")
        print(f"Added {updated_count} new matches")
    else:
        print("\nNo new matches found to add")



# Example usage
if __name__ == "__main__":
    # To scrape yesterday's matches
    update_stats_dataset(days_ago=7)
    
    # To scrape multiple days
    # for days in range(1,6):
    #     update_stats_dataset(days_ago=days)
    #     time.sleep(10)