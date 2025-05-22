import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from dotenv import load_dotenv
from datetime import datetime
import random
import re
import logging
import difflib
import httpx  # For MinimalSupabaseClient
from typing import List, Dict, Any  # For MinimalSupabaseClient

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "aae0279e8feccdcfb5b40c85fdd65a66")

# --- Definition of MinimalSupabaseClient ---
class MinimalSupabaseClient:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase_url = supabase_url.rstrip('/')
        self.supabase_key = supabase_key
        self.base_headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(headers=self.base_headers, timeout=30.0)
        self.current_table = None

    def table(self, table_name: str):
        self.current_table = table_name
        return self

    def select(self, columns: str = "*", params: dict = None):
        if not self.current_table:
            logging.error("Table not set for select operation.")
            return {"data": [], "error": "Table not set", "count": None}
        
        url = f"{self.supabase_url}/rest/v1/{self.current_table}?select={columns}"
        
        try:
            response = self.client.get(url, params=params if params else {})
            response.raise_for_status()
            count = None
            content_range = response.headers.get("content-range")
            if content_range and "/" in content_range:
                count_str = content_range.split("/")[-1]
                if count_str != "*":
                    count = int(count_str)
            return {"data": response.json(), "error": None, "count": count}
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error during select on {self.current_table}: {e.response.text if e.response else str(e)}")
            return {"data": [], "error": str(e), "count": None}
        except Exception as e:
            logging.error(f"Generic error during select on {self.current_table}: {e}")
            return {"data": [], "error": str(e), "count": None}

    def insert(self, data: list, upsert: bool = False):
        if not self.current_table:
            logging.error("Table not set for insert operation.")
            return {"data": [], "error": "Table not set"}

        url = f"{self.supabase_url}/rest/v1/{self.current_table}"
        
        custom_headers = self.base_headers.copy()
        custom_headers["Prefer"] = "return=representation"
        if upsert:
            custom_headers["Prefer"] += ",resolution=merge-duplicates"
            logging.info(f"Performing upsert on {self.current_table} with Prefer: {custom_headers['Prefer']}")

        try:
            response = self.client.post(url, json=data, headers=custom_headers)
            response.raise_for_status()
            return {"data": response.json(), "error": None}
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error during insert on {self.current_table}: {e.response.text if e.response else str(e)}")
            return {"data": [], "error": str(e)}
        except Exception as e:
            logging.error(f"Generic error during insert on {self.current_table}: {e}")
            return {"data": [], "error": str(e)}

    def delete_rows(self, params: dict = None):
        if not self.current_table:
            logging.error("Table not set for delete operation.")
            return {"data": [], "error": "Table not set"}

        url = f"{self.supabase_url}/rest/v1/{self.current_table}"
        
        custom_headers = self.base_headers.copy()
        custom_headers["Prefer"] = "return=representation"

        try:
            response = self.client.delete(url, params=params if params else {}, headers=custom_headers)
            response.raise_for_status()
            return {"data": response.json() if response.status_code != 204 else [], "error": None}
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error during delete on {self.current_table}: {e.response.text if e.response else str(e)}")
            return {"data": [], "error": str(e)}
        except Exception as e:
            logging.error(f"Generic error during delete on {self.current_table}: {e}")
            return {"data": [], "error": str(e)}

# Initialize Supabase client
supabase = MinimalSupabaseClient(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY)

# ScraperAPI configuration
SCRAPERAPI_ENDPOINT = "http://api.scraperapi.com"

def get_scraperapi_response(url, retries=5):
    """
    Fetch a URL using ScraperAPI with retry logic
    """
    params = {
        'api_key': SCRAPERAPI_KEY,
        'url': url,
        'render': 'true',  # Enable JavaScript rendering
        'country_code': 'fr',  # Use French proxy
        'device_type': 'desktop',
        'premium': 'true',  # Use premium proxies for better success rate
        'session_number': random.randint(1, 1000),  # Random session for variety
        'keep_headers': 'true',  # Keep original headers
        'autoparse': 'false',  # Disable autoparse for better control
        'format': 'html',  # Ensure HTML format
        'wait': '5000',  # Wait 5 seconds for page to load
        'scroll': 'true',  # Enable scrolling to load dynamic content
        'screenshot': 'false'  # Disable screenshot to save bandwidth
    }
    
    # Add custom headers to make request look more legitimate
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(retries):
        try:
            logging.info(f"Fetching {url} via ScraperAPI (attempt {attempt + 1}/{retries})")
            
            # Randomize session number for each attempt
            params['session_number'] = random.randint(1, 1000)
            
            response = requests.get(SCRAPERAPI_ENDPOINT, params=params, headers=headers, timeout=90)
            
            if response.status_code == 200:
                content = response.text
                # Check if we got a 403 error page
                if "Error 403" in content or "Forbidden" in content:
                    logging.warning(f"Received 403 Forbidden page from Betclic (attempt {attempt + 1})")
                    if attempt < retries - 1:
                        # Try with different country codes
                        if attempt == 1:
                            params['country_code'] = 'be'  # Try Belgium
                            logging.info("Switching to Belgium proxy")
                        elif attempt == 2:
                            params['country_code'] = 'ch'  # Try Switzerland
                            logging.info("Switching to Switzerland proxy")
                        time.sleep(10)  # Wait longer before retry
                        continue
                else:
                    logging.info(f"Successfully fetched {url}")
                    return content
            else:
                logging.warning(f"ScraperAPI returned status code {response.status_code} for {url}")
                if attempt < retries - 1:
                    time.sleep(10)  # Wait longer before retry
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(10)  # Wait longer before retry
    
    # If all attempts failed, try one last time with ultra-premium settings
    logging.info("Trying final attempt with ultra-premium ScraperAPI settings...")
    try:
        ultra_params = {
            'api_key': SCRAPERAPI_KEY,
            'url': url,
            'render': 'true',
            'country_code': 'fr',
            'device_type': 'desktop',
            'premium': 'true',
            'ultra_premium': 'true',  # Use ultra premium if available
            'session_number': random.randint(1, 10000),
            'keep_headers': 'true',
            'wait': '10000',  # Wait 10 seconds
            'scroll': 'true',
            'residential': 'true'  # Use residential proxies if available
        }
        
        response = requests.get(SCRAPERAPI_ENDPOINT, params=ultra_params, headers=headers, timeout=120)
        
        if response.status_code == 200:
            content = response.text
            if "Error 403" not in content and "Forbidden" not in content:
                logging.info("Ultra-premium attempt successful!")
                return content
            else:
                logging.warning("Ultra-premium attempt also received 403")
        
    except Exception as e:
        logging.error(f"Ultra-premium attempt failed: {e}")
    
    logging.error(f"Failed to fetch {url} after {retries + 1} attempts (including ultra-premium)")
    return None

def scrape_betclic_matches():
    """
    Scrape Betclic tennis matches using ScraperAPI
    """
    url = "https://www.betclic.fr/tennis-stennis"
    
    # Get the page content via ScraperAPI
    page_content = get_scraperapi_response(url)
    
    if not page_content:
        logging.error("Failed to get page content from ScraperAPI")
        return []
    
    # Save page content for debugging
    with open("page_debug.html", "w", encoding="utf-8") as f:
        f.write(page_content)
    logging.info("HTML saved to page_debug.html")
    
    soup = BeautifulSoup(page_content, "html.parser")
    
    matches = []
    seen_urls = set()
    scraped_dt = datetime.now()
    
    # Find match cards
    match_cards = soup.find_all("sports-events-event-card")
    logging.info(f"Found {len(match_cards)} match cards")
    
    for card_index, card in enumerate(match_cards):
        try:
            current_date = ""
            current_heure = ""
            current_tournoi = ""
            current_tour = ""

            # Extract player names
            players = card.find_all("div", class_="scoreboard_contestantLabel")
            player1 = players[0].text.strip() if len(players) > 0 else ""
            player2 = players[1].text.strip() if len(players) > 1 else ""

            # Extract match URL
            a_tag = card.find("a", class_="cardEvent")
            match_url = ""
            if a_tag and "href" in a_tag.attrs:
                match_url = "https://www.betclic.fr" + a_tag["href"]

            if not match_url or match_url in seen_urls:
                logging.debug(f"Match {player1} vs {player2} ignored (empty URL or already seen: {match_url})")
                continue
            seen_urls.add(match_url)

            # Extract full player names from URL slug
            player1_full, player2_full = player1, player2
            if a_tag and "href" in a_tag.attrs:
                match_obj = re.search(r'/([a-z0-9\-]+)-m\d+$', a_tag["href"])
                if match_obj:
                    full_slug = match_obj.group(1)
                    parts = full_slug.split('-')
                    # Logic to divide player names from slug
                    n_parts = len(parts)
                    split_point = n_parts // 2
                    slug1 = '-'.join(parts[:split_point])
                    slug2 = '-'.join(parts[split_point:])

                    def slug_to_name(slug):
                        return ' '.join([x.capitalize() for x in slug.replace('-', ' ').split()])

                    player1_full = slug_to_name(slug1)
                    player2_full = slug_to_name(slug2)

            # Extract date and time
            event_info_time = card.find("div", class_="event_infoTime")
            if event_info_time and event_info_time.text.strip():
                date_heure_text = event_info_time.text.strip()
                # Handle "Auj.", "Dem." and full dates
                if "Auj." in date_heure_text or "Dem." in date_heure_text:
                    parts = date_heure_text.split()
                    if len(parts) >= 2:
                        current_date = parts[0]  # Can be "Auj." or "Dem."
                        current_heure = parts[-1]  # Time is always the last part
                else:  # e.g., "Jeu. 01/01 15:00"
                    parts = date_heure_text.split()
                    if len(parts) == 3:  # "Jeu. 01/01 15:00"
                        current_date = f"{parts[0]} {parts[1]}"  # "Jeu. 01/01"
                        current_heure = parts[2]  # "15:00"
                    elif len(parts) == 2:  # Case "01/01 15:00"
                        current_date = parts[0]
                        current_heure = parts[1]

            # Extract tournament name from URL
            if a_tag and "href" in a_tag.attrs:
                url_parts = a_tag["href"].split("/")
                if len(url_parts) > 2:
                    tournoi_slug_full = url_parts[2]
                    # Take everything before the first "-c" followed by digits
                    tournoi_match = re.match(r"^(.*?)(-c\d+)?$", tournoi_slug_full)
                    if tournoi_match:
                        tournoi_slug = tournoi_match.group(1)
                        current_tournoi = tournoi_slug.replace('-', ' ').title()
                    else:
                        current_tournoi = tournoi_slug_full.replace('-', ' ').title()

            logging.info(
                f"Match {card_index + 1}/{len(match_cards)}: {player1_full} vs {player2_full} | Date: {current_date}, Heure: {current_heure} | Tournoi: {current_tournoi} | URL: {match_url}")

            matches.append({
                "date": current_date,
                "heure": current_heure,
                "tournoi": current_tournoi,
                "tour": current_tour,
                "player1": player1_full,
                "player2": player2_full,
                "match_url": match_url,
                "scraped_date": scraped_dt.date().isoformat(),
                "scraped_time": scraped_dt.time().strftime("%H:%M:%S"),
            })

        except Exception as e:
            logging.error(f"Error processing match card {card_index}: {e}")
            continue

    logging.info(f"Total matches extracted: {len(matches)}")
    return matches

def normalize_name(name):
    """Normalize player name for comparison"""
    return ' '.join(str(name).replace('\xa0', ' ').split()).lower()

def player_to_tennisabstract_url(player_name_on_site):
    """Convert player name to Tennis Abstract URL format"""
    normalized_for_url = str(player_name_on_site).lower()
    # Remove non-alphanumeric characters except spaces
    normalized_for_url = re.sub(r'[^a-z0-9\s-]', '', normalized_for_url)
    # Replace spaces and hyphens with nothing
    normalized_for_url = normalized_for_url.replace(' ', '').replace('-', '')
    return f"https://www.tennisabstract.com/cgi-bin/player.cgi?p={normalized_for_url}"

def find_best_slug_url(name, elo_df_local):
    """Find the best matching player URL from ELO data"""
    if elo_df_local.empty:
        return player_to_tennisabstract_url(name)

    norm_name_site = normalize_name(name)

    # 1. Try exact match (after normalization)
    elo_df_local['normalized_player'] = elo_df_local['player'].apply(normalize_name)
    exact_match = elo_df_local[elo_df_local['normalized_player'] == norm_name_site]
    if not exact_match.empty:
        matched_elo_name = exact_match['player'].iloc[0]
        logging.debug(f"Exact match for '{name}' -> '{matched_elo_name}'")
        return player_to_tennisabstract_url(matched_elo_name)

    # 2. Try approximate match with difflib
    names_list_elo = elo_df_local['normalized_player'].tolist()
    close_matches = difflib.get_close_matches(norm_name_site, names_list_elo, n=1, cutoff=0.80)

    if close_matches:
        matched_normalized_name = close_matches[0]
        original_elo_name_series = elo_df_local[elo_df_local['normalized_player'] == matched_normalized_name]['player']
        if not original_elo_name_series.empty:
            original_elo_name = original_elo_name_series.iloc[0]
            logging.debug(f"Close match for '{name}' ({norm_name_site}) -> '{original_elo_name}' ({matched_normalized_name})")
            return player_to_tennisabstract_url(original_elo_name)

    # 3. Fallback to direct conversion
    logging.warning(f"No close match for '{name}' in Elo DB. Using direct conversion for URL.")
    return player_to_tennisabstract_url(name)

def main():
    """Main function to orchestrate the scraping process"""
    try:
        logging.info("Starting Betclic scraper v2 with ScraperAPI...")
        
        # Scrape matches
        matches = scrape_betclic_matches()
        
        if not matches:
            logging.warning("No matches found after scraping")
            return

        # Create DataFrame
        df = pd.DataFrame(matches)
        logging.info(f"Created DataFrame with {len(df)} matches")

        # Get ELO data from Supabase
        logging.info("Retrieving ELO data from Supabase...")
        try:
            elo_response = supabase.table("atp_elo_ratings").select("*")
            if elo_response["error"] is None and elo_response["data"]:
                elo_players = elo_response["data"]
                elo_df = pd.DataFrame(elo_players)
                logging.info(f"Loaded {len(elo_df)} players from ELO data")
            else:
                logging.warning(f"No ELO data received from Supabase or error. Error: {elo_response['error']}. The 'atp_elo_ratings' table may be empty.")
                elo_df = pd.DataFrame(columns=['player'])
        except Exception as e:
            logging.error(f"Error retrieving ELO data: {e}")
            elo_df = pd.DataFrame(columns=['player'])

        # Generate Tennis Abstract URLs for players
        logging.info("Searching for player matches and generating Tennis Abstract URLs...")
        df["player1_url"] = df["player1"].apply(lambda n: find_best_slug_url(n, elo_df))
        df["player2_url"] = df["player2"].apply(lambda n: find_best_slug_url(n, elo_df))

        # Ensure column order for database
        final_columns = ["date", "heure", "tournoi", "tour", "player1", "player2", "match_url",
                         "player1_url", "player2_url", "scraped_date", "scraped_time"]
        
        # Ensure all columns exist
        for col in final_columns:
            if col not in df.columns:
                df[col] = None if col.endswith("_url") else ""

        df_for_upload = df[final_columns].copy()
        
        # Filter matches with players present in ELO database
        if not elo_df.empty:
            normalized_players_set = set(elo_df['player'].apply(normalize_name))
            df_for_upload = df_for_upload[
                (df_for_upload["player1"].apply(normalize_name).isin(normalized_players_set)) &
                (df_for_upload["player2"].apply(normalize_name).isin(normalized_players_set))
            ]
        
        logging.info(f"Matches with both players in ELO database: {len(df_for_upload)}/{len(df)}")

        # Delete old matches from table
        logging.info("Deleting old matches from 'upcoming_matches' table...")
        try:
            delete_response = supabase.table("upcoming_matches").delete_rows(params={"id": "neq.-1"})
            logging.info(f"Delete response: {delete_response['data'] if delete_response['error'] is None else delete_response['error']}")
            if delete_response["error"]:
                logging.error(f"Error deleting old matches: {delete_response['error']}")
        except Exception as e:
            logging.error(f"Error deleting old matches: {e}")

        # Insert new matches (in chunks of 100)
        logging.info(f"Inserting {len(df_for_upload)} new matches...")
        data_to_insert = df_for_upload.to_dict(orient='records')

        chunk_size = 100
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            try:
                insert_response = supabase.table("upcoming_matches").insert(chunk)
                logging.info(f"Chunk {i // chunk_size + 1} inserted. Response: {insert_response['data'] if insert_response['error'] is None else insert_response['error']}")
                if insert_response["error"] is not None:
                    logging.error(f"Supabase error during chunk insertion: {insert_response['error']}")
            except Exception as e:
                logging.error(f"Error inserting chunk {i // chunk_size + 1}: {e}")

        logging.info(f"Process completed. {len(df_for_upload)} matches potentially processed for insertion.")
        
    except Exception as e:
        logging.error(f"Error in main function: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()