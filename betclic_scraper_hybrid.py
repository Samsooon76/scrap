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

# Selenium imports for fallback
try:
    import undetected_chromedriver as uc
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium not available. Will only use ScraperAPI.")

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

def get_scraperapi_response(url, retries=3):
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
        'wait': '45000',  # Wait 45s for complete loading
        'scroll': 'true',  # Enable scrolling to load dynamic content
        'scroll_count': '50',  # Scroll 50 times to load ALL matches
        'scroll_timeout': '3000',  # Wait 3s between scrolls (faster but more scrolls)
        'scroll_pause_time': '2000',  # Extra pause after scrolling sequence
        'js_snippet': 'window.scrollTo(0, document.body.scrollHeight);',  # Custom JS to ensure full scroll
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
            
            response = requests.get(SCRAPERAPI_ENDPOINT, params=params, headers=headers, timeout=70)
            
            if response.status_code == 200:
                content = response.text
                # Check if we got a 403 error page
                if "Error 403" in content or "Forbidden" in content:
                    logging.warning(f"Received 403 Forbidden page from Betclic (attempt {attempt + 1})")
                    if attempt < retries - 1:
                        time.sleep(5)  # Wait before retry
                        continue
                else:
                    logging.info(f"Successfully fetched {url}")
                    return content
            else:
                logging.warning(f"ScraperAPI returned status code {response.status_code} for {url}")
                if attempt < retries - 1:
                    time.sleep(5)  # Wait before retry
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(5)  # Wait before retry
    
    logging.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def scrape_betclic_matches():
    """
    Scrape matches using ScraperAPI with HTML parsing
    """
    url = "https://www.betclic.fr/tennis-stennis"  # All tennis matches
    
    # Try ScraperAPI
    logging.info("Attempting to scrape with ScraperAPI...")
    page_content = get_scraperapi_response(url)
    
    scraperapi_matches = []
    if page_content:
        # Save page content for debugging
        with open("page_debug_scraperapi.html", "w", encoding="utf-8") as f:
            f.write(page_content)
        logging.info("ScraperAPI HTML saved to page_debug_scraperapi.html")
        
        soup = BeautifulSoup(page_content, "html.parser")
        
        # HTML parsing to get all matches
        logging.info("Using HTML parsing approach to get all matches...")
        match_cards = soup.find_all("sports-events-event-card")
        logging.info(f"ScraperAPI: Found {len(match_cards)} match cards via HTML parsing")
        
        if match_cards:
            scraped_dt = datetime.now()
            seen_urls = set()
            
            for card_index, card in enumerate(match_cards):
                try:
                    # Extract player names
                    players = card.find_all("div", class_="scoreboard_contestantLabel")
                    if len(players) >= 2:
                        player1 = players[0].get_text(strip=True)
                        player2 = players[1].get_text(strip=True)
                        
                        # Extract match URL
                        a_tag = card.find("a")
                        if a_tag and a_tag.get("href"):
                            match_url = "https://www.betclic.fr" + a_tag["href"]
                            
                            if match_url not in seen_urls:
                                seen_urls.add(match_url)
                                
                                # Extract date and time
                                date_str = "Unknown"
                                heure_str = "Unknown"
                                
                                # Look for time info in various possible locations
                                time_elements = card.find_all(["div", "span"], class_=["event_infoTime", "event-time", "match-time"])
                                for time_elem in time_elements:
                                    if time_elem and time_elem.get_text(strip=True):
                                        time_text = time_elem.get_text(strip=True)
                                        if ":" in time_text:  # Likely contains time
                                            parts = time_text.split()
                                            if len(parts) >= 2:
                                                date_str = parts[0]
                                                heure_str = parts[-1]
                                            elif len(parts) == 1 and ":" in parts[0]:
                                                heure_str = parts[0]
                                        break
                                
                                # Extract tournament info from URL
                                tournoi = ""
                                url_parts = a_tag["href"].split("/")
                                if len(url_parts) > 2:
                                    tournoi_slug = url_parts[2]
                                    # Clean up tournament name
                                    tournoi_match = re.match(r"^(.*?)(-c\d+)?$", tournoi_slug)
                                    if tournoi_match:
                                        tournoi_slug = tournoi_match.group(1)
                                    tournoi = tournoi_slug.replace('-', ' ').title()
                                
                                match_info = {
                                    "date": date_str,
                                    "heure": heure_str, 
                                    "tournoi": tournoi,
                                    "tour": "",
                                    "player1": player1,
                                    "player2": player2,
                                    "scraped_date": scraped_dt.strftime("%Y-%m-%d"),
                                    "scraped_time": scraped_dt.strftime("%H:%M:%S"),
                                    "match_url": match_url
                                }
                                
                                scraperapi_matches.append(match_info)
                                logging.debug(f"Match {card_index + 1}: {player1} vs {player2} | {date_str} {heure_str} | {tournoi}")
                                
                except Exception as e:
                    logging.debug(f"Error parsing HTML match card {card_index + 1}: {e}")
                    continue
        
        logging.info(f"ScraperAPI HTML parsing complete: {len(scraperapi_matches)} total matches extracted")
    
    return scraperapi_matches

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
        logging.info("Starting Betclic scraper (ScraperAPI with HTML parsing only)...")
        
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
        
        # TEMPORARILY DISABLED: Keep all matches regardless of ELO database
        # This will be re-enabled once we have both ATP and WTA data
        logging.info(f"All matches will be uploaded: {len(df_for_upload)}/{len(df)}")

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