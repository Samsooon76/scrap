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
import httpx
from typing import List, Dict, Any
import json

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "aae0279e8feccdcfb5b40c85fdd65a66")

# MinimalSupabaseClient
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
    Fetch a URL using ScraperAPI with enhanced parameters for Render
    """
    params = {
        'api_key': SCRAPERAPI_KEY,
        'url': url,
        'render': 'true',
        'country_code': 'fr',
        'device_type': 'desktop',
        'premium': 'true',
        'session_number': random.randint(1, 1000),
        'keep_headers': 'true',
        'autoparse': 'false',
        'format': 'html',
        'wait': '60000',  # Augmenté à 60s pour permettre le chargement complet
        'scroll': 'true',
        'scroll_count': '100',  # Plus de scrolls pour capturer plus de matches
        'scroll_timeout': '2000',  # 2s entre les scrolls
        'scroll_pause_time': '3000',  # Pause après tous les scrolls
        'js_snippet': '''
            // Script JavaScript pour forcer le chargement de tous les matches
            for(let i = 0; i < 100; i++) {
                window.scrollTo(0, document.body.scrollHeight);
                await new Promise(resolve => setTimeout(resolve, 100));
            }
            // Attendre que tous les éléments soient chargés
            await new Promise(resolve => setTimeout(resolve, 5000));
        ''',
        'screenshot': 'false'
    }
    
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
            logging.info(f"Fetching {url} via ScraperAPI (attempt {attempt + 1}/{retries}) - Enhanced for Render")
            
            # Randomize session for each attempt
            params['session_number'] = random.randint(1, 1000)
            
            response = requests.get(SCRAPERAPI_ENDPOINT, params=params, headers=headers, timeout=120)
            
            if response.status_code == 200:
                content = response.text
                if "Error 403" in content or "Forbidden" in content:
                    logging.warning(f"Received 403 Forbidden page (attempt {attempt + 1})")
                    if attempt < retries - 1:
                        time.sleep(10)
                        continue
                else:
                    logging.info(f"Successfully fetched {url} - Content length: {len(content)}")
                    return content
            else:
                logging.warning(f"ScraperAPI returned status code {response.status_code}")
                if attempt < retries - 1:
                    time.sleep(10)
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            if attempt < retries - 1:
                time.sleep(10)
    
    logging.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def extract_json_matches(soup):
    """Extract matches from JSON data in script tags"""
    json_matches = []
    script_tags = soup.find_all("script")
    
    for script in script_tags:
        if script.string and '"matches":[' in script.string:
            script_content = script.string
            start_idx = script_content.find('"matches":[')
            if start_idx != -1:
                # Extract JSON array
                bracket_count = 0
                start_bracket = script_content.find('[', start_idx)
                current_idx = start_bracket

                while current_idx < len(script_content):
                    if script_content[current_idx] == '[':
                        bracket_count += 1
                    elif script_content[current_idx] == ']':
                        bracket_count -= 1
                        if bracket_count == 0:
                            end_idx = current_idx + 1
                            break
                    current_idx += 1

                if bracket_count == 0:
                    matches_json_str = script_content[start_bracket:end_idx]
                    try:
                        matches_data = json.loads(matches_json_str)
                        logging.info(f"JSON: Found {len(matches_data)} matches in script data")
                        
                        scraped_dt = datetime.now()
                        seen_urls = set()
                        
                        for match_data in matches_data:
                            try:
                                match_id = match_data.get("matchId", "")
                                contestants = match_data.get("contestants", [])
                                if len(contestants) >= 2:
                                    player1 = contestants[0].get("name", "")
                                    player2 = contestants[1].get("name", "")

                                    competition = match_data.get("competition", {})
                                    tournoi = competition.get("name", "")

                                    match_date_utc = match_data.get("matchDateUtc", "")
                                    if match_date_utc:
                                        try:
                                            match_dt = datetime.fromisoformat(match_date_utc.replace('Z', '+00:00'))
                                            date_str = match_dt.strftime("%d/%m")
                                            heure_str = match_dt.strftime("%H:%M")
                                        except:
                                            date_str = "Unknown"
                                            heure_str = "Unknown"
                                    else:
                                        date_str = "Unknown"
                                        heure_str = "Unknown"

                                    if player1 and player2 and match_id:
                                        def name_to_slug(name):
                                            return name.lower().replace(" ", "-").replace(".", "")

                                        player1_slug = name_to_slug(player1)
                                        player2_slug = name_to_slug(player2)
                                        competition_slug = name_to_slug(tournoi) if tournoi else "unknown"

                                        match_url = f"https://www.betclic.fr/tennis-stennis/{competition_slug}/{player1_slug}-{player2_slug}-m{match_id}"

                                        if match_url not in seen_urls:
                                            seen_urls.add(match_url)

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

                                            json_matches.append(match_info)

                            except Exception as e:
                                logging.warning(f"Error processing JSON match: {e}")
                                continue
                        break
                    except json.JSONDecodeError as e:
                        logging.warning(f"Failed to parse JSON matches: {e}")
                        continue
    
    return json_matches

def extract_html_matches(soup):
    """Extract matches from HTML sports-events-event-card elements"""
    html_matches = []
    match_cards = soup.find_all("sports-events-event-card")
    logging.info(f"HTML: Found {len(match_cards)} match cards")
    
    scraped_dt = datetime.now()
    seen_urls = set()
    
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
                continue
            seen_urls.add(match_url)

            # Extract full player names from URL
            player1_full, player2_full = player1, player2
            if a_tag and "href" in a_tag.attrs:
                match_obj = re.search(r'/([a-z0-9\-]+)-m\d+$', a_tag["href"])
                if match_obj:
                    full_slug = match_obj.group(1)
                    parts = full_slug.split('-')
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
                if "Auj." in date_heure_text or "Dem." in date_heure_text:
                    parts = date_heure_text.split()
                    if len(parts) >= 2:
                        current_date = parts[0]
                        current_heure = parts[-1]
                else:
                    parts = date_heure_text.split()
                    if len(parts) == 3:
                        current_date = f"{parts[0]} {parts[1]}"
                        current_heure = parts[2]
                    elif len(parts) == 2:
                        current_date = parts[0]
                        current_heure = parts[1]

            # Extract tournament name from URL
            if a_tag and "href" in a_tag.attrs:
                url_parts = a_tag["href"].split("/")
                if len(url_parts) > 2:
                    tournoi_slug_full = url_parts[2]
                    tournoi_match = re.match(r"^(.*?)(-c\d+)?$", tournoi_slug_full)
                    if tournoi_match:
                        tournoi_slug = tournoi_match.group(1)
                        current_tournoi = tournoi_slug.replace('-', ' ').title()
                    else:
                        current_tournoi = tournoi_slug_full.replace('-', ' ').title()

            if player1_full and player2_full:
                html_matches.append({
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
            logging.warning(f"Error processing HTML card {card_index}: {e}")
            continue
    
    return html_matches

def scrape_betclic_matches():
    """
    Enhanced scraping optimized for Render environment
    """
    url = "https://www.betclic.fr/tennis-stennis"
    
    logging.info("=== ENHANCED BETCLIC SCRAPER FOR RENDER ===")
    page_content = get_scraperapi_response(url)
    
    if not page_content:
        logging.error("Failed to get page content from ScraperAPI")
        return []

    # Save debug content
    with open("page_debug_render.html", "w", encoding="utf-8") as f:
        f.write(page_content)
    logging.info("Page content saved to page_debug_render.html")

    soup = BeautifulSoup(page_content, "html.parser")
    
    # Extract matches using both methods
    logging.info("=== EXTRACTING JSON MATCHES ===")
    json_matches = extract_json_matches(soup)
    
    logging.info("=== EXTRACTING HTML MATCHES ===")
    html_matches = extract_html_matches(soup)
    
    # Combine and deduplicate matches
    all_matches = []
    seen_urls = set()
    
    # Add JSON matches first
    for match in json_matches:
        if match["match_url"] not in seen_urls:
            seen_urls.add(match["match_url"])
            all_matches.append(match)
    
    # Add HTML matches that are not already in JSON
    for match in html_matches:
        if match["match_url"] not in seen_urls:
            seen_urls.add(match["match_url"])
            all_matches.append(match)
    
    logging.info(f"=== FINAL RESULTS ===")
    logging.info(f"JSON matches: {len(json_matches)}")
    logging.info(f"HTML matches: {len(html_matches)}")
    logging.info(f"Total unique matches: {len(all_matches)}")
    
    return all_matches

def normalize_name(name):
    """Normalize player name for comparison"""
    return ' '.join(str(name).replace('\xa0', ' ').split()).lower()

def player_to_tennisabstract_url(player_name_on_site):
    """Convert player name to Tennis Abstract URL format"""
    normalized_for_url = str(player_name_on_site).lower()
    normalized_for_url = re.sub(r'[^a-z0-9\s-]', '', normalized_for_url)
    normalized_for_url = normalized_for_url.replace(' ', '').replace('-', '')
    return f"https://www.tennisabstract.com/cgi-bin/player.cgi?p={normalized_for_url}"

def find_best_slug_url(name, elo_df_local):
    """Find the best matching player URL from ELO data"""
    if elo_df_local.empty:
        return player_to_tennisabstract_url(name)

    norm_name_site = normalize_name(name)

    # Try exact match
    elo_df_local['normalized_player'] = elo_df_local['player'].apply(normalize_name)
    exact_match = elo_df_local[elo_df_local['normalized_player'] == norm_name_site]
    if not exact_match.empty:
        matched_elo_name = exact_match['player'].iloc[0]
        logging.debug(f"Exact match for '{name}' -> '{matched_elo_name}'")
        return player_to_tennisabstract_url(matched_elo_name)

    # Try approximate match
    names_list_elo = elo_df_local['normalized_player'].tolist()
    close_matches = difflib.get_close_matches(norm_name_site, names_list_elo, n=1, cutoff=0.80)

    if close_matches:
        matched_normalized_name = close_matches[0]
        original_elo_name_series = elo_df_local[elo_df_local['normalized_player'] == matched_normalized_name]['player']
        if not original_elo_name_series.empty:
            original_elo_name = original_elo_name_series.iloc[0]
            logging.debug(f"Close match for '{name}' ({norm_name_site}) -> '{original_elo_name}' ({matched_normalized_name})")
            return player_to_tennisabstract_url(original_elo_name)

    # Fallback
    logging.warning(f"No close match for '{name}' in Elo DB. Using direct conversion.")
    return player_to_tennisabstract_url(name)

def main():
    """Main function optimized for Render"""
    try:
        logging.info("=== STARTING ENHANCED BETCLIC SCRAPER FOR RENDER ===")
        
        # Detect environment
        is_render = 'RENDER' in os.environ
        logging.info(f"Running on Render: {is_render}")

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
                logging.warning(f"No ELO data received. Error: {elo_response['error']}")
                elo_df = pd.DataFrame(columns=['player'])
        except Exception as e:
            logging.error(f"Error retrieving ELO data: {e}")
            elo_df = pd.DataFrame(columns=['player'])

        # Generate Tennis Abstract URLs
        logging.info("Generating Tennis Abstract URLs...")
        df["player1_url"] = df["player1"].apply(lambda n: find_best_slug_url(n, elo_df))
        df["player2_url"] = df["player2"].apply(lambda n: find_best_slug_url(n, elo_df))

        # Prepare final columns
        final_columns = ["date", "heure", "tournoi", "tour", "player1", "player2", "match_url",
                         "player1_url", "player2_url", "scraped_date", "scraped_time"]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None if col.endswith("_url") else ""

        df_for_upload = df[final_columns].copy()

        # For Render: Keep ALL matches instead of filtering by ELO
        # This ensures we don't lose matches due to ELO filtering
        if is_render:
            logging.info(f"RENDER MODE: Keeping all {len(df_for_upload)} matches (no ELO filtering)")
        else:
            # Local mode: apply ELO filtering
            if not elo_df.empty:
                normalized_players_set = set(elo_df['player'].apply(normalize_name))
                df_for_upload = df_for_upload[
                    (df_for_upload["player1"].apply(normalize_name).isin(normalized_players_set)) &
                    (df_for_upload["player2"].apply(normalize_name).isin(normalized_players_set))
                ]
                logging.info(f"LOCAL MODE: Filtered to {len(df_for_upload)}/{len(df)} matches with ELO data")

        # Delete old matches
        logging.info("Deleting old matches from 'upcoming_matches' table...")
        try:
            delete_response = supabase.table("upcoming_matches").delete_rows(params={"id": "neq.-1"})
            if delete_response["error"]:
                logging.error(f"Error deleting old matches: {delete_response['error']}")
            else:
                logging.info("Successfully deleted old matches")
        except Exception as e:
            logging.error(f"Error deleting old matches: {e}")

        # Insert new matches
        logging.info(f"Inserting {len(df_for_upload)} new matches...")
        data_to_insert = df_for_upload.to_dict(orient='records')

        chunk_size = 100
        total_inserted = 0
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            try:
                insert_response = supabase.table("upcoming_matches").insert(chunk)
                if insert_response["error"] is None:
                    total_inserted += len(chunk)
                    logging.info(f"Chunk {i // chunk_size + 1} inserted successfully ({len(chunk)} matches)")
                else:
                    logging.error(f"Error inserting chunk {i // chunk_size + 1}: {insert_response['error']}")
            except Exception as e:
                logging.error(f"Error inserting chunk {i // chunk_size + 1}: {e}")

        logging.info(f"=== PROCESS COMPLETED ===")
        logging.info(f"Total matches scraped: {len(matches)}")
        logging.info(f"Matches inserted to database: {total_inserted}")
        logging.info(f"Success rate: {total_inserted}/{len(df_for_upload)} matches inserted")

    except Exception as e:
        logging.error(f"Error in main function: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 