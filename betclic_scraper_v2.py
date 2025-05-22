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
        'wait': '10000',  # Wait 10 seconds for page to load
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

def close_popins(driver_instance):
    """Close popups and overlays - copied from your working script"""
    logging.info("Tentative de fermeture des popins...")
    popin_closed_by_click = False
    # Essayer de cliquer sur les boutons de consentement courants
    known_popin_selectors = [
        "button#popin_tc_privacy_button_2",  # "Tout Accepter" spécifique Betclic
        "button[class*='popin_tc_privacy_button'][mode='primary']",  # Boutons primaires dans les popins de privacy
        "button[aria-label='Continuer sans accepter']",
        "button[id^='onetrust-accept-btn-handler']",  # Onetrust
        "button.didomi-components-button.didomi-components-button-primary",  # Didomi
    ]
    for selector in known_popin_selectors:
        try:
            buttons = driver_instance.find_elements(By.CSS_SELECTOR, selector)
            for btn in buttons:
                if btn.is_displayed() and btn.is_enabled():
                    logging.info(f"Click sur popin via selector: {selector}")
                    driver_instance.execute_script("arguments[0].click();", btn)  # Clic JS plus robuste
                    popin_closed_by_click = True
                    time.sleep(2)  # Attendre que la popin disparaisse
        except Exception as e:
            logging.debug(f"Erreur en cliquant sur {selector}: {e}")

    if popin_closed_by_click:
        logging.info("Popin fermée par clic.")
    else:
        logging.info("Aucune popin évidente trouvée pour clic, ou échec du clic.")

    # Forcer la suppression des overlays/popins si toujours présents
    js_remove_selectors = [
        '[class*="popin_tc_privacy"]',  # Betclic privacy
        '[id^="onetrust-banner"]',  # Onetrust banner
        '[id="didomi-host"]',  # Didomi host
        '[class*="overlay"]',  # Classes génériques d'overlay
        '[role="dialog"]'  # Rôles de dialogue souvent utilisés pour les modales
    ]
    removed_count = 0
    for selector in js_remove_selectors:
        script = f"""
            let count = 0;
            document.querySelectorAll('{selector}').forEach(el => {{
                el.remove();
                count++;
            }});
            return count;
        """
        try:
            removed = driver_instance.execute_script(script)
            if removed > 0:
                logging.info(f"{removed} élément(s) correspondant à '{selector}' supprimé(s) via JS.")
                removed_count += removed
        except Exception as e:
            logging.warning(f"Erreur lors de la suppression JS de '{selector}': {e}")

    if removed_count > 0:
        logging.info(f"Total de {removed_count} éléments de popin/overlay supprimés via JS.")
    else:
        logging.info("Aucun élément de popin/overlay supplémentaire supprimé via JS.")
    time.sleep(1)

def scrape_with_selenium(url):
    """Fallback scraping using Selenium - copied from your working script"""
    if not SELENIUM_AVAILABLE:
        logging.error("Selenium not available for fallback")
        return []
    
    logging.info("Using Selenium fallback approach...")
    
    # --- Options pour le scroll ---
    MAX_SCROLL_ATTEMPTS = 10  # Nombre maximum de tentatives de scroll
    SCROLL_PAUSE_TIME = 3  # Temps d'attente en secondes après chaque scroll pour que le contenu charge
    TARGET_MATCH_COUNT = 100  # Optionnel: arrêter si on a trouvé au moins X matchs après scroll

    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.110 Safari/537.36"
    )

    # Initialisation du driver
    driver = uc.Chrome(options=chrome_options)
    
    matches = []
    seen_urls = set()
    scraped_dt = datetime.now()

    try:
        driver.get(url)
        logging.info("Page chargée. Attente initiale de 5 secondes...")
        time.sleep(5)

        close_popins(driver)  # Appeler la fonction de fermeture des popins
        logging.info("Attente de 2 secondes après la gestion des popins...")
        time.sleep(2)

        # --- Logique de scroll ---
        logging.info("Début du scroll pour charger plus de matchs...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        match_elements_count_before_scroll = 0

        for i in range(MAX_SCROLL_ATTEMPTS):
            logging.info(f"Scroll attempt {i + 1}/{MAX_SCROLL_ATTEMPTS}")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)  # Attendre que la page charge

            new_height = driver.execute_script("return document.body.scrollHeight")
            current_match_elements = driver.find_elements(By.TAG_NAME, "sports-events-event-card")
            current_match_elements_count = len(current_match_elements)

            logging.info(
                f"Hauteur actuelle: {new_height}, Nombre d'éléments 'sports-events-event-card': {current_match_elements_count}")

            if new_height == last_height and current_match_elements_count == match_elements_count_before_scroll:
                logging.info("Fin du scroll : la hauteur de la page et le nombre de matchs n'ont pas changé.")
                break

            last_height = new_height
            match_elements_count_before_scroll = current_match_elements_count

            if TARGET_MATCH_COUNT > 0 and current_match_elements_count >= TARGET_MATCH_COUNT:
                logging.info(f"Nombre de matchs cible ({TARGET_MATCH_COUNT}) atteint ou dépassé. Arrêt du scroll.")
                break

            # Petite pause supplémentaire si le contenu semble toujours se charger
            time.sleep(1)
        else:  # Exécuté si la boucle for se termine sans 'break' (c'est-à-dire, MAX_SCROLL_ATTEMPTS atteint)
            logging.info(f"Nombre maximum de tentatives de scroll ({MAX_SCROLL_ATTEMPTS}) atteint.")

        logging.info("Fin du scroll.")

        # Extract matches
        logging.info("Extraction des informations des matchs après scroll...")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        match_cards = soup.find_all("sports-events-event-card")
        logging.info(f"Nombre total de cartes de match trouvées après scroll: {len(match_cards)}")

        for card_index, card in enumerate(match_cards):
            current_date = ""
            current_heure = ""
            current_tournoi = ""
            current_tour = ""

            # Noms des joueurs
            players = card.find_all("div", class_="scoreboard_contestantLabel")
            player1 = players[0].text.strip() if len(players) > 0 else ""
            player2 = players[1].text.strip() if len(players) > 1 else ""

            # URL de la rencontre
            a_tag = card.find("a", class_="cardEvent")
            match_url = ""
            if a_tag and "href" in a_tag.attrs:
                match_url = "https://www.betclic.fr" + a_tag["href"]

            if not match_url or match_url in seen_urls:
                logging.debug(f"Match {player1} vs {player2} ignoré (URL vide ou déjà vue: {match_url})")
                continue
            seen_urls.add(match_url)

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

            # Date et heure
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

            # Extraction du nom du tournoi depuis l'URL
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
        logging.error(f"Erreur lors de l'extraction Selenium: {str(e)}", exc_info=True)
    finally:
        # Enregistrement de la page pour le débogage
        with open("page_debug_selenium.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source if 'driver' in locals() and driver.page_source else "No page source available")
        logging.info("HTML Selenium sauvegardé dans page_debug_selenium.html")

        # Fermeture du navigateur
        if 'driver' in locals():
            driver.quit()

    return matches

def scrape_betclic_matches():
    """
    Hybrid scraping: try ScraperAPI first, fallback to Selenium if needed
    """
    url = "https://www.betclic.fr/tennis-stennis"
    
    # First, try ScraperAPI
    logging.info("Attempting to scrape with ScraperAPI...")
    page_content = get_scraperapi_response(url)
    
    scraperapi_matches = []
    if page_content:
        # Save page content for debugging
        with open("page_debug_scraperapi.html", "w", encoding="utf-8") as f:
            f.write(page_content)
        logging.info("ScraperAPI HTML saved to page_debug_scraperapi.html")
        
        soup = BeautifulSoup(page_content, "html.parser")
        
        # Try to extract JSON data first
        try:
            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string and '"matches":[' in script.string:
                    script_content = script.string
                    start_idx = script_content.find('"matches":[')
                    if start_idx != -1:
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
                                import json
                                json_matches = json.loads(matches_json_str)
                                logging.info(f"ScraperAPI: Successfully extracted {len(json_matches)} matches from JSON data")
                                
                                scraped_dt = datetime.now()
                                seen_urls = set()
                                
                                for match_data in json_matches:
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
                                                    
                                                    scraperapi_matches.append(match_info)
                                                    
                                    except Exception as e:
                                        logging.warning(f"Error processing JSON match: {e}")
                                        continue
                                break
                            except json.JSONDecodeError as e:
                                logging.warning(f"Failed to parse JSON matches: {e}")
                                continue
        except Exception as e:
            logging.warning(f"Error extracting JSON data from ScraperAPI: {e}")
        
        # If no JSON matches, try HTML parsing
        if not scraperapi_matches:
            logging.info("No JSON matches found in ScraperAPI response, trying HTML parsing...")
            match_cards = soup.find_all("sports-events-event-card")
            logging.info(f"ScraperAPI: Found {len(match_cards)} match cards via HTML parsing")
            # Could implement HTML parsing here if needed
    
    # Decide whether to use ScraperAPI results or fallback to Selenium
    if len(scraperapi_matches) > 40:  # If we got more than 40 matches, ScraperAPI worked well
        logging.info(f"ScraperAPI successful: {len(scraperapi_matches)} matches found. Using ScraperAPI results.")
        return scraperapi_matches
    else:
        logging.info(f"ScraperAPI returned only {len(scraperapi_matches)} matches. Falling back to Selenium...")
        selenium_matches = scrape_with_selenium(url)
        
        if len(selenium_matches) > len(scraperapi_matches):
            logging.info(f"Selenium successful: {len(selenium_matches)} matches found. Using Selenium results.")
            return selenium_matches
        else:
            logging.info(f"Selenium didn't improve results. Using best available: {max(len(scraperapi_matches), len(selenium_matches))} matches.")
            return scraperapi_matches if len(scraperapi_matches) >= len(selenium_matches) else selenium_matches

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
        logging.info("Starting Betclic hybrid scraper (ScraperAPI + Selenium fallback)...")
        
        # Scrape matches using hybrid approach
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
