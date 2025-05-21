import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import json
import httpx
from dotenv import load_dotenv
from datetime import datetime
import random
import re
import logging
import difflib

# Version V2 (mise à jour le 22/05/2025) - VERSION FINALE AVEC HTML SCRAPING ET LOGS DE DEBUG AMÉLIORÉS
logging.info("=== RUNNING FINAL HTML SCRAPING VERSION WITH MINIMAL SUPABASE CLIENT (22/05/2025) - DEBUG LOGS V2 ===")

# Configuration du logging - AVANT tout pour voir tous les logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.critical("SUPABASE_URL and SUPABASE_KEY environment variables are not set or empty.")
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set for the script to run.")

# Define a minimal Supabase client class that uses httpx directly
class MinimalSupabaseClient:
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.key = key
        self.rest_url = f"{self.url}/rest/v1"
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
    def table(self, table_name):
        return MinimalSupabaseTable(self, table_name)
        
    def request(self, method, url, **kwargs):
        # Merge headers
        headers = {**self.headers, **kwargs.get('headers', {})}
        kwargs['headers'] = headers
        
        # Make request with httpx
        with httpx.Client(timeout=30.0) as client:
            response = client.request(method, url, **kwargs)
            
        # Check for errors
        if response.status_code >= 400:
            logging.error(f"Supabase API error: {response.status_code} - {response.text}")
            response.raise_for_status()
            
        return response.json()
        
class MinimalSupabaseTable:
    def __init__(self, client, table_name):
        self.client = client
        self.table_name = table_name
        self.url = f"{client.rest_url}/{table_name}"
        self.current_query = {}
        
    def select(self, columns="*"):
        self.current_query['select'] = columns
        return self
        
    def eq(self, column, value):
        self.current_query[column] = f"eq.{value}"
        return self
        
    def neq(self, column, value):
        self.current_query[column] = f"neq.{value}"
        return self
        
    def execute(self):
        # Build query params
        params = {}
        if 'select' in self.current_query:
            params['select'] = self.current_query.pop('select')
            
        # Add other filters
        for key, value in self.current_query.items():
            params[key] = value
            
        # Reset current query
        self.current_query = {}
        
        # Make request
        try:
            result = self.client.request('GET', self.url, params=params)
            return SupabaseResponse(result)
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return SupabaseResponse([])
            
    def insert(self, data):
        try:
            result = self.client.request('POST', self.url, json=data)
            return SupabaseResponse(result)
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            return SupabaseResponse([])
            
    def delete(self):
        return self
        
class SupabaseResponse:
    def __init__(self, data):
        self.data = data

# Use our minimal implementation
logging.info("Using minimal Supabase client implementation...")
supabase = MinimalSupabaseClient(SUPABASE_URL, SUPABASE_KEY)

url = "https://www.betclic.fr/tennis-stennis"

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
# Déclaration initiale du driver pour qu'il soit dans le scope du finally
driver = None 

def close_popins(driver_instance):
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
        logging.info(f"Vérification du sélecteur de pop-in: {selector}")
        try:
            buttons = driver_instance.find_elements(By.CSS_SELECTOR, selector)
            if not buttons:
                logging.info(f"Aucun bouton trouvé pour le sélecteur: {selector}")
                continue
            logging.info(f"{len(buttons)} bouton(s) trouvé(s) pour le sélecteur: {selector}")
            for i, btn in enumerate(buttons):
                logging.info(f"Bouton {i+1}/{len(buttons)} pour {selector}: Affiché? {btn.is_displayed()}, Activé? {btn.is_enabled()}")
                if btn.is_displayed() and btn.is_enabled():
                    logging.info(f"Tentative de clic sur popin via selector: {selector} (bouton {i+1})")
                    driver_instance.execute_script("arguments[0].click();", btn)  # Clic JS plus robuste
                    logging.info(f"Clic JS exécuté pour {selector} (bouton {i+1})")
                    popin_closed_by_click = True
                    time.sleep(2)  # Attendre que la popin disparaisse
                    break # Sortir de la boucle des boutons pour ce sélecteur
            if popin_closed_by_click:
                 break # Sortir de la boucle des sélecteurs si un clic a réussi
        except Exception as e:
            logging.error(f"Erreur en cherchant/cliquant sur {selector}: {e}")

    if popin_closed_by_click:
        logging.info("Popin fermée par clic.")
    else:
        logging.info("Aucune popin évidente n'a pu être fermée par clic.")

    # Forcer la suppression des overlays/popins si toujours présents
    logging.info("Tentative de suppression des overlays/popins via JS...")
    js_remove_selectors = [
        '[class*="popin_tc_privacy"]',  # Betclic privacy
        '[id^="onetrust-banner"]',  # Onetrust banner
        '[id="didomi-host"]',  # Didomi host
        '[class*="overlay"]',  # Classes génériques d'overlay
        '[role="dialog"]'  # Rôles de dialogue souvent utilisés pour les modales
    ]
    total_removed_count = 0
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
                total_removed_count += removed
        except Exception as e:
            logging.warning(f"Erreur lors de la suppression JS de '{selector}': {e}")

    if total_removed_count > 0:
        logging.info(f"Total de {total_removed_count} éléments de popin/overlay supprimés via JS.")
    else:
        logging.info("Aucun élément de popin/overlay supplémentaire n'a été supprimé via JS.")
    time.sleep(1)

# DÉMARRAGE DU SCRIPT PRINCIPAL
matches = [] # Initialisation pour le cas où le try échoue avant

try:
    logging.info("Initialisation du driver Chrome...")
    driver = uc.Chrome(options=chrome_options)
    logging.info("Driver initialisé.")

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
    # --- Fin de la logique de scroll ---

    seen_urls = set()
    scraped_dt = datetime.now()

    logging.info("Extraction des informations des matchs après scroll...")
    # Il est crucial de récupérer le page_source APRÈS tous les scrolls
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    match_cards = soup.find_all("sports-events-event-card")
    logging.info(f"Nombre total de cartes de match trouvées après scroll: {len(match_cards)}")

    for card_index, card in enumerate(match_cards):
        current_date = ""  # Réinitialiser pour chaque carte
        current_heure = ""  # Réinitialiser pour chaque carte
        current_tournoi = ""  # Réinitialiser pour chaque carte
        current_tour = ""  # Réinitialiser pour chaque carte (toujours vide pour Betclic apparemment)

        # Noms des joueurs
        players_elements = card.find_all("div", class_="scoreboard_contestantLabel")
        player1 = players_elements[0].text.strip() if len(players_elements) > 0 else ""
        player2 = players_elements[1].text.strip() if len(players_elements) > 1 else ""

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
            else:  
                player1_full = player1
                player2_full = player2
        else:  
            player1_full = player1
            player2_full = player2

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
    logging.error(f"Erreur globale lors de l'extraction: {str(e)}", exc_info=True)
finally:
    logging.info("Bloc finally atteint.")
    # Enregistrement de la page pour le débogage
    if driver and hasattr(driver, 'page_source'):
        page_content_to_save = driver.page_source
        logging.info(f"Contenu de la page récupéré pour page_debug.html (longueur: {len(page_content_to_save)} caractères).")
    else:
        page_content_to_save = "Driver non initialisé ou page_source non disponible."
        logging.warning(page_content_to_save)
        
    with open("page_debug.html", "w", encoding="utf-8") as f:
        f.write(page_content_to_save)
    logging.info("HTML sauvegardé dans page_debug.html")

    # Log du début du fichier page_debug.html
    try:
        with open("page_debug.html", "r", encoding="utf-8") as f_read:
            debug_content_preview = f_read.read(2000) # Lire les premiers 2000 caractères
            logging.info(f"Début du contenu de page_debug.html (aperçu de {len(debug_content_preview)} caractères) :\n{debug_content_preview}")
    except Exception as e_read:
        logging.error(f"Impossible de lire page_debug.html pour l'aperçu : {e_read}")

    # Fermeture du navigateur
    if driver:
        logging.info("Tentative de fermeture du driver...")
        driver.quit()
        logging.info("Driver fermé.")
    else:
        logging.info("Driver non initialisé, pas de fermeture nécessaire.")

logging.info(f"Nombre total de matchs extraits: {len(matches)}")

# Création du DataFrame
df = pd.DataFrame(matches)

# --- Traitement post-extraction et sauvegarde Supabase ---
if not df.empty:
    # Récupérer les joueurs ratings depuis Supabase
    logging.info("Récupération des données Elo depuis Supabase...")
    try:
        elo_response = supabase.table("atp_elo_ratings").select("*").execute()
        if elo_response.data:
            elo_players = elo_response.data
            elo_df = pd.DataFrame(elo_players)
        else:
            logging.warning("Aucune donnée Elo reçue de Supabase. La table 'atp_elo_ratings' est peut-être vide.")
            elo_df = pd.DataFrame(columns=['player'])  # DataFrame vide avec la colonne attendue
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données Elo: {e}")
        elo_df = pd.DataFrame(columns=['player'])

    # Ajoute une liste des noms normalisés
    normalized_players_list = elo_df['player'].apply(lambda x: ' '.join(str(x).replace('\xa0', ' ').split()).lower()).tolist()


    def normalize_name(name):
        return ' '.join(str(name).replace('\xa0', ' ').split()).lower()


    def player_to_tennisabstract_url(player_name_on_site):
        normalized_for_url = str(player_name_on_site).lower()
        normalized_for_url = re.sub(r'[^a-z0-9\s-]', '', normalized_for_url)
        normalized_for_url = normalized_for_url.replace(' ', '').replace('-', '')
        return f"https://www.tennisabstract.com/cgi-bin/player.cgi?p={normalized_for_url}"


    def find_best_slug_url(name, elo_df_local):
        if elo_df_local.empty:
            return player_to_tennisabstract_url(name)

        norm_name_site = normalize_name(name)

        elo_df_local['normalized_player'] = elo_df_local['player'].apply(normalize_name)
        exact_match = elo_df_local[elo_df_local['normalized_player'] == norm_name_site]
        if not exact_match.empty:
            matched_elo_name = exact_match['player'].iloc[0]
            logging.debug(f"Exact match for '{name}' -> '{matched_elo_name}'")
            return player_to_tennisabstract_url(matched_elo_name)

        names_list_elo = elo_df_local['normalized_player'].tolist()
        close_matches = difflib.get_close_matches(norm_name_site, names_list_elo, n=1, cutoff=0.80)

        if close_matches:
            matched_normalized_name = close_matches[0]
            original_elo_name_series = elo_df_local[elo_df_local['normalized_player'] == matched_normalized_name]['player']
            if not original_elo_name_series.empty:
                original_elo_name = original_elo_name_series.iloc[0]
                logging.debug(f"Close match for '{name}' ({norm_name_site}) -> '{original_elo_name}' ({matched_normalized_name})")
                return player_to_tennisabstract_url(original_elo_name)

        logging.warning(f"No close match for '{name}' in Elo DB. Using direct conversion for URL.")
        return player_to_tennisabstract_url(name)


    logging.info("Recherche des correspondances de joueurs et génération des URLs Tennis Abstract...")
    df["player1_url"] = df["player1"].apply(lambda n: find_best_slug_url(n, elo_df))
    df["player2_url"] = df["player2"].apply(lambda n: find_best_slug_url(n, elo_df))

    final_columns = ["date", "heure", "tournoi", "tour", "player1", "player2", "match_url",
                     "player1_url", "player2_url", "scraped_date", "scraped_time"]
    for col in final_columns:
        if col not in df.columns:
            df[col] = None if col.endswith("_url") else ""

    df_for_upload = df[final_columns].copy()
    normalized_players_set = set(elo_df['player'].apply(normalize_name))
    df_for_upload = df_for_upload[
        (df_for_upload["player1"].apply(normalize_name).isin(normalized_players_set)) &
        (df_for_upload["player2"].apply(normalize_name).isin(normalized_players_set))
    ]
    logging.info(f"Matches avec 2 joueurs présents dans la base Elo: {len(df_for_upload)}/{len(df)}")

    logging.info("Suppression des anciens matchs de la table 'upcoming_matches'...")
    try:
        delete_response = supabase.table("upcoming_matches").delete().neq('id', -1).execute()
        logging.info(f"Réponse de la suppression: {delete_response.data if hasattr(delete_response, 'data') else 'Pas de données de réponse'}")
    except Exception as e:
        logging.error(f"Erreur lors de la suppression des anciens matchs: {e}")

    logging.info(f"Insertion de {len(df_for_upload)} nouveaux matchs...")
    data_to_insert = df_for_upload.to_dict(orient='records')

    chunk_size = 100
    for i in range(0, len(data_to_insert), chunk_size):
        chunk = data_to_insert[i:i + chunk_size]
        try:
            insert_response = supabase.table("upcoming_matches").insert(chunk).execute()
            logging.info(f"Chunk {i // chunk_size + 1} inséré. Réponse: {insert_response.data if hasattr(insert_response, 'data') else 'Pas de données de réponse'}")
            if hasattr(insert_response, 'error') and insert_response.error:
                logging.error(f"Erreur Supabase lors de l'insertion du chunk: {insert_response.error}")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion du chunk {i // chunk_size + 1}: {e}")

    logging.info(f"Processus terminé. {len(df_for_upload)} matchs potentiellement traités pour insertion.")
else:
    logging.warning("Aucun match trouvé après scroll, la base de données n'est pas modifiée.")

logging.info("Script terminé.") 