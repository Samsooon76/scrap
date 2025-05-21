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

# Version V2 (créée le 21/05/2025) - utilise une implémentation minimale pour éviter les erreurs d'import

# Configuration du logging
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
driver = uc.Chrome(options=chrome_options)


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
        try:
            buttons = driver_instance.find_elements(By.CSS_SELECTOR, selector)
            for btn in buttons:
                if btn.is_displayed() and btn.is_enabled():
                    logging.info(f"Click sur popin via selector: {selector}")
                    driver_instance.execute_script("arguments[0].click();", btn)  # Clic JS plus robuste
                    popin_closed_by_click = True
                    time.sleep(2)  # Attendre que la popin disparaisse
                    # break # Sortir si un bouton a été cliqué
        except Exception as e:
            logging.debug(f"Erreur en cliquant sur {selector}: {e}")
        # if popin_closed_by_click:
        #     break

    if popin_closed_by_click:
        logging.info("Popin fermée par clic.")
    else:
        logging.info("Aucune popin évidente trouvée pour clic, ou échec du clic.")

    # Forcer la suppression des overlays/popins si toujours présents
    # Cibler des conteneurs de popins connus
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


matches = []
seen_urls = set()
scraped_dt = datetime.now()

try:
    logging.info("Extraction des informations des matchs après scroll...")
    # Il est crucial de récupérer le page_source APRÈS tous les scrolls
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    match_cards = soup.find_all("sports-events-event-card")
    logging.info(f"Nombre total de cartes de match trouvées après scroll: {len(match_cards)}")

    # La limite des 40 premiers matchs est enlevée, car on veut tout ce qui a été chargé
    # match_cards = match_cards[:40] # Supprimé

    for card_index, card in enumerate(match_cards):
        current_date = ""  # Réinitialiser pour chaque carte
        current_heure = ""  # Réinitialiser pour chaque carte
        current_tournoi = ""  # Réinitialiser pour chaque carte
        current_tour = ""  # Réinitialiser pour chaque carte (toujours vide pour Betclic apparemment)

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
                # Logique améliorée pour diviser les noms, gère les noms composés
                # Exemple: 'alex-de-minaur-vs-jan-lennard-struff'
                # On cherche le 'vs' implicite. Le slug est 'joueur1-joueur2'
                # On ne peut pas juste diviser par 2 si un joueur a un nom composé.
                # Cependant, Betclic semble utiliser le même nombre de tirets pour chaque joueur
                # Ex: 'alex-de-minaur' (2 tirets) et 'jan-lennard-struff' (2 tirets)
                # Si les noms sont p.ex. 'taylor-fritz' et 'sebastian-baez', on divise par 2.
                # Pour l'instant, on garde la division par 2, mais il faut être conscient de sa fragilité.
                # Une meilleure approche serait d'avoir une liste de noms connus ou une heuristique plus fine.
                n_parts = len(parts)
                # Si le nombre de parties est impair, c'est difficile, on fait une approximation
                # ex: novak-djokovic-carlos-alcaraz -> djokovic est partagé.
                # Le slug betclic est souvent 'nom1part1-nom1part2-nom2part1-nom2part2'
                # Donc diviser au milieu est généralement correct.
                split_point = n_parts // 2
                slug1 = '-'.join(parts[:split_point])
                slug2 = '-'.join(parts[split_point:])


                def slug_to_name(slug):
                    return ' '.join([x.capitalize() for x in slug.replace('-', ' ').split()])


                player1_full = slug_to_name(slug1)
                player2_full = slug_to_name(slug2)
            else:  # Fallback si le regex ne match pas, mais qu'on a les noms courts
                player1_full = player1
                player2_full = player2
        else:  # Fallback si pas de a_tag
            player1_full = player1
            player2_full = player2

        # Date et heure
        event_info_time = card.find("div", class_="event_infoTime")
        if event_info_time and event_info_time.text.strip():
            date_heure_text = event_info_time.text.strip()
            # Gérer "Auj.", "Dem." et les dates complètes
            if "Auj." in date_heure_text or "Dem." in date_heure_text:  # ex: "Auj. 14:00" ou "Dem. Jeu. 23:00"
                parts = date_heure_text.split()
                if len(parts) >= 2:
                    current_date = parts[0]  # Peut être "Auj." ou "Dem."
                    current_heure = parts[-1]  # L'heure est toujours la dernière partie
            else:  # ex: "Jeu. 01/01 15:00"
                parts = date_heure_text.split()
                if len(parts) == 3:  # "Jeu. 01/01 15:00"
                    current_date = f"{parts[0]} {parts[1]}"  # "Jeu. 01/01"
                    current_heure = parts[2]  # "15:00"
                elif len(parts) == 2:  # Cas "01/01 15:00" (moins probable sans jour)
                    current_date = parts[0]
                    current_heure = parts[1]

        # Extraction du nom du tournoi depuis l'URL (généralement plus fiable)
        if a_tag and "href" in a_tag.attrs:
            url_parts = a_tag["href"].split("/")
            if len(url_parts) > 2:  # e.g., /fr/tennis/atp-rome-c33/...
                tournoi_slug_full = url_parts[2]  # "atp-rome-c33" ou "roland-garros- 프랑스 오픈-c123"
                # Prendre tout avant le premier "-c" suivi de chiffres
                tournoi_match = re.match(r"^(.*?)(-c\d+)?$", tournoi_slug_full)
                if tournoi_match:
                    tournoi_slug = tournoi_match.group(1)
                    current_tournoi = tournoi_slug.replace('-', ' ').title()
                else:  # Fallback si le regex ne match pas
                    current_tournoi = tournoi_slug_full.replace('-', ' ').title()

        logging.info(
            f"Match {card_index + 1}/{len(match_cards)}: {player1_full} vs {player2_full} | Date: {current_date}, Heure: {current_heure} | Tournoi: {current_tournoi} | URL: {match_url}")

        matches.append({
            "date": current_date,
            "heure": current_heure,
            "tournoi": current_tournoi,
            "tour": current_tour,  # Reste vide car non trouvé sur la page Betclic
            "player1": player1_full,
            "player2": player2_full,
            "match_url": match_url,
            "scraped_date": scraped_dt.date().isoformat(),
            "scraped_time": scraped_dt.time().strftime("%H:%M:%S"),
        })
except Exception as e:
    logging.error(f"Erreur lors de l'extraction: {str(e)}", exc_info=True)
finally:
    # Enregistrement de la page pour le débogage
    with open("page_debug.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source if 'driver' in locals() and driver.page_source else "No page source available")
    logging.info("HTML sauvegardé dans page_debug.html")

    # Fermeture du navigateur
    if 'driver' in locals():
        driver.quit()

logging.info(f"Nombre total de matchs extraits: {len(matches)}")

# Création du DataFrame
df = pd.DataFrame(matches)  # Les colonnes seront automatiquement créées à partir des clés du dict

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
        # Tennis Abstract utilise souvent des noms sans accents et en minuscules pour les URLs
        # ex: 'Novak Djokovic' -> 'novakdjokovic'
        # Il faut gérer les cas comme 'Alex De Minaur' -> 'alexdeminaur'

        # Simple normalisation pour l'URL Tennis Abstract
        normalized_for_url = str(player_name_on_site).lower()
        # Retirer accents (simple pour l'instant, une librairie comme `unidecode` serait mieux)
        # Pour les besoins de l'URL, on retire juste les caractères non alphanumériques (sauf espace qu'on retire ensuite)
        normalized_for_url = re.sub(r'[^a-z0-9\s-]', '', normalized_for_url)
        # Remplacer les espaces et tirets par rien
        normalized_for_url = normalized_for_url.replace(' ', '').replace('-', '')
        return f"https://www.tennisabstract.com/cgi-bin/player.cgi?p={normalized_for_url}"


    def find_best_slug_url(name, elo_df_local):
        if elo_df_local.empty:
            # Si elo_df est vide, on ne peut pas faire de matching intelligent,
            # on utilise la conversion directe du nom du site.
            return player_to_tennisabstract_url(name)

        norm_name_site = normalize_name(name)

        # 1. Tentative de match exact (après normalisation)
        elo_df_local['normalized_player'] = elo_df_local['player'].apply(normalize_name)
        exact_match = elo_df_local[elo_df_local['normalized_player'] == norm_name_site]
        if not exact_match.empty:
            matched_elo_name = exact_match['player'].iloc[0]
            logging.debug(f"Exact match for '{name}' -> '{matched_elo_name}'")
            return player_to_tennisabstract_url(matched_elo_name)

        # 2. Tentative de match approché avec difflib
        names_list_elo = elo_df_local['normalized_player'].tolist()
        close_matches = difflib.get_close_matches(norm_name_site, names_list_elo, n=1,
                                                  cutoff=0.80)  # Cutoff un peu plus strict

        if close_matches:
            # Retrouver le nom original de la base Elo à partir du nom normalisé matché
            matched_normalized_name = close_matches[0]
            original_elo_name_series = elo_df_local[elo_df_local['normalized_player'] == matched_normalized_name][
                'player']
            if not original_elo_name_series.empty:
                original_elo_name = original_elo_name_series.iloc[0]
                logging.debug(
                    f"Close match for '{name}' ({norm_name_site}) -> '{original_elo_name}' ({matched_normalized_name})")
                return player_to_tennisabstract_url(original_elo_name)

        # 3. Si aucun match, fallback sur la conversion directe du nom du site
        logging.warning(f"No close match for '{name}' in Elo DB. Using direct conversion for URL.")
        return player_to_tennisabstract_url(name)


    logging.info("Recherche des correspondances de joueurs et génération des URLs Tennis Abstract...")
    df["player1_url"] = df["player1"].apply(lambda n: find_best_slug_url(n, elo_df))
    df["player2_url"] = df["player2"].apply(lambda n: find_best_slug_url(n, elo_df))

    # Assurer l'ordre des colonnes pour la base de données
    final_columns = ["date", "heure", "tournoi", "tour", "player1", "player2", "match_url",
                     "player1_url", "player2_url", "scraped_date", "scraped_time"]
    # S'assurer que toutes les colonnes existent, ajouter celles qui manquent avec None ou ""
    for col in final_columns:
        if col not in df.columns:
            df[col] = None if col.endswith("_url") else ""  # ou pd.NA

    df_for_upload = df[final_columns].copy()
    # Normalise les noms des joueurs
    normalized_players_set = set(elo_df['player'].apply(normalize_name))
    df_for_upload = df_for_upload[
        (df_for_upload["player1"].apply(normalize_name).isin(normalized_players_set)) &
        (df_for_upload["player2"].apply(normalize_name).isin(normalized_players_set))
    ]
    # logging.info(f"Matches avec 2 URLs joueurs valides: {len(df_for_upload)}/{len(df)}")
    logging.info(f"Matches avec 2 joueurs présents dans la base Elo: {len(df_for_upload)}/{len(df)}")

    # Supprime tous les anciens matchs à venir
    logging.info("Suppression des anciens matchs de la table 'upcoming_matches'...")
    try:
        delete_response = supabase.table("upcoming_matches").delete().neq('id',
                                                                          -1).execute()  # neq est un placeholder pour "delete all"
        logging.info(
            f"Réponse de la suppression: {delete_response.data if hasattr(delete_response, 'data') else 'Pas de données de réponse'}")
    except Exception as e:
        logging.error(f"Erreur lors de la suppression des anciens matchs: {e}")

    # Insère les nouveaux matchs (par chunk de 100 pour éviter les timeouts ou limites de payload)
    logging.info(f"Insertion de {len(df_for_upload)} nouveaux matchs...")
    data_to_insert = df_for_upload.to_dict(orient='records')

    chunk_size = 100
    for i in range(0, len(data_to_insert), chunk_size):
        chunk = data_to_insert[i:i + chunk_size]
        try:
            insert_response = supabase.table("upcoming_matches").insert(chunk).execute()
            logging.info(
                f"Chunk {i // chunk_size + 1} inséré. Réponse: {insert_response.data if hasattr(insert_response, 'data') else 'Pas de données de réponse'}")
            if hasattr(insert_response, 'error') and insert_response.error:
                logging.error(f"Erreur Supabase lors de l'insertion du chunk: {insert_response.error}")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion du chunk {i // chunk_size + 1}: {e}")

    logging.info(f"Processus terminé. {len(df_for_upload)} matchs potentiellement traités pour insertion.")
else:
    logging.warning("Aucun match trouvé après scroll, la base de données n'est pas modifiée.")

logging.info("Script terminé.") 