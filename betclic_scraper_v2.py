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

# Version V2 (mise à jour le 21/05/2025) - adapté à la nouvelle structure du site Betclic

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

# --- Configurer Chrome ---
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

driver.get(url)
logging.info("Page chargée. Attente initiale de 5 secondes...")
time.sleep(5)

# --- Extraire les données JSON des matchs ---
logging.info("Extraction des données JSON des matchs...")
page_source = driver.page_source
soup = BeautifulSoup(page_source, "html.parser")

# Chercher les données JSON embarquées dans la page
matches = []
scraped_dt = datetime.now()
seen_urls = set()

try:
    # Trouver tous les matchs de tennis dans le JSON embarqué dans la page
    # Le format a changé, les matchs sont maintenant stockés dans un objet JSON sur la page
    json_data = None
    
    # Recherche des données JSON dans différents patterns possibles
    script_patterns = [
        re.compile(r'({"matches":\[.*?\]})'),
        re.compile(r'__initial_state__\s*=\s*({.*?});'),
        re.compile(r'window\.__INITIAL_STATE__\s*=\s*({.*?});')
    ]
    
    for pattern in script_patterns:
        try:
            for script in soup.find_all("script"):
                if script.string:
                    match = pattern.search(script.string)
                    if match:
                        try:
                            json_text = match.group(1)
                            potential_data = json.loads(json_text)
                            if isinstance(potential_data, dict) and ('matches' in potential_data or 'match' in potential_data):
                                json_data = potential_data
                                logging.info("Données JSON des matchs trouvées!")
                                break
                        except json.JSONDecodeError:
                            continue
            if json_data:
                break
        except Exception as e:
            logging.warning(f"Erreur avec pattern {pattern}: {e}")
    
    # Si nous n'avons pas pu extraire avec un pattern spécifique, essayons une approche plus générique
    if not json_data:
        logging.info("Tentative d'extraction générique des données JSON...")
        match_regex = re.compile(r'"matchId":"[^"]+","name":"([^"]+)","matchDateUtc":"([^"]+)"')
        match_data = match_regex.findall(page_source)
        
        # Si on trouve des matchs avec cette méthode, créons une structure similaire
        if match_data:
            logging.info(f"Extrait {len(match_data)} matchs via regex")
            
            for match_name, match_date in match_data:
                # Extraction des noms des joueurs
                name_parts = match_name.split(" - ")
                if len(name_parts) == 2:
                    player1 = name_parts[0].strip()
                    player2 = name_parts[1].strip()
                    
                    # Création d'une URL fictive basée sur les noms (puisque nous ne pouvons pas extraire les vraies URLs)
                    player1_slug = re.sub(r'[^a-z0-9]', '-', player1.lower())
                    player2_slug = re.sub(r'[^a-z0-9]', '-', player2.lower())
                    match_url = f"https://www.betclic.fr/tennis-stennis/{player1_slug}-vs-{player2_slug}-m{random.randint(10000, 99999)}"
                    
                    # Extraire la date et l'heure
                    try:
                        match_datetime = datetime.fromisoformat(match_date.replace('Z', '+00:00'))
                        current_date = match_datetime.strftime("%d/%m")
                        current_heure = match_datetime.strftime("%H:%M")
                    except:
                        current_date = ""
                        current_heure = ""
                    
                    # Chercher le tournoi
                    tourney_match = re.search(r'"competition":\{"id":"[^"]+","name":"([^"]+)"', page_source)
                    current_tournoi = tourney_match.group(1) if tourney_match else ""
                    
                    matches.append({
                        "date": current_date,
                        "heure": current_heure,
                        "tournoi": current_tournoi,
                        "tour": "",
                        "player1": player1,
                        "player2": player2,
                        "match_url": match_url,
                        "scraped_date": scraped_dt.date().isoformat(),
                        "scraped_time": scraped_dt.time().strftime("%H:%M:%S"),
                    })
        else:
            logging.warning("Aucun match trouvé via regex dans la page.")
    else:
        # Traitement des données JSON si elles ont été trouvées
        if 'matches' in json_data:
            tennis_matches = json_data.get('matches', [])
            logging.info(f"Nombre de matchs trouvés dans le JSON: {len(tennis_matches)}")
            
            for match in tennis_matches:
                match_name = match.get('name', '')
                name_parts = match_name.split(" - ")
                if len(name_parts) != 2:
                    continue
                    
                player1 = name_parts[0].strip()
                player2 = name_parts[1].strip()
                
                match_date_utc = match.get('matchDateUtc', '')
                current_date = ""
                current_heure = ""
                
                try:
                    match_datetime = datetime.fromisoformat(match_date_utc.replace('Z', '+00:00'))
                    current_date = match_datetime.strftime("%d/%m")
                    current_heure = match_datetime.strftime("%H:%M")
                except:
                    pass
                
                # Trouver le tournoi
                competition = match.get('competition', {})
                current_tournoi = competition.get('name', '') if competition else ""
                
                # Générer l'URL du match
                match_id = match.get('matchId', '')
                match_url = f"https://www.betclic.fr/tennis-stennis/match-{match_id}"
                
                matches.append({
                    "date": current_date,
                    "heure": current_heure,
                    "tournoi": current_tournoi,
                    "tour": "",
                    "player1": player1,
                    "player2": player2,
                    "match_url": match_url,
                    "scraped_date": scraped_dt.date().isoformat(),
                    "scraped_time": scraped_dt.time().strftime("%H:%M:%S"),
                })
    
    logging.info(f"Nombre total de matchs extraits: {len(matches)}")
    
except Exception as e:
    logging.error(f"Erreur lors de l'extraction: {str(e)}", exc_info=True)
finally:
    # Enregistrement de la page pour le débogage
    with open("page_debug.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source if 'driver' in locals() and driver.page_source else "No page source available")
    logging.info("HTML sauvegardé dans page_debug.html")

    # Fermeture du navigateur
    driver.quit()

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
    logging.warning("Aucun match trouvé après extraction, la base de données n'est pas modifiée.")

logging.info("Script terminé.") 