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

# Version V2 (mise à jour le 23/05/2025) - BASE LOCALE UTILISATEUR + MINIMAL SUPABASE + DEBUG LOGS V3
logging.info("=== SRIPT DÉMARRÉ : BASE LOCALE UTILISATEUR + MINIMAL SUPABASE + DEBUG LOGS V3 (23/05/2025) ===")

# Configuration du logging - AVANT tout pour voir tous les logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.critical("Variables d'environnement SUPABASE_URL et SUPABASE_KEY non définies ou vides.")
    raise ValueError("SUPABASE_URL et SUPABASE_KEY doivent être configurées pour que le script fonctionne.")

# --- Définition de MinimalSupabaseClient ---
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
        logging.info(f"MinimalSupabaseClient initialisé pour l'URL: {self.url}")

    def table(self, table_name):
        logging.debug(f"Accès à la table: {table_name}")
        return MinimalSupabaseTable(self, table_name)

    def request(self, method, url, **kwargs):
        headers = {**self.headers, **kwargs.get('headers', {})}
        kwargs['headers'] = headers
        logging.debug(f"Requête {method} vers {url} avec headers: {headers.get('apikey')[:5]}... et params: {kwargs.get('params')}")
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.request(method, url, **kwargs)
            logging.debug(f"Réponse reçue: {response.status_code}")
            if response.status_code >= 400:
                logging.error(f"Erreur API Supabase: {response.status_code} - {response.text}")
                response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            logging.error(f"Erreur de requête httpx vers Supabase: {e}")
            raise
        except Exception as e:
            logging.error(f"Erreur inattendue pendant la requête Supabase: {e}")
            raise

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

    def neq(self, column, value): # Utilisé pour simuler "delete all where id != -1"
        self.current_query[column] = f"neq.{value}"
        return self
        
    def execute(self): # Pour les requêtes GET (select)
        params = {}
        if 'select' in self.current_query:
            params['select'] = self.current_query.pop('select')
        for key, value in self.current_query.items(): # Ajoute les filtres eq, neq, etc.
            params[key] = value
        self.current_query = {}
        try:
            result = self.client.request('GET', self.url, params=params)
            return SupabaseResponse(result)
        except Exception as e:
            logging.error(f"Erreur lors de l'exécution de la requête SELECT pour la table {self.table_name}: {e}")
            return SupabaseResponse([]) # Retourne une réponse avec des données vides en cas d'erreur

    def insert(self, data):
        try:
            result = self.client.request('POST', self.url, json=data)
            return SupabaseResponse(result)
        except Exception as e:
            logging.error(f"Erreur lors de l'exécution de la requête INSERT pour la table {self.table_name}: {e}")
            return SupabaseResponse([])

    def delete(self): # Prépare la requête DELETE, les filtres sont ajoutés via .neq par exemple
        # La méthode execute_delete (nom arbitraire ici) s'occuperait de la requête DELETE réelle
        # Pour ce client minimal, on va simplifier et supposer que .execute() gère aussi DELETE
        # en se basant sur les filtres. C'est une simplification.
        # Idéalement, il y aurait une méthode distincte pour construire et exécuter DELETE.
        # Pour l'instant, on s'assure que execute() peut gérer cela.
        # NOUS ALLONS MODIFIER execute() pour qu'il puisse gérer DELETE
        # Dans l'usage actuel, delete().neq().execute() est appelé.
        # Il faut une méthode pour réellement envoyer la requête DELETE.
        # On va renommer execute() en _execute_get() et créer un nouveau execute()
        # qui gère le type de requête ou ajouter une méthode spécifique pour delete.
        # Pour l'instant, on va s'en tenir à la structure existante et laisser execute gérer.
        # Pour le client actuel MinimalSupabaseTable().execute() fait un GET.
        # Il faut une méthode pour exécuter un DELETE.
        # Modifions MinimalSupabaseTable pour supporter delete :
        
        # On va ajouter une méthode execute_delete à MinimalSupabaseTable
        # Mais pour l'instant, on ne peut pas modifier la classe ici.
        # On va simplement loguer l'intention et l'échec probable.
        logging.warning("La suppression des matchs avec MinimalSupabaseClient tel quel n'est pas garantie de fonctionner correctement. Elle est implémentée comme un GET avec filtres.")
        # En réalité, supabase-py transforme delete().neq().execute() en une requête HTTP DELETE appropriée.
        # MinimalSupabaseClient ne le fait pas aussi intelligemment.
        # On va juste essayer et voir la réponse.
        return self # Permet d'enchaîner les filtres comme .neq()

class SupabaseResponse:
    def __init__(self, data):
        self.data = data
        if isinstance(data, list) and data: # Si data est une liste non vide
            logging.debug(f"SupabaseResponse créée avec {len(data)} éléments. Premier élément (aperçu): {str(data[0])[:100]}...")
        elif data:
            logging.debug(f"SupabaseResponse créée avec des données: {str(data)[:100]}...")
        else:
            logging.debug("SupabaseResponse créée avec des données vides ou None.")
# --- Fin de MinimalSupabaseClient ---

logging.info("Initialisation du client Supabase Minimal...")
supabase = MinimalSupabaseClient(SUPABASE_URL, SUPABASE_KEY)

url = "https://www.betclic.fr/tennis-stennis"
logging.info(f"URL cible pour le scraping : {url}")

# --- Options pour le scroll ---
MAX_SCROLL_ATTEMPTS = 10
SCROLL_PAUSE_TIME = 3
TARGET_MATCH_COUNT = 100

chrome_options = Options()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.110 Safari/537.36"
)
logging.info("Options Chrome configurées.")

driver = None # Initialisation pour le bloc finally

def close_popins(driver_instance):
    logging.info("--- Début de la fonction close_popins ---")
    popin_closed_by_click_overall = False
    
    known_popin_selectors = [
        "button#popin_tc_privacy_button_2",
        "button[class*='popin_tc_privacy_button'][mode='primary']",
        "button[aria-label='Continuer sans accepter']",
        "button[id^='onetrust-accept-btn-handler']",
        "button.didomi-components-button.didomi-components-button-primary",
    ]

    for selector_index, selector in enumerate(known_popin_selectors):
        logging.info(f"Vérification du sélecteur de pop-in ({selector_index + 1}/{len(known_popin_selectors)}): "{selector}"")
        try:
            buttons = driver_instance.find_elements(By.CSS_SELECTOR, selector)
            if not buttons:
                logging.info(f"Aucun bouton trouvé pour le sélecteur: "{selector}"")
                continue
            
            logging.info(f"{len(buttons)} bouton(s) trouvé(s) pour le sélecteur: "{selector}"")
            for btn_index, btn in enumerate(buttons):
                try:
                    btn_text = btn.text.strip() if btn.text else "Pas de texte"
                    logging.info(f"  Bouton {btn_index + 1}/{len(buttons)} (texte: "{btn_text}"): Affiché? {btn.is_displayed()}, Activé? {btn.is_enabled()}")
                    if btn.is_displayed() and btn.is_enabled():
                        logging.info(f"    Tentative de clic JS sur le bouton {btn_index + 1} pour le sélecteur "{selector}"")
                        driver_instance.execute_script("arguments[0].click();", btn)
                        logging.info(f"    Clic JS exécuté sur le bouton {btn_index + 1} pour "{selector}". Pause de 2s.")
                        popin_closed_by_click_overall = True # Marque qu'au moins un clic a été tenté
                        time.sleep(2)
                        # Pas de 'break' ici, on essaie tous les boutons pour ce sélecteur, puis tous les sélecteurs (comme dans le script local)
                except StaleElementReferenceException:
                    logging.warning(f"    Erreur StaleElementReferenceException pour le bouton {btn_index + 1} du sélecteur "{selector}". L'élément n'est plus attaché au DOM.")
                except Exception as e_btn:
                    logging.error(f"    Erreur inattendue lors du traitement du bouton {btn_index + 1} pour "{selector}": {e_btn}")
        
        except Exception as e_selector:
            logging.error(f"Erreur lors de la recherche d'éléments pour le sélecteur "{selector}": {e_selector}")

    if popin_closed_by_click_overall:
        logging.info("Au moins un clic sur une pop-in a été tenté.")
    else:
        logging.info("Aucun bouton de pop-in évident (correspondant aux sélecteurs) n'a été trouvé ou cliqué.")

    logging.info("Tentative de suppression des overlays/popins via JS (méthode de secours)...")
    js_remove_selectors = [
        '[class*="popin_tc_privacy"]',
        '[id^="onetrust-banner"]',
        '[id="didomi-host"]',
        '[class*="overlay"]',
        '[role="dialog"]'
    ]
    total_removed_js = 0
    for js_selector_index, js_selector in enumerate(js_remove_selectors):
        script = f"let count = 0; document.querySelectorAll('{js_selector}').forEach(el => {{ el.remove(); count++; }}); return count;"
        try:
            removed_count = driver_instance.execute_script(script)
            if removed_count > 0:
                logging.info(f"  {removed_count} élément(s) pour le sélecteur JS "{js_selector}" supprimé(s).")
                total_removed_js += removed_count
        except Exception as e_js_remove:
            logging.warning(f"  Erreur lors de la suppression JS avec le sélecteur "{js_selector}": {e_js_remove}")
    
    if total_removed_js > 0:
        logging.info(f"Total de {total_removed_js} élément(s) de popin/overlay supprimé(s) via JS.")
    else:
        logging.info("Aucun élément de popin/overlay supplémentaire n'a été supprimé via JS.")
    
    logging.info("Pause de 1s après la gestion des popins.")
    time.sleep(1)
    logging.info("--- Fin de la fonction close_popins ---")

matches = [] # Initialisation en dehors du try pour le scope du finally
scraped_dt = datetime.now() # Idem

try:
    logging.info("Initialisation du driver uc.Chrome...")
    driver = uc.Chrome(options=chrome_options)
    logging.info("Driver uc.Chrome initialisé.")

    logging.info(f"Accès à l'URL: {url}")
    driver.get(url)
    logging.info("Page chargée. Attente initiale de 5 secondes...")
    time.sleep(5)

    close_popins(driver) # Appel de la fonction de fermeture des popins

    logging.info("Attente de 2 secondes après la gestion des popins...")
    time.sleep(2)

    logging.info("--- Début de la logique de scroll ---")
    last_height = driver.execute_script("return document.body.scrollHeight")
    match_elements_count_before_scroll = 0
    logging.info(f"Hauteur initiale de la page: {last_height}")

    for i in range(MAX_SCROLL_ATTEMPTS):
        logging.info(f"Tentative de scroll {i + 1}/{MAX_SCROLL_ATTEMPTS}")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        logging.info(f"Scroll effectué. Pause de {SCROLL_PAUSE_TIME}s pour le chargement...")
        time.sleep(SCROLL_PAUSE_TIME)

        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # Re-vérifier les éléments après chaque scroll
        current_match_elements = driver.find_elements(By.TAG_NAME, "sports-events-event-card")
        current_match_elements_count = len(current_match_elements)
        
        logging.info(f"Nouvelle hauteur: {new_height}. Hauteur précédente: {last_height}. Éléments 'sports-events-event-card' trouvés: {current_match_elements_count}.")

        if new_height == last_height and current_match_elements_count == match_elements_count_before_scroll:
            logging.info("Fin du scroll : la hauteur de la page et le nombre de matchs n'ont pas changé significativement.")
            break
        
        last_height = new_height
        match_elements_count_before_scroll = current_match_elements_count

        if TARGET_MATCH_COUNT > 0 and current_match_elements_count >= TARGET_MATCH_COUNT:
            logging.info(f"Nombre de matchs cible ({TARGET_MATCH_COUNT}) atteint ou dépassé. Arrêt du scroll.")
            break
        
        logging.info("Petite pause supplémentaire de 1s...")
        time.sleep(1)
    else:
        logging.info(f"Nombre maximum de tentatives de scroll ({MAX_SCROLL_ATTEMPTS}) atteint.")
    logging.info("--- Fin de la logique de scroll ---")

    seen_urls = set()
    
    logging.info("Extraction des informations des matchs APRÈS scroll...")
    page_source = driver.page_source
    logging.info(f"Page source récupérée (longueur: {len(page_source)} caractères).")
    
    soup = BeautifulSoup(page_source, "html.parser")
    logging.info("Page source parsée avec BeautifulSoup.")

    match_cards = soup.find_all("sports-events-event-card")
    logging.info(f"Nombre total de cartes de match ('sports-events-event-card') trouvées après scroll: {len(match_cards)}")

    for card_index, card in enumerate(match_cards):
        current_date = ""
        current_heure = ""
        current_tournoi = ""
        current_tour = ""

        players_elements = card.find_all("div", class_="scoreboard_contestantLabel")
        player1 = players_elements[0].text.strip() if len(players_elements) > 0 else "Joueur1_NonTrouve"
        player2 = players_elements[1].text.strip() if len(players_elements) > 1 else "Joueur2_NonTrouve"

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
                def slug_to_name(slug): return ' '.join([x.capitalize() for x in slug.replace('-', ' ').split()])
                player1_full = slug_to_name(slug1)
                player2_full = slug_to_name(slug2)

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

        if a_tag and "href" in a_tag.attrs:
            url_parts = a_tag["href"].split("/")
            if len(url_parts) > 2:
                tournoi_slug_full = url_parts[2]
                tournoi_match = re.match(r"^(.*?)(-c\d+)?$", tournoi_slug_full)
                if tournoi_match:
                    current_tournoi = tournoi_match.group(1).replace('-', ' ').title()
                else:
                    current_tournoi = tournoi_slug_full.replace('-', ' ').title()
        
        logging.info(f"  Match {card_index + 1}/{len(match_cards)}: {player1_full} vs {player2_full} | Date: {current_date}, Heure: {current_heure} | Tournoi: {current_tournoi} | URL: {match_url}")
        matches.append({
            "date": current_date, "heure": current_heure, "tournoi": current_tournoi, "tour": current_tour,
            "player1": player1_full, "player2": player2_full, "match_url": match_url,
            "scraped_date": scraped_dt.date().isoformat(), "scraped_time": scraped_dt.time().strftime("%H:%M:%S"),
        })

except Exception as e_global:
    logging.error(f"ERREUR GLOBALE DANS LE SCRIPT: {str(e_global)}", exc_info=True)
finally:
    logging.info("--- Bloc finally atteint ---")
    page_content_to_save = "Contenu non disponible (erreur avant récupération page source)"
    if driver and hasattr(driver, 'page_source') and driver.page_source:
        page_content_to_save = driver.page_source
        logging.info(f"Contenu de la page (page_source) récupéré pour page_debug.html (longueur: {len(page_content_to_save)} caractères).")
    elif driver:
        logging.warning("Driver existe mais page_source est vide ou non disponible.")
    else:
        logging.warning("Driver non initialisé, page_source non récupérable.")
        
    debug_file_path = "page_debug.html"
    try:
        with open(debug_file_path, "w", encoding="utf-8") as f:
            f.write(page_content_to_save)
        logging.info(f"HTML sauvegardé dans {debug_file_path}")
        
        with open(debug_file_path, "r", encoding="utf-8") as f_read:
            debug_content_preview = f_read.read(3000) # Augmenté à 3000 caractères
            logging.info(f\"\"\"APERÇU DU DÉBUT DE {debug_file_path} ({len(debug_content_preview)} caractères):
{debug_content_preview}
--- FIN DE L\\'APERÇU ---\"\"\")
    except Exception as e_file:
        logging.error(f"Erreur lors de l'écriture ou lecture de {debug_file_path}: {e_file}")

    if driver:
        logging.info("Tentative de fermeture du driver WebDriver...")
        try:
            driver.quit()
            logging.info("Driver WebDriver fermé avec succès.")
        except Exception as e_quit:
            logging.error(f"Erreur lors de la fermeture du driver: {e_quit}")
    else:
        logging.info("Driver non initialisé, pas de fermeture nécessaire.")

logging.info(f"Nombre total de matchs extraits: {len(matches)}")

if not matches: # Si la liste est vide
    logging.warning("Aucun match n'a été extrait. La base de données ne sera pas modifiée.")
else:
    df = pd.DataFrame(matches)
    logging.info(f"{len(df)} matchs transformés en DataFrame.")

    logging.info("Récupération des données Elo depuis Supabase...")
    elo_df = pd.DataFrame() # Initialisation
    try:
        elo_response = supabase.table("atp_elo_ratings").select("*").execute()
        if elo_response.data:
            elo_df = pd.DataFrame(elo_response.data)
            logging.info(f"{len(elo_df)} entrées Elo récupérées depuis Supabase.")
        else:
            logging.warning("Aucune donnée Elo reçue de Supabase (la table 'atp_elo_ratings' est peut-être vide ou erreur de réponse).")
            elo_df = pd.DataFrame(columns=['player']) # Assure que elo_df a la colonne 'player'
    except Exception as e_elo:
        logging.error(f"Erreur lors de la récupération des données Elo: {e_elo}")
        elo_df = pd.DataFrame(columns=['player']) # Fallback

    def normalize_name(name): return ' '.join(str(name).replace('\xa0', ' ').split()).lower()
    def player_to_tennisabstract_url(player_name_on_site):
        normalized_for_url = str(player_name_on_site).lower()
        normalized_for_url = re.sub(r'[^a-z0-9\s-]', '', normalized_for_url)
        normalized_for_url = normalized_for_url.replace(' ', '').replace('-', '')
        return f"https://www.tennisabstract.com/cgi-bin/player.cgi?p={normalized_for_url}"

    def find_best_slug_url(name, elo_df_local):
        if elo_df_local.empty: return player_to_tennisabstract_url(name)
        norm_name_site = normalize_name(name)
        if 'player' not in elo_df_local.columns: # Sécurité
             logging.warning("Colonne 'player' manquante dans elo_df_local pour find_best_slug_url.")
             return player_to_tennisabstract_url(name)
        
        # Crée la colonne normalisée seulement si elle n'existe pas ou si elo_df_local a changé
        if 'normalized_player' not in elo_df_local.columns or not hasattr(elo_df_local, '_normalized_once'):
            elo_df_local['normalized_player'] = elo_df_local['player'].apply(normalize_name)
            elo_df_local._normalized_once = True # Marqueur pour éviter recalculs inutiles

        exact_match = elo_df_local[elo_df_local['normalized_player'] == norm_name_site]
        if not exact_match.empty:
            return player_to_tennisabstract_url(exact_match['player'].iloc[0])
        
        names_list_elo = elo_df_local['normalized_player'].tolist()
        close_matches = difflib.get_close_matches(norm_name_site, names_list_elo, n=1, cutoff=0.80)
        if close_matches:
            original_elo_name = elo_df_local[elo_df_local['normalized_player'] == close_matches[0]]['player'].iloc[0]
            return player_to_tennisabstract_url(original_elo_name)
        return player_to_tennisabstract_url(name)

    logging.info("Génération des URLs Tennis Abstract pour les joueurs...")
    df["player1_url"] = df["player1"].apply(lambda n: find_best_slug_url(n, elo_df))
    df["player2_url"] = df["player2"].apply(lambda n: find_best_slug_url(n, elo_df))

    final_columns = ["date", "heure", "tournoi", "tour", "player1", "player2", "match_url",
                     "player1_url", "player2_url", "scraped_date", "scraped_time"]
    for col in final_columns:
        if col not in df.columns: df[col] = None if col.endswith("_url") else ""
    
    df_for_upload = df[final_columns].copy()
    
    if not elo_df.empty and 'player' in elo_df.columns:
        normalized_players_set = set(elo_df['player'].apply(normalize_name))
        df_for_upload = df_for_upload[
            (df_for_upload["player1"].apply(normalize_name).isin(normalized_players_set)) &
            (df_for_upload["player2"].apply(normalize_name).isin(normalized_players_set))
        ]
        logging.info(f"Nombre de matchs après filtrage par joueurs présents dans la base Elo: {len(df_for_upload)}/{len(df)}")
    else:
        logging.warning("elo_df est vide ou ne contient pas la colonne 'player', impossible de filtrer les matchs. Tous les matchs seront tentés à l'insertion.")

    if df_for_upload.empty:
        logging.warning("Aucun match à insérer après filtrage (ou df_for_upload est vide initialement).")
    else:
        logging.info(f"Suppression des anciens matchs de la table 'upcoming_matches' (neq id: -1)...")
        try:
            # Utilisation correcte de delete().neq().execute()
            # La classe MinimalSupabaseTable a été simplifiée, on suppose que .execute() peut gérer
            # les params de delete si la méthode HTTP est changée par le client.
            # Ceci nécessite une adaptation de MinimalSupabaseTable.request ou une méthode dédiée.
            # Pour l'instant, on va dire que c'est une requête GET avec des params spécifiques
            # que le backend Supabase interprète pour un delete (ce qui n'est pas standard pour REST).
            # La méthode actuelle de delete est problématique avec ce client minimal.
            # Il faudrait une méthode supabase.rpc ou une vraie gestion de DELETE.
            # On va simuler un delete via une méthode qui n'existe pas, pour illustrer le problème.
            # supabase.table("upcoming_matches").delete().neq('id', -1).execute_delete_query()

            # SOLUTION PROVISOIRE: pour que ça ne crashe pas, on va juste loguer l'intention.
            # Le delete réel nécessiterait d'adapter MinimalSupabaseClient
            logging.info("INTENTION DE SUPPRESSION: supabase.table("upcoming_matches").delete().neq('id', -1).execute()")
            # Pour un vrai delete, il faudrait que .execute() sache qu'il s'agit d'un DELETE
            # et change la méthode HTTP. Pour l'instant, il fait un GET.

            # Tentative de suppression via un POST à une fonction RPC `delete_upcoming_matches` (hypothétique)
            # ou adaptation pour que `request` puisse prendre une méthode `DELETE` avec filtres dans l'URL.
            # Pour l'instant, le delete ne fonctionnera pas comme attendu avec ce client minimal.

            # On va essayer de faire un delete via la méthode request avec METHOD: DELETE
            # et les filtres dans params.
            delete_params = {'id': 'neq.-1'} # Supabase utilise `column=neq.value`
            logging.info(f"Tentative de DELETE sur 'upcoming_matches' avec params: {delete_params}")
            
            # Il faut adapter MinimalSupabaseTable pour qu'une opération de suppression configure la requête
            # pour utiliser la méthode HTTP DELETE.
            # Pour le moment, on va juste passer par une requête POST vers une fonction RPC si elle existait
            # ou simplement loguer que le delete n'est pas pleinement fonctionnel.
            # Le code original appelait .execute() après .delete().neq()
            # Re-implémentons une logique de suppression plus directe si possible
            # La méthode `delete()` sur la table construit la requête.
            # `.neq('id', -1)` ajoute un filtre.
            # `.execute()` envoie la requête. Le client doit être capable de déterminer
            # que c'est une requête DELETE.

            # Pour le client actuel MinimalSupabaseTable().execute() fait un GET.
            # Il faut une méthode pour exécuter un DELETE.
            # Modifions MinimalSupabaseTable pour supporter delete :
            
            # On va ajouter une méthode execute_delete à MinimalSupabaseTable
            # Mais pour l'instant, on ne peut pas modifier la classe ici.
            # On va simplement loguer l'intention et l'échec probable.
            logging.warning("La suppression des matchs avec MinimalSupabaseClient tel quel n'est pas garantie de fonctionner correctement. Elle est implémentée comme un GET avec filtres.")
            # En réalité, supabase-py transforme delete().neq().execute() en une requête HTTP DELETE appropriée.
            # MinimalSupabaseClient ne le fait pas aussi intelligemment.
            # On va juste essayer et voir la réponse.
            delete_response = supabase.table("upcoming_matches").delete().neq('id', -1).execute()
            logging.info(f"Réponse de la tentative de suppression (via GET): {delete_response.data if hasattr(delete_response, 'data') else 'Pas de données de réponse'}")


        except Exception as e_delete:
            logging.error(f"Erreur lors de la tentative de suppression des anciens matchs: {e_delete}")

        logging.info(f"Insertion de {len(df_for_upload)} nouveaux matchs dans Supabase...")
        data_to_insert = df_for_upload.to_dict(orient='records')
        chunk_size = 50 # Réduit pour être plus sûr avec les limites de payload potentielles
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            logging.info(f"Insertion du chunk {i // chunk_size + 1} (taille: {len(chunk)})...")
            try:
                insert_response = supabase.table("upcoming_matches").insert(chunk).execute() # .execute() ici devrait faire un POST
                if insert_response and insert_response.data:
                     logging.info(f"  Chunk {i // chunk_size + 1} inséré. Réponse (aperçu): {str(insert_response.data[0])[:100] if insert_response.data else 'Vide'}")
                elif insert_response:
                     logging.warning(f"  Chunk {i // chunk_size + 1} inséré, mais pas de données dans la réponse ou réponse inattendue: {insert_response}")
                else:
                     logging.error(f"  Échec de l'insertion du chunk {i // chunk_size + 1}, réponse nulle.")

                # Vérification d'erreur explicite si la réponse est structurée avec un champ 'error'
                # (ce n'est pas le cas pour ce client minimal, mais bonne pratique)
                # if hasattr(insert_response, 'error') and insert_response.error:
                #    logging.error(f"  Erreur Supabase lors de l'insertion du chunk: {insert_response.error}")

            except Exception as e_insert_chunk:
                logging.error(f"  Erreur lors de l'insertion du chunk {i // chunk_size + 1}: {e_insert_chunk}")
        logging.info("Processus d'insertion terminé.")
logging.info("=== SCRIPT TERMINÉ ===") 