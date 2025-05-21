from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import time
from webdriver_manager.chrome import ChromeDriverManager
import datetime
import os
from supabase import create_client, Client, __version__ as supabase_py_version
from supabase.lib.client_options import ClientOptions, AuthClientOptions
import httpx
from dotenv import load_dotenv
import re
import csv
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

urls_to_scrape = []
with open('atp_elo_ratings_rows.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)  # saute l'en-tête
    for row in reader:
        if row and row[0].startswith("http"):
            urls_to_scrape.append(row[0])

def normalize_column(col):
    # Enlève espaces, met tout en minuscule, garde lettres/chiffres/_ et %
    return re.sub(r'[^a-zA-Z0-9_%]', '', col).lower()

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.critical("SUPABASE_URL and SUPABASE_KEY environment variables are not set or empty.")
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set for the script to run.")

# Prepare headers for the custom httpx client
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "X-Client-Info": f"supabase-py/{supabase_py_version}",
}

# Use the same default timeout as gotrue-py (dependency of supabase-py)
default_gotrue_timeout = httpx.Timeout(10.0, connect=5.0)

# Create a custom httpx client instance
custom_httpx_client = httpx.Client(
    headers=headers,
    trust_env=False,  # Explicitly disable proxy environment variables
    timeout=default_gotrue_timeout
)

# Configure Supabase client options to use our custom httpx client for auth
auth_options = AuthClientOptions(http_client=custom_httpx_client)
client_options = ClientOptions(auth=auth_options)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, client_options)
    logging.info("Supabase client created successfully with custom httpx client (trust_env=False).")
except Exception as e:
    # Log the detailed error and re-raise to ensure visibility if creation still fails
    logging.error(f"Failed to create Supabase client even with custom httpx client: {e}", exc_info=True)
    raise

def clean_nbsp(text):
    return text.replace('\xa0', ' ')

tables = {
    "recent_results": "recent-results",
    "career_splits": "career-splits",
    "last52_splits": "last52-splits",
    "head_to_head": "head-to-heads",
    "pbp_points": "pbp-points",
    "pbp_games": "pbp-games",
    "winners_errors": "winners-errors"
}

# Fonction d'insertion dans supabase
def insert_df(table_name, df):
    # Récupère dynamiquement les colonnes de la table sur Supabase
    resp = supabase.table(table_name).select("*").limit(1).execute()
    if not resp.data:
        # Si la table est vide, on prend les clés du DataFrame actuel
        table_columns = set(normalize_column(col) for col in df.columns)
    else:
        table_columns = set(normalize_column(col) for col in resp.data[0].keys())
    # Ne garde que les colonnes qui existent en base
    filtered_cols = [col for col in df.columns if normalize_column(col) in table_columns]
    filtered_df = df[filtered_cols]
    # Renommer les colonnes du DataFrame pour qu'elles correspondent à la normalisation
    filtered_df.columns = [normalize_column(col) for col in filtered_df.columns]
    data = filtered_df.to_dict(orient='records')
    for chunk_start in range(0, len(data), 100):
        chunk = data[chunk_start:chunk_start+100]
        supabase.table(table_name).insert(chunk).execute()

for player_url in urls_to_scrape:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(player_url)
    time.sleep(3)  # On attend que tout le JS charge

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    scraped_at = datetime.date.today().isoformat()

    for key, table_id in tables.items():
        table = soup.find("table", id=table_id)
        # Si la table n'est pas trouvée et que l'id se termine par '-splits', on tente avec l'id alternatif
        if not table and table_id.endswith("-splits"):
            alt_id = f"{table_id}-chall"
            table_alt = soup.find("table", id=alt_id)
            if table_alt:
                print(f"Table {table_id} non trouvée, mais table alternative {alt_id} trouvée et utilisée")
                table = table_alt
        if not table:
            with open("error.txt", "a") as f:
                f.write(f"{datetime.datetime.now()} - Table {table_id} non trouvée pour {player_url}\n")
            print(f"Table {table_id} non trouvée")
            continue
        headers = [clean_nbsp(th.get_text(" ", strip=True)) for th in table.find("thead").find_all("th")]
        rows = []
        for tr in table.find("tbody").find_all("tr"):
            cells = [clean_nbsp(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
            if cells:
                rows.append(cells)
        df = pd.DataFrame(rows, columns=headers)
        def make_columns_unique(cols):
            counts = {}
            result = []
            for c in cols:
                if c in counts:
                    counts[c] += 1
                    result.append(f"{c}_{counts[c]}")
                else:
                    counts[c] = 0
                    result.append(c)
            return result

        df.columns = make_columns_unique(df.columns)
        # Remove empty or anonymous columns
        df = df.loc[:, df.columns != '']
        df.columns = [col.lower() for col in df.columns]
        df['scraped_at'] = scraped_at
        df['player_slug'] = player_url
        print(df)
        print(f"Table {key} ({len(df)} lignes)")
        try:
            # Supprime les anciennes lignes de ce joueur pour cette table
            supabase.table(key).delete().eq('player_slug', player_url).execute()
            insert_df(key, df)
        except Exception as e:
            with open("error.txt", "a") as f:
                f.write(f"{datetime.datetime.now()} - Erreur BDD pour table {key} et url {player_url} : {str(e)}\n") 