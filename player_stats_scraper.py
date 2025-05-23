from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import time
from webdriver_manager.chrome import ChromeDriverManager
import datetime
import os
import json
import httpx
from dotenv import load_dotenv
import re
import csv
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Chargement des URLs depuis le CSV
urls_to_scrape = []
try:
    with open('atp_elo_ratings_rows.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # saute l'en-tête
        for row in reader:
            if row and row[0].startswith("http"):
                urls_to_scrape.append(row[0])
    logging.info(f"Loaded {len(urls_to_scrape)} player URLs to scrape")
except Exception as e:
    logging.error(f"Error loading CSV file: {e}")
    urls_to_scrape = []

def normalize_column(col):
    # Enlève espaces, met tout en minuscule, garde lettres/chiffres/_ et %
    return re.sub(r'[^a-zA-Z0-9_%]', '', col).lower()

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
        
    def limit(self, count):
        self.current_query["limit"] = count
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

# Use our minimal implementation directly
logging.info("Using minimal Supabase client implementation...")
supabase = MinimalSupabaseClient(SUPABASE_URL, SUPABASE_KEY)

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
    try:
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
        logging.info(f"Successfully inserted {len(data)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")

def create_chrome_driver():
    """Create Chrome driver optimized for Render environment"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")  # Tennis Abstract works without JS
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--single-process")  # Important for cloud environments
    
    # Detect if running on Render
    is_render = 'RENDER' in os.environ
    if is_render:
        logging.info("Running on Render - using optimized Chrome options")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)  # 30 second timeout
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome driver: {e}")
        raise

# Main scraping loop
successful_scrapes = 0
failed_scrapes = 0

logging.info(f"Starting to scrape {len(urls_to_scrape)} player URLs...")

for i, player_url in enumerate(urls_to_scrape):
    try:
        logging.info(f"Processing player {i+1}/{len(urls_to_scrape)}: {player_url}")
        
        driver = create_chrome_driver()
        driver.get(player_url)
        time.sleep(3)  # Wait for content to load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        scraped_at = datetime.date.today().isoformat()
        player_processed = False

        for key, table_id in tables.items():
            try:
                table = soup.find("table", id=table_id)
                # Si la table n'est pas trouvée et que l'id se termine par '-splits', on tente avec l'id alternatif
                if not table and table_id.endswith("-splits"):
                    alt_id = f"{table_id}-chall"
                    table_alt = soup.find("table", id=alt_id)
                    if table_alt:
                        logging.info(f"Table {table_id} not found, using alternative {alt_id}")
                        table = table_alt
                        
                if not table:
                    logging.warning(f"Table {table_id} not found for {player_url}")
                    continue
                    
                headers = [clean_nbsp(th.get_text(" ", strip=True)) for th in table.find("thead").find_all("th")]
                rows = []
                for tr in table.find("tbody").find_all("tr"):
                    cells = [clean_nbsp(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
                    if cells:
                        rows.append(cells)
                        
                if not rows:
                    logging.warning(f"No data rows found in table {table_id} for {player_url}")
                    continue
                    
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
                
                logging.info(f"Table {key}: {len(df)} rows found")
                
                # Delete old data for this player
                supabase.table(key).delete().eq('player_slug', player_url).execute()
                
                # Insert new data
                insert_df(key, df)
                player_processed = True
                
            except Exception as e:
                logging.error(f"Error processing table {key} for {player_url}: {e}")
                continue
        
        if player_processed:
            successful_scrapes += 1
            logging.info(f"Successfully processed player {i+1}/{len(urls_to_scrape)}")
        else:
            failed_scrapes += 1
            logging.warning(f"Failed to process any tables for player {i+1}/{len(urls_to_scrape)}")
            
    except Exception as e:
        failed_scrapes += 1
        logging.error(f"Error processing player {i+1}/{len(urls_to_scrape)} ({player_url}): {e}")
        # Ensure driver is closed even on error
        try:
            if 'driver' in locals():
                driver.quit()
        except:
            pass
        continue

logging.info(f"=== SCRAPING COMPLETE ===")
logging.info(f"Successful scrapes: {successful_scrapes}")
logging.info(f"Failed scrapes: {failed_scrapes}")
logging.info(f"Success rate: {successful_scrapes}/{len(urls_to_scrape)} ({successful_scrapes/len(urls_to_scrape)*100:.1f}%)") 