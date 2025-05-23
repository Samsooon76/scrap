#!/usr/bin/env python3
"""
Script de diagnostic pour Render - Vérifications environnement
"""
import os
import requests
import sys
from datetime import datetime

def main():
    print("=== DIAGNOSTIC RENDER ENVIRONMENT ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Python version: {sys.version}")
    
    # 1. Vérification des variables d'environnement
    print("\n=== ENVIRONMENT VARIABLES ===")
    env_vars = ['SUPABASE_URL', 'SUPABASE_KEY', 'SCRAPERAPI_KEY', 'RENDER']
    
    for var in env_vars:
        value = os.getenv(var)
        if value:
            # Masquer les clés sensibles
            if 'KEY' in var or 'URL' in var:
                display_value = f"{value[:10]}...{value[-4:]}" if len(value) > 14 else "***"
            else:
                display_value = value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: NOT SET")
    
    # 2. Test ScraperAPI basique
    print("\n=== SCRAPERAPI TEST ===")
    scraperapi_key = os.getenv("SCRAPERAPI_KEY", "aae0279e8feccdcfb5b40c85fdd65a66")
    print(f"Using key: {scraperapi_key[:10]}...{scraperapi_key[-4:]}")
    
    test_url = "https://httpbin.org/json"
    scraperapi_endpoint = "http://api.scraperapi.com"
    
    params = {
        'api_key': scraperapi_key,
        'url': test_url,
        'render': 'false'  # Test simple sans rendu
    }
    
    try:
        response = requests.get(scraperapi_endpoint, params=params, timeout=30)
        print(f"✅ ScraperAPI Status: {response.status_code}")
        print(f"✅ Response length: {len(response.text)} chars")
        
        if response.status_code == 200:
            print("✅ ScraperAPI is working")
        else:
            print(f"❌ ScraperAPI error: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ ScraperAPI failed: {e}")
    
    # 3. Test Betclic simple
    print("\n=== BETCLIC SCRAPING TEST ===")
    betclic_url = "https://www.betclic.fr/tennis-stennis"
    
    params = {
        'api_key': scraperapi_key,
        'url': betclic_url,
        'render': 'true',
        'wait': '10000',
        'country_code': 'fr'
    }
    
    try:
        print("Testing ScraperAPI with Betclic...")
        response = requests.get(scraperapi_endpoint, params=params, timeout=60)
        print(f"✅ Betclic Status: {response.status_code}")
        print(f"✅ Content length: {len(response.text)} chars")
        
        if response.status_code == 200:
            # Compter les cartes de match
            content = response.text
            card_count = content.count('sports-events-event-card')
            print(f"✅ Found {card_count} sports-events-event-card elements")
            
            # Vérifier la présence de JSON
            json_matches = '"matches":[' in content
            print(f"✅ JSON data present: {json_matches}")
            
            # Sauvegarder pour debug
            with open('/tmp/debug_betclic.html', 'w', encoding='utf-8') as f:
                f.write(content)
            print("✅ Content saved to /tmp/debug_betclic.html")
            
        else:
            print(f"❌ Betclic error: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Betclic test failed: {e}")
    
    # 4. Vérification Selenium
    print("\n=== SELENIUM AVAILABILITY ===")
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.chrome.options import Options
        print("✅ Selenium imports successful")
        
        # Test Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            driver = uc.Chrome(options=chrome_options)
            driver.get("https://httpbin.org/json")
            print("✅ Chrome/Selenium working")
            driver.quit()
        except Exception as e:
            print(f"❌ Chrome/Selenium failed: {e}")
            
    except ImportError as e:
        print(f"❌ Selenium not available: {e}")
    
    print("\n=== DIAGNOSTIC COMPLETE ===")

if __name__ == "__main__":
    main() 