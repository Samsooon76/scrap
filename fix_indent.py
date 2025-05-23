with open('betclic_scraper_hybrid.py', 'r') as f:
    content = f.read()

# Fix the problematic section by replacing it with correct indentation
old_section = '''        scraperapi_matches = []
        if match_cards:
            scraped_dt = datetime.now()
            seen_urls = set()
            
            for card_index, card in enumerate(match_cards):
                try:
                    # Extract player names
                    players = card.find_all("div", class_="scoreboard_contestantLabel")
                     if len(players) >= 2:
                         player1 = players[0].get_text(strip=True)
                         player2 = players[1].get_text(strip=True)'''

new_section = '''        scraperapi_matches = []
        if match_cards:
            scraped_dt = datetime.now()
            seen_urls = set()
            
            for card_index, card in enumerate(match_cards):
                try:
                    # Extract player names
                    players = card.find_all("div", class_="scoreboard_contestantLabel")
                    if len(players) >= 2:
                        player1 = players[0].get_text(strip=True)
                        player2 = players[1].get_text(strip=True)'''

content = content.replace(old_section, new_section)

# More replacements to fix the rest of the indentation
content = content.replace('                         # Extract match URL', '                        # Extract match URL')
content = content.replace('                         a_tag = card.find("a")', '                        a_tag = card.find("a")')
content = content.replace('                         if a_tag and a_tag.get("href"):', '                        if a_tag and a_tag.get("href"):')
content = content.replace('                             match_url = "https://www.betclic.fr" + a_tag["href"]', '                            match_url = "https://www.betclic.fr" + a_tag["href"]')
content = content.replace('                             ', '                            ')
content = content.replace('                                 ', '                                ')

with open('betclic_scraper_hybrid.py', 'w') as f:
    f.write(content)

print("Fixed indentation issues")