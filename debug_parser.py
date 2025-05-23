from bs4 import BeautifulSoup

with open('page_debug_scraperapi.html', 'r') as f:
    content = f.read()

soup = BeautifulSoup(content, 'html.parser')
cards = soup.find_all('sports-events-event-card')
print(f'BeautifulSoup trouve: {len(cards)} cartes')

# Vérifier avec différents sélecteurs
cards2 = soup.select('sports-events-event-card')
print(f'CSS selector trouve: {len(cards2)} cartes')

# Vérifie si certaines cartes sont dans des containers différents
all_tags = soup.find_all(lambda tag: tag.name and 'sports-events-event-card' in tag.name)
print(f'Tous les tags similaires: {len(all_tags)}')

# Vérifie les premiers éléments pour comprendre la structure
print(f'\nPremières 5 cartes:')
for i, card in enumerate(cards[:5]):
    players = card.find_all("div", class_="scoreboard_contestantLabel")
    if len(players) >= 2:
        player1 = players[0].get_text(strip=True)
        player2 = players[1].get_text(strip=True)
        print(f'Carte {i+1}: {player1} vs {player2}')
    else:
        print(f'Carte {i+1}: Structure inattendue')

# Vérifie si il y a des doublons ou des cartes vides
valid_cards = 0
seen_urls = set()
for card in cards:
    a_tag = card.find("a")
    if a_tag and a_tag.get("href"):
        match_url = "https://www.betclic.fr" + a_tag["href"]
        if match_url not in seen_urls:
            seen_urls.add(match_url)
            valid_cards += 1

print(f'\nCartes valides (avec URL unique): {valid_cards}')
print(f'URLs uniques trouvées: {len(seen_urls)}')