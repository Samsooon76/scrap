# Tennis-Scraper

Ce projet contient deux scripts pour récupérer des données de tennis :
1. `betclic_scraper.py` : scrape les matchs à venir sur Betclic
2. `player_stats_scraper.py` : récupère les statistiques des joueurs sur Tennis Abstract

## Prérequis

- Un compte [Render](https://render.com/)
- Un projet [Supabase](https://supabase.com/) avec les tables requises

## Configuration de Render

### Méthode de déploiement

1. Connectez-vous à votre compte Render
2. Liez votre compte GitHub ou GitLab
3. Importez ce dépôt
4. Les Cron Jobs seront automatiquement configurés via le fichier `render.yaml`

### Variables d'environnement

Configurez ces variables d'environnement dans Render :

- `SUPABASE_URL` : l'URL de votre projet Supabase
- `SUPABASE_KEY` : la clé d'API de votre projet Supabase

## Structure des tables Supabase

### Table `upcoming_matches`

- `id` : int (auto-généré)
- `date` : text
- `heure` : text
- `tournoi` : text
- `tour` : text
- `player1` : text
- `player2` : text
- `match_url` : text
- `player1_url` : text
- `player2_url` : text
- `scraped_date` : text
- `scraped_time` : text

### Table `atp_elo_ratings` (requise pour le mapping des joueurs)

- `id` : int (auto-généré)
- `player` : text (nom du joueur)
- `elo` : float (Elo global)
- `helo` : float (Elo surface dure)
- `celo` : float (Elo terre battue)
- `gelo` : float (Elo gazon)

### Tables de statistiques joueurs

Plusieurs tables sont créées automatiquement pour stocker les statistiques des joueurs, notamment :
- `career_splits`
- `last52_splits`
- `head_to_head`
- etc.

## Fonctionnement des scripts

### betclic_scraper.py

Ce script s'exécute 3 fois par jour (5h, 13h et 21h) et :
1. Scrape les matchs à venir sur Betclic
2. Mappe les noms des joueurs avec la base Elo
3. Génère les URLs Tennis Abstract pour chaque joueur
4. Remplace tous les matchs dans la table `upcoming_matches`

### player_stats_scraper.py

Ce script s'exécute une fois par jour à 3h du matin et :
1. Lit le fichier `atp_elo_ratings_rows.csv` qui contient les URLs des joueurs
2. Visite chaque page de joueur sur Tennis Abstract
3. Extrait différentes statistiques (résultats récents, statistiques par surface, etc.)
4. Met à jour les tables correspondantes dans Supabase
