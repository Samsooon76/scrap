FROM python:3.11

# Installer Chrome et les dépendances requises
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    libxss1 \
    libnss3 \
    libnspr4 \
    libasound2 \
    libxkbcommon0 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libu2f-udev \
    libvulkan1

# Installer Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable

# Créer un répertoire pour l'application
WORKDIR /app

# Copier les fichiers nécessaires
COPY requirements.txt .
COPY betclic_scraper.py .
COPY player_stats_scraper.py .
COPY atp_elo_ratings_rows.csv .

# Installer les dépendances Python
RUN pip install --upgrade pip && pip install -r requirements.txt

# Commande par défaut (à remplacer par le script spécifique dans Render)
CMD ["python", "betclic_scraper.py"] 