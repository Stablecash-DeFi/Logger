# Utilise l'image Python officielle en tant que base
FROM python:3.9

# Crée le répertoire de travail de l'application
WORKDIR /app

# Copie les fichiers nécessaires dans le conteneur
COPY requirements.txt .
COPY app.py .

# Installe les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Expose le port sur lequel l'application s'exécute
EXPOSE 8080

# Démarre l'application
CMD ["python", "app.py"]
