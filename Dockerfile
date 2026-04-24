FROM python:3.11-slim

WORKDIR /app

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code source
COPY . .

# Dossiers de sortie
RUN mkdir -p storage/parquet \
             frontend \
             reporting/output \
             logs

# Point d'entrée : le planificateur tourne en continu
CMD ["python", "scheduler.py"]
