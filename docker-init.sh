#!/bin/bash

# Attendi che il database sia pronto (se necessario)
echo "Initializing..."

# Inizializza il database
superset db upgrade

# Crea l'admin user se non esiste
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password "${ADMIN_PASSWORD:-admin}" || true

# Inizializza
superset init

# Avvia Superset
echo "Starting Superset..."
gunicorn -b 0.0.0.0:8088 \
    --workers 3 \
    --timeout 60 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
