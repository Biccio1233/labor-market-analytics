#!/bin/bash

# Attendi che il database sia pronto
echo "Waiting for database..."
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
  sleep 1
done
echo "Database is ready!"

# Inizializza il database se necessario
superset db upgrade

# Crea l'utente admin se non esiste
superset fab create-admin \
    --username admin \
    --firstname admin \
    --lastname admin \
    --email admin@localhost \
    --password admin || true

# Inizializza Superset
superset init

# Avvia Superset
gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 10 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
