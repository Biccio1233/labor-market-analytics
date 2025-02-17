FROM apache/superset:4.1.1

# Installa le dipendenze necessarie
USER root
RUN pip install flask-cors psycopg2-binary redis

# Copia l'estensione Eurostat
COPY ./extensions/eurostat /app/superset/extensions/eurostat/

# Copia il file di configurazione
COPY ./superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
