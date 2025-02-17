FROM apache/superset:4.1.1

# Installa le dipendenze necessarie
USER root
RUN pip install flask-cors psycopg2-binary redis gunicorn

# Copia l'estensione Eurostat
COPY ./extensions/eurostat /app/superset/extensions/eurostat/

# Copia il file di configurazione
COPY ./superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# Copia lo script di avvio
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

USER superset

EXPOSE 8088

ENTRYPOINT ["/app/docker-entrypoint.sh"]
