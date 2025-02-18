FROM apache/superset:4.1.1

USER root
RUN pip install psycopg2-binary redis

COPY ./superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# Script di inizializzazione
COPY ./docker-init.sh /app/docker-init.sh
RUN chmod +x /app/docker-init.sh

USER superset
EXPOSE 8088

CMD ["/app/docker-init.sh"]
