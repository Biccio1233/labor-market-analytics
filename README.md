# Labor Market Analytics Superset

Questo progetto integra Apache Superset con un'estensione personalizzata per l'accesso ai dati Eurostat.

## Struttura del Progetto

```
.
├── Dockerfile              # Configurazione Docker per Superset
├── docker-compose.yml      # Configurazione per ambiente di sviluppo
├── superset_config.py      # Configurazione di Superset
└── extensions/
    └── eurostat/          # Estensione personalizzata per Eurostat
```

## Deployment su Railway

1. Crea un nuovo progetto su Railway
2. Collega il repository GitHub
3. Aggiungi i servizi necessari:
   - PostgreSQL
   - Redis
4. Configura le variabili d'ambiente:
   ```
   SUPERSET_SECRET_KEY=your_secret_key
   POSTGRES_USER=superset
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=superset
   REDIS_URL=redis://your_redis_url
   ```

## Sviluppo Locale

Per eseguire il progetto localmente:

```bash
docker-compose up -d
```

Accedi a http://localhost:8088 con:
- Username: admin
- Password: admin
