"""
Esempio di configurazione Superset migliorata
Da salvare in superset_config.py (o un nome a tua scelta)
"""

import os
from dotenv import load_dotenv

# Carichiamo le variabili d'ambiente dal file .env
load_dotenv()

# =============================================================================
# CONFIGURAZIONE PRINCIPALE SUPERSET
# =============================================================================

# Costruisce la URI del database usando i parametri da .env
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')

SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?sslmode=require"
)

# Chiave segreta per firmare i cookie di sessione
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'insecure-secret-key')

# Configurazione cache (opzionale, se vuoi usare Redis)
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_DB = os.getenv('REDIS_DB', '0')

CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 3600,  # 1 ora
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
}

# =============================================================================
# ESTENSIONI
# =============================================================================

# Aggiungi il path delle estensioni al Python path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXTENSIONS_DIR = os.path.join(BASE_DIR, 'extensions')
sys.path.append(BASE_DIR)

# Inizializza l'estensione Eurostat
from extensions.eurostat import init_app

def init_extensions(app):
    """Inizializza tutte le estensioni"""
    init_app(app)

# =============================================================================
# ALTRE OPZIONI DI CUSTOMIZZAZIONE
# =============================================================================

# Se vuoi abilitare la feature di upload CSV e altre
UPLOAD_FOLDER = os.path.join(os.path.expanduser('~'), '.superset_uploads')

# Feature flags per funzionalità avanzate
FEATURE_FLAGS = {
    "ROW_LEVEL_SECURITY": True,
    # Altre feature flags...
}

# =============================================================================
# CONFIGURAZIONI TEMPLATES PER DASHBOARD
# =============================================================================

DASHBOARD_CONFIGS = {
    'mercato_lavoro': {
        'title': 'Analisi Mercato del Lavoro',
        'charts': [
            {
                'title': 'Tasso di Occupazione per Regione',
                'viz_type': 'big_number_total',
                'datasource': 'v_kpi_lavoro',  # Nome “datasource” definito su Superset
                'metrics': ['tasso_occupazione'],
                'filters': [{'col': 'time_period', 'op': '==', 'val': '2023'}]
            },
            {
                'title': 'Trend Occupazione',
                'viz_type': 'line',
                'datasource': 'v_mercato_lavoro',
                'metrics': ['valore'],
                'groupby': ['time_period'],
                'filters': [{'col': 'condizione_professionale', 'op': '==', 'val': 'Occupati'}]
            },
            {
                'title': 'Distribuzione per Settore',
                'viz_type': 'sunburst',
                'datasource': 'v_analisi_settoriale',
                'metrics': ['valore'],
                'groupby': ['regione', 'settore']
            }
        ]
    }
}

# =============================================================================
# CUSTOM CSS
# =============================================================================
CUSTOM_CSS = """
/*
Esempio di CSS personalizzato per i dashboard di Superset
*/

.dashboard-header {
    background-color: #2A3F5F;
    color: white;
    padding: 1rem;
    border-radius: 8px 8px 0 0;
}

.slice_container {
    border-radius: 12px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    padding: 16px;
    background: #fff;
}

.filter-box {
    background: #f5f5f5;
    padding: 12px;
    border-radius: 8px;
    margin-bottom: 16px;
    font-size: 0.9rem;
}
"""

# =============================================================================
# ISTRUZIONI SETUP
# =============================================================================
SETUP_INSTRUCTIONS = """
Istruzioni Setup SuperSet (esempio):
1) pip install apache-superset
2) superset db upgrade
3) superset fab create-admin
4) superset init
5) Imposta la variabile d'ambiente SUPERSET_CONFIG_PATH a questo file:
   export SUPERSET_CONFIG_PATH=/path/to/superset_config.py
6) Esegui superset run -p 8088 --with-threads --reload --debug
7) Accedi su http://localhost:8088 e configuralo (crea database, dataset, ecc.)

Per importare le viste (SQL) nel DB, usa ad esempio:
   psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f superset_views.sql
"""

if __name__ == "__main__":
    print(SETUP_INSTRUCTIONS)
