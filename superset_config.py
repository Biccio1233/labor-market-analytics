import os
from dotenv import load_dotenv
from flask_caching.backends import RedisCache

# Carica il file .env (opzionale)
load_dotenv()

# =============================================================================
# TEMA E UI CUSTOMIZZATA
# =============================================================================

# Tema personalizzato per Labor Market Analytics
THEME = {
    'name': 'LaborMarketAnalytics',
    'brandColor': '#0cc2aa',      # Colore primario
    'secondaryColor': '#a88add',  # Colore secondario
    'backgroundColor': '#f0f3f4', # Sfondo
    'accentColor': '#7266ba'      # Accenti
}

# Customizzazione Dashboard
DASHBOARD_TEMPLATE_PARAMS = {
    'common': {
        'navbar_color': THEME['brandColor'],
        'accent_color': THEME['accentColor']
    }
}

# Configurazione per il frontend
FRONTEND_CONFIG = {
    'custom_filters': True,
    'enable_chart_search': True,
    'enable_dashboard_search': True,
    'search_all_dashboards': True
}

# Configurazioni per il layout
DASHBOARD_POSITIONS = {
    'GRID_DEFAULT_CARD_SIZE': 6,  # Default card size in grid
    'GRID_MIN_CARD_SIZE': 4,      # Minimum card size
    'GRID_MAX_CARD_SIZE': 12,     # Maximum card size
    'GRID_GUTTER_SIZE': 16        # Spazio tra le cards
}

# Configurazione responsive
RESPONSIVE_CONFIG = {
    'breakpoints': {
        'xs': 0,
        'sm': 576,
        'md': 768,
        'lg': 992,
        'xl': 1200
    }
}

# =============================================================================
# CONFIGURAZIONE ESTENSIONI
# =============================================================================

# Configurazione estensione Eurostat
CUSTOM_TEMPLATE_PROCESSORS = {
    'eurostat': 'superset.extensions.eurostat.processor.EurostatTemplateProcessor'
}

EUROSTAT_CONFIG = {
    'api_base_url': 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/',
    'cache_timeout': 3600,  # Cache per 1 ora
    'max_results': 1000
}

# =============================================================================
# CONFIGURAZIONE DATABASE E CACHE
# =============================================================================

# Configurazione cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',  # Usa 'redis' in produzione
    'CACHE_DEFAULT_TIMEOUT': 300  # 5 minuti
}

# Configurazione database PostgreSQL
DB_USER = os.getenv('POSTGRES_USER', 'superset')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'superset')
DB_HOST = os.getenv('POSTGRES_HOST', 'db')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'superset')
SQLALCHEMY_DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configurazione Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_DB = os.getenv('REDIS_DB', '0')

# Cache con Redis
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_DB,
    'CACHE_DEFAULT_TIMEOUT': 300
}

# =============================================================================
# SICUREZZA E AUTENTICAZIONE
# =============================================================================

# Chiave segreta per le sessioni
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'E44mxV9iAx6vyWvQ0BNPklqwfaU1uFHNenWfCbVfj+EFs70Xb4s3YtPE')

# Configurazione base di sicurezza
ENABLE_PROXY_FIX = True
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*']
}

# =============================================================================
# LOGGING E MONITORAGGIO
# =============================================================================

# Configurazione base del logging
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'DEBUG'
FILENAME = os.path.join(os.path.dirname(__file__), 'logs', 'superset.log')
