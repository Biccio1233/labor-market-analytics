from flask import Blueprint
from superset.extensions import appbuilder
from .views import EurostatViewsManager, EurostatDatasetManager

# Creazione del blueprint Flask
bp = Blueprint(
    'eurostat',
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/eurostat',
)

# Registrazione delle views
appbuilder.add_view(
    EurostatViewsManager,
    "Available Views",
    icon="fa-table",
    category="Eurostat",
    category_icon="fa-euro"
)

appbuilder.add_view(
    EurostatDatasetManager,
    "Manage Datasets",
    icon="fa-database",
    category="Eurostat"
)

# Funzione di inizializzazione dell'estensione
def init_app(app):
    app.register_blueprint(bp)
