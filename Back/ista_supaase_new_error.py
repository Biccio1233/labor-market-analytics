import os
import re
import sys
import requests
import pandas as pd
import psycopg2
from io import StringIO
import xml.etree.ElementTree as ET
from tqdm import tqdm
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# =============================================================================
# CONFIGURAZIONI
# =============================================================================

# Schema dedicato per le tabelle ISTAT
ISTAT_SCHEMA = "istat"

NAMESPACES = {
    'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'xml': 'http://www.w3.org/XML/1998/namespace'
}

# Parametri di connessione al database
DB_NAME = 'postgres'
DB_USER = 'postgres.djjawimszfspglkygynu'
DB_PASSWORD = "1233PippoFra!?"
DB_HOST = 'aws-0-eu-central-2.pooler.supabase.com'
DB_PORT = '6543'

# Cartella di destinazione per i file scaricati
DOWNLOAD_DIR = os.path.join(os.getcwd(), "istat")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Campi da escludere (nelle tabelle CSV)
EXCLUDE_FIELDS = {
    'break', 'conf_status', 'obs_pre_break', 'obs_status', 'base_per',
    'unit_meas', 'unit_mult', 'metadata_en', 'metadata_it'
}


# =============================================================================
# FUNZIONE PLACEHOLDER PER CREAZIONE DB (SE SERVISSE)
# =============================================================================

def ensure_database_exists():
    """
    Se vuoi controllare/creare un database 'postgres' a runtime,
    implementa la logica qui.
    Per ora è un semplice placeholder che non fa nulla.
    """
    pass


# =============================================================================
# FUNZIONI DI SUPPORTO
# =============================================================================

def connect_to_database():
    """
    Si connette al database e assicura che lo schema ISTAT esista.
    Restituisce la connessione al database.
    """
    try:
        # Connessione al database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Crea lo schema ISTAT se non esiste
        with conn.cursor() as cur:
            # Verifica se lo schema esiste
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (ISTAT_SCHEMA,))
            
            schema_exists = cur.fetchone()[0]
            
            if not schema_exists:
                print(f"\nCreazione dello schema {ISTAT_SCHEMA}...")
                cur.execute(sql.SQL("CREATE SCHEMA {}").format(
                    sql.Identifier(ISTAT_SCHEMA)
                ))
                print(f"Schema {ISTAT_SCHEMA} creato con successo.")
            
            # Imposta lo schema come parte del search_path
            cur.execute(sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(ISTAT_SCHEMA)
            ))

        return conn

    except Exception as e:
        print(f"Errore durante la connessione al database: {str(e)}")
        raise


def get_table_name(base_name):
    """
    Restituisce il nome completo della tabella includendo lo schema ISTAT.
    Usa psycopg2.sql per l'escape corretto dei nomi.
    """
    return sql.SQL("{}.").format(sql.Identifier(ISTAT_SCHEMA)) + sql.Identifier(base_name)


def table_exists(conn, table_name):
    """
    Verifica se una tabella `table_name` esiste già nello schema ISTAT.
    Restituisce True se esiste, False altrimenti.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """, (ISTAT_SCHEMA, table_name))
        return cur.fetchone()[0]


def sanitize_column_name(name):
    """
    Converte il nome colonna in minuscolo, sostituisce spazi con underscore,
    e rimuove caratteri non alfanumerici.
    """
    name = name.lower().strip().replace(' ', '_')
    name = re.sub(r'\W|^(?=\d)', '', name)
    return name


def rename_file_after_import(file_name):
    """
    Rinomina il file scaricato in *_import.[ext] per marcare l’avvenuta importazione.
    """
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    import_file_path = os.path.join(
        DOWNLOAD_DIR,
        f"{os.path.splitext(file_name)[0]}_import{os.path.splitext(file_name)[1]}"
    )
    if os.path.exists(file_path) and not os.path.exists(import_file_path):
        os.rename(file_path, import_file_path)
        print(f"File {file_path} rinominato in {import_file_path}")


# =============================================================================
# PARTE 1: Scarica Dataflow e Datastructure
# =============================================================================

def download_and_parse_xml(url, max_retries=3, timeout=30):
    """
    Scarica il file XML dall'URL e restituisce l'elemento root di ElementTree.
    Implementa retry in caso di errori temporanei.
    """
    import time

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return ET.fromstring(response.content)
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  
                    print(f"Errore del server (HTTP {response.status_code}). Ritento in {wait_time} secondi...")
                    time.sleep(wait_time)
                    continue
            print(f"Errore nel download: HTTP {response.status_code}")
            if response.text:
                print(f"Dettagli errore: {response.text[:200]}...")
            sys.exit(1)
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"Timeout. Riprovo {attempt + 2}/{max_retries}...")
                continue
            print(f"Timeout dopo {max_retries} tentativi.")
            sys.exit(1)
        except requests.exceptions.RequestException as e:
            print(f"Errore download: {str(e)}")
            sys.exit(1)
        except ET.ParseError as e:
            print(f"Errore parsing XML: {str(e)}")
            sys.exit(1)


def extract_data_from_dataflow(root):
    """
    Estrae i Dataflow dal file XML.
    """
    data = []
    for element in root.findall('.//structure:Dataflow', namespaces=NAMESPACES):
        id_value = element.attrib.get('id')
        agencyID_value = element.attrib.get('agencyID')
        version_value = element.attrib.get('version')

        ref_element = element.find('.//structure:Structure/Ref', namespaces=NAMESPACES)
        if ref_element is not None:
            ref_id_value = ref_element.attrib.get('id')
            package_value = ref_element.attrib.get('package')
        else:
            ref_id_value = None
            package_value = None

        name_it_element = element.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
        name_it_value = name_it_element.text if name_it_element is not None else None

        name_en_element = element.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)
        name_en_value = name_en_element.text if name_en_element is not None else None

        row = {
            'ID': id_value,
            'Nome_it': name_it_value,
            'Nome_en': name_en_value,
            'ref_id': ref_id_value,
            'version': version_value,
            'agencyID': agencyID_value,
            'package': package_value
        }
        data.append(row)
    return data


def extract_data_from_datastructure(root):
    """
    Estrae i DataStructure, i Details e i Groups dal file XML.
    """
    data = []
    details = []
    groups = []
    for element in root.findall('.//structure:DataStructure', namespaces=NAMESPACES):
        id_value = element.attrib.get('id')
        agencyID_value = element.attrib.get('agencyID')
        version_value = element.attrib.get('version')

        name_it_element = element.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
        name_it_value = name_it_element is not None and name_it_element.text or None

        name_en_element = element.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)
        name_en_value = name_en_element is not None and name_en_element.text or None

        row = {
            'ID': id_value,
            'Nome_it': name_it_value,
            'Nome_en': name_en_value,
            'version': version_value,
            'agencyID': agencyID_value
        }
        data.append(row)

        # Estrai i dettagli (Dimension, Attribute, Measure)
        for detail in (element.findall('.//structure:Dimension', namespaces=NAMESPACES) +
                       element.findall('.//structure:Attribute', namespaces=NAMESPACES) +
                       element.findall('.//structure:Measure', namespaces=NAMESPACES)):
            detail_id = detail.attrib.get('id')

            concept_ref = detail.find('.//structure:ConceptIdentity/Ref', namespaces=NAMESPACES)
            concept_id = concept_ref.attrib.get('id') if concept_ref is not None else None
            concept_agency = concept_ref.attrib.get('agencyID') if concept_ref is not None else None
            maintainableParentID = concept_ref.attrib.get('maintainableParentID') if concept_ref is not None else None
            maintainableParentVersion = concept_ref.attrib.get('maintainableParentVersion') if concept_ref is not None else None
            concept_class = concept_ref.attrib.get('class') if concept_ref is not None else None

            local_representation = detail.find('.//structure:LocalRepresentation/structure:Enumeration/Ref', namespaces=NAMESPACES)
            if local_representation is not None:
                enum_id = local_representation.attrib.get('id')
                enum_version = local_representation.attrib.get('version')
                enum_agencyID = local_representation.attrib.get('agencyID')
                enum_package = local_representation.attrib.get('package')
                enum_class = local_representation.attrib.get('class')
            else:
                enum_id = None
                enum_version = None
                enum_agencyID = None
                enum_package = None
                enum_class = None

            detail_row = {
                'datastructure_id': id_value,
                'type': detail.tag.split('}')[-1],  # es. "Dimension"/"Attribute"/"Measure"
                'detail_id': detail_id,
                'concept_id': concept_id,
                'concept_agency': concept_agency,
                'maintainableParentID': maintainableParentID,
                'maintainableParentVersion': maintainableParentVersion,
                'concept_class': concept_class,
                'position': detail.attrib.get('position') if 'position' in detail.attrib else None,
                'codelist': (detail.find('.//structure:LocalRepresentation/Ref', namespaces=NAMESPACES).attrib.get('id')
                             if detail.find('.//structure:LocalRepresentation/Ref', namespaces=NAMESPACES) is not None else None),
                'enum_id': enum_id,
                'enum_version': enum_version,
                'enum_agencyID': enum_agencyID,
                'enum_package': enum_package,
                'enum_class': enum_class
            }
            details.append(detail_row)

        # Estrai i gruppi
        for group in element.findall('.//structure:Group', namespaces=NAMESPACES):
            group_id = group.attrib.get('id')
            group_row = {
                'datastructure_id': id_value,
                'group_id': group_id,
            }
            groups.append(group_row)

    return data, details, groups


def save_to_postgresql(data, table_name, conn):
    """
    Salva (in upsert) i DataFlow o DataStructure in Postgres.
    """
    print(f"Salvataggio dati in {table_name}")
    if table_name == 'dataflow':
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dataflow (
            ID VARCHAR PRIMARY KEY,
            Nome_it VARCHAR,
            Nome_en VARCHAR,
            ref_id VARCHAR,
            version VARCHAR,
            agencyID VARCHAR,
            package VARCHAR
        )
        """
        insert_update_query = """
        INSERT INTO dataflow (ID, Nome_it, Nome_en, ref_id, version, agencyID, package)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ID) DO UPDATE SET
            Nome_it = EXCLUDED.Nome_it,
            Nome_en = EXCLUDED.Nome_en,
            ref_id = EXCLUDED.ref_id,
            version = EXCLUDED.version,
            agencyID = EXCLUDED.agencyID,
            package = EXCLUDED.package
        """
    elif table_name == 'datastructure':
        create_table_query = """
        CREATE TABLE IF NOT EXISTS datastructure (
            ID VARCHAR PRIMARY KEY,
            Nome_it VARCHAR,
            Nome_en VARCHAR,
            version VARCHAR,
            agencyID VARCHAR
        )
        """
        insert_update_query = """
        INSERT INTO datastructure (ID, Nome_it, Nome_en, version, agencyID)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ID) DO UPDATE SET
            Nome_it = EXCLUDED.Nome_it,
            Nome_en = EXCLUDED.Nome_en,
            version = EXCLUDED.version,
            agencyID = EXCLUDED.agencyID
        """

    with conn.cursor() as cur:
        # Crea tabella se non esiste
        cur.execute(create_table_query)
        conn.commit()

        # Inserisci/aggiorna
        from tqdm import tqdm
        with tqdm(total=len(data), desc=f"Inserimento dati in {table_name}") as pbar:
            for row in data:
                cur.execute(insert_update_query, tuple(row.values()))
                pbar.update(1)
        conn.commit()


def save_details_to_postgresql(details, conn):
    """
    Salva i details in tabella dedicata (datastructure_details).
    """
    print("Salvataggio dettagli in datastructure_details")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS datastructure_details (
        id SERIAL PRIMARY KEY,
        datastructure_id VARCHAR,
        type VARCHAR,
        detail_id VARCHAR,
        concept_id VARCHAR,
        concept_agency VARCHAR,
        maintainableParentID VARCHAR,
        maintainableParentVersion VARCHAR,
        concept_class VARCHAR,
        position VARCHAR,
        codelist VARCHAR,
        enum_id VARCHAR,
        enum_version VARCHAR,
        enum_agencyID VARCHAR,
        enum_package VARCHAR,
        enum_class VARCHAR,
        FOREIGN KEY (datastructure_id) REFERENCES datastructure(ID)
    )
    """
    insert_query = """
    INSERT INTO datastructure_details (
        datastructure_id, type, detail_id, concept_id, concept_agency,
        maintainableParentID, maintainableParentVersion, concept_class,
        position, codelist, enum_id, enum_version, enum_agencyID,
        enum_package, enum_class
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Crea tabella se non esiste
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

    # Inserisci i dettagli in batch
    batch_size = 500
    total = len(details)
    total_batches = (total + batch_size - 1) // batch_size

    from tqdm import tqdm
    with tqdm(total=total, desc="Inserimento dettagli") as pbar:
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total)
            batch = details[start_idx:end_idx]

            retry_count = 0
            max_retries = 3
            while retry_count < max_retries:
                try:
                    with conn.cursor() as cur:
                        for detail in batch:
                            cur.execute(insert_query, tuple(detail.values()))
                            pbar.update(1)
                        conn.commit()
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        print(f"\nErrore fatale nel batch {batch_idx + 1}/{total_batches}: {str(e)}")
                        raise
                    print(f"\nErrore inserimento batch {batch_idx + 1}/{total_batches}, tentativo {retry_count}/{max_retries}: {e}")
                    print("Attendo 5 secondi e riprovo...")
                    import time
                    time.sleep(5)
                    try:
                        conn.rollback()
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                    except Exception:
                        print("Connessione persa. Riconnessione in corso...")
                        conn = connect_to_database()

    print("\nSalvataggio dettagli completato.")
    return True


def save_groups_to_postgresql(groups, conn):
    """
    Salva i group in tabella dedicata (datastructure_groups).
    """
    print("Salvataggio gruppi in datastructure_groups")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS datastructure_groups (
        id SERIAL PRIMARY KEY,
        datastructure_id VARCHAR,
        group_id VARCHAR,
        FOREIGN KEY (datastructure_id) REFERENCES datastructure(ID)
    )
    """
    insert_query = """
    INSERT INTO datastructure_groups (datastructure_id, group_id)
    VALUES (%s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

        from tqdm import tqdm
        with tqdm(total=len(groups), desc="Inserimento gruppi") as pbar:
            for group in groups:
                cur.execute(insert_query, tuple(group.values()))
                pbar.update(1)
        conn.commit()


def execute_part1(conn):
    """
    Esecuzione Parte 1: Scaricamento e inserimento dei Dataflow e Datastructure
    """
    print("\nEsecuzione Parte 1: Scaricamento e inserimento dei Dataflow e Datastructure")
    dataflow_url = 'https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/ALL/latest'
    datastructure_url = 'https://esploradati.istat.it/SDMXWS/rest/datastructure/IT1/ALL/latest'

    print("Scaricamento Dataflow...")
    dataflow_root = download_and_parse_xml(dataflow_url)
    print("Scaricamento Datastructure...")
    datastructure_root = download_and_parse_xml(datastructure_url)

    print("Estrazione dati da Dataflow...")
    dataflow_data = extract_data_from_dataflow(dataflow_root)

    print("Estrazione dati da Datastructure...")
    datastructure_data, datastructure_details, datastructure_groups = extract_data_from_datastructure(datastructure_root)

    # Salva i dati
    save_to_postgresql(dataflow_data, 'dataflow', conn)
    save_to_postgresql(datastructure_data, 'datastructure', conn)
    save_details_to_postgresql(datastructure_details, conn)
    save_groups_to_postgresql(datastructure_groups, conn)
    print("Parte 1 completata.\n")

# =============================================================================
# PARTE 1-BIS: Popola le categorie e crea la mappatura Dataflow-Categorie
# =============================================================================

def extract_categories(root):
    """
    Estrae le categorie (id, name_it, name_en) dal CategoryScheme XML.
    """
    categories = {}
    total_categories_found = 0
    for category_scheme in root.findall('.//structure:CategoryScheme', namespaces=NAMESPACES):
        for category in category_scheme.findall('.//structure:Category', namespaces=NAMESPACES):
            total_categories_found += 1
            cat_id = category.attrib.get('id')
            name_it_elem = category.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
            name_en_elem = category.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)

            categories[cat_id] = {
                'name_it': name_it_elem.text if name_it_elem is not None else None,
                'name_en': name_en_elem.text if name_en_elem is not None else None
            }
    print(f"Numero totale categorie trovate: {total_categories_found}")
    return categories


def populate_categories(conn):
    """
    Scarica lo schema categorie e popola la tabella 'categories'.
    """
    url = 'https://esploradati.istat.it/SDMXWS/rest/categoryscheme/IT1/ALL/latest'
    print("Scaricamento categorie da ISTAT...")

    root = download_and_parse_xml(url)
    categories = extract_categories(root)

    print(f"Salvataggio di {len(categories)} categorie nel database.")

    with conn.cursor() as cur:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS categories (
            category_id VARCHAR PRIMARY KEY,
            name_it VARCHAR,
            name_en VARCHAR
        )
        """
        cur.execute(create_table_query)
        conn.commit()

        insert_query = """
        INSERT INTO categories (category_id, name_it, name_en)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_id) DO NOTHING
        """
        for cat_id, cat_data in categories.items():
            cur.execute(insert_query, (cat_id, cat_data['name_it'], cat_data['name_en']))
        conn.commit()

    print(f"{len(categories)} categorie inserite correttamente.")


def execute_category_mapping(conn):
    """
    Crea la mappatura dataflow->categoria nella tabella dataflow_categories,
    usando prefissi (prima dell'underscore).
    """
    print("Esecuzione mappatura categorie...")

    # Carico tutte le categorie
    categories_db = {}
    with conn.cursor() as cur:
        cur.execute("SELECT category_id, name_it FROM categories")
        for row in cur.fetchall():
            category_id, name_it = row
            categories_db[category_id] = {'name_it': name_it}

    # Carico tutti i dataflow
    dataflows = []
    with conn.cursor() as cur:
        cur.execute("SELECT ID FROM dataflow")
        dataflows = [r[0] for r in cur.fetchall()]

    # Logica di matching: se dataflow_id inizia con `cat_id + "_"` => mappalo
    dataflow_category_map = {}
    for df_id in dataflows:
        matching_category = None
        max_len = 0
        for cat_id in categories_db:
            if df_id.startswith(cat_id + '_'):
                if len(cat_id) > max_len:
                    matching_category = cat_id
                    max_len = len(cat_id)
        if matching_category:
            dataflow_category_map.setdefault(matching_category, []).append(df_id)

    # Crea tabella e inserisci
    with conn.cursor() as cur:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dataflow_categories (
            dataflow_id VARCHAR,
            category_id VARCHAR,
            FOREIGN KEY (dataflow_id) REFERENCES dataflow(ID),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
        """
        cur.execute(create_table_query)
        conn.commit()

        insert_query = """
        INSERT INTO dataflow_categories (dataflow_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """

        total_inserts = sum(len(dflist) for dflist in dataflow_category_map.values())
        with tqdm(total=total_inserts, desc="Inserimento mappature df-categorie") as pbar:
            for cat_id, dflist in dataflow_category_map.items():
                for df_id in dflist:
                    cur.execute(insert_query, (df_id, cat_id))
                    pbar.update(1)
        conn.commit()

    print("Mappatura categorie completata.\n")
    return categories_db, dataflow_category_map


def create_dataflow_category_view(conn):
    """
    Crea/aggiorna la view dataflow_category_view per comodità.
    """
    with conn.cursor() as cur:
        query = """
        CREATE OR REPLACE VIEW dataflow_category_view AS
        SELECT
            dc.dataflow_id,
            df.nome_it AS dataflow_nome_it,
            dc.category_id,
            c.name_it AS category_nome_it
        FROM dataflow_categories dc
        JOIN dataflow df ON dc.dataflow_id = df.id
        JOIN categories c ON dc.category_id = c.category_id
        """
        cur.execute(query)
        conn.commit()
    print("Vista 'dataflow_category_view' creata/aggiornata correttamente.")


# ------------------------------------------------------------------------------
# FUNZIONE select_category(conn) PER SCEGLIERE UNA CATEGORIA CON OPZIONE B
# ------------------------------------------------------------------------------

def select_category(conn):
    """
    Mostra l'elenco di (category_id, name_it, name_en) e chiede
    all'utente di digitare *direttamente* l'ID della categoria (oppure 0 per annullare).
    Restituisce l'ID categoria scelto o None se l'utente annulla.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT category_id, name_it, name_en FROM categories ORDER BY category_id")
        rows = cur.fetchall()

    if not rows:
        print("Nessuna categoria trovata nella tabella 'categories'.")
        return None

    # Prepara un dizionario per rapida ricerca: { category_id: (name_it, name_en) }
    cat_dict = {}
    for (cat_id, name_it, name_en) in rows:
        cat_dict[cat_id] = (name_it, name_en)

    print("\nCategorie disponibili (category_id => Nome):")
    for cat_id in sorted(cat_dict):
        name_it, name_en = cat_dict[cat_id]
        display_name = name_it or name_en or cat_id
        print(f" - {cat_id} => {display_name}")

    while True:
        user_input = input("\nDigita l'ID della categoria (0 per annullare): ").strip()
        if user_input == "0":
            return None
        if user_input in cat_dict:
            print(f"Hai selezionato la categoria con ID: {user_input}")
            return user_input
        else:
            print(f"L'ID '{user_input}' non esiste tra le categorie. Riprova (o digita 0 per annullare).")


# =============================================================================
# PARTE 2: CSV + Classificazioni
# =============================================================================

def download_content(url, file_name):
    """
    Scarica un file generico (CSV/XML) e lo salva in DOWNLOAD_DIR.
    Se esiste già in versione non-importata, ritorna (None, None).
    """
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    import_file_path = os.path.join(
        DOWNLOAD_DIR,
        f"{os.path.splitext(file_name)[0]}_import{os.path.splitext(file_name)[1]}"
    )

    # Se il file è già stato importato, nessuna azione
    if os.path.exists(import_file_path):
        print(f"File {import_file_path} già importato, nessuna azione necessaria.")
        return None, None

    # Se esiste (non importato), cancelliamolo
    if os.path.exists(file_path):
        print(f"File {file_path} esistente. Lo cancello e riscarico.")
        os.remove(file_path)

    # Scarica
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"Errore nel download del file: HTTP {response.status_code}")
        return None, None

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    with open(file_path, 'wb') as f:
        with tqdm(total=total_size, unit='iB', unit_scale=True,
                  desc=f"Downloading {file_name}") as t:
            for data in response.iter_content(block_size):
                t.update(len(data))
                f.write(data)

    # Determina tipo (csv/xml) dal nome
    if file_name.endswith('.csv'):
        return file_path, 'csv'
    elif file_name.endswith('.xml'):
        return file_path, 'xml'
    else:
        return file_path, None


def extract_data_from_csv(file_path):
    """
    Legge un CSV a chunk e rimuove colonne in EXCLUDE_FIELDS.
    Restituisce un DataFrame concatenato.
    """
    print(f"Lettura CSV: {file_path}")
    try:
        chunks = pd.read_csv(file_path, chunksize=10000, delimiter=',')
        data_parts = []
        for chunk in tqdm(chunks, desc="Lettura del CSV", unit='chunk'):
            keep_cols = [c for c in chunk.columns if c.lower() not in EXCLUDE_FIELDS]
            data_parts.append(chunk[keep_cols])
        data_combined = pd.concat(data_parts, ignore_index=True)
        if not data_combined.empty:
            print(f"Prime righe:\n{data_combined.head()}")
        else:
            print("Il CSV non contiene dati.")
        return data_combined
    except Exception as e:
        print(f"Errore estrazione CSV: {e}")
        return pd.DataFrame()


def create_table_from_data(table_name, data, conn):
    """
    Crea una tabella (nome = `table_name`) con tutte le colonne come TEXT,
    e copia i dati del DataFrame in blocchi da 10k righe.
    """
    if data.empty:
        print(f"Nessun dato trovato per la tabella {table_name}")
        return False

    try:
        with conn.cursor() as cur:
            # Rimuoviamo la tabella se esiste
            drop_table_query = f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
            print(f"Eliminazione tabella con query: {drop_table_query}")
            cur.execute(drop_table_query)

            # Sanitizza colonne
            columns = [sanitize_column_name(col) for col in data.columns]
            if len(columns) != len(set(columns)):
                print("Errore: nomi di colonne duplicati dopo sanitizzazione.")
                return False

            print(f"Nomi colonne CSV: {columns}")
            columns_str = ', '.join([f'"{col}" TEXT' for col in columns])

            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_str})'
            print(f"Creazione tabella: {create_table_query}")
            cur.execute(create_table_query)
            conn.commit()

            total_rows = len(data)
            chunk_size = 10000
            from io import StringIO
            for i in tqdm(range(0, total_rows, chunk_size),
                          desc=f"Caricamento dati in {table_name}",
                          unit='rows'):
                buffer = StringIO()
                chunk = data.iloc[i:i + chunk_size]
                # Scrive CSV senza header/index in buffer
                chunk.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                copy_sql = f'COPY "{table_name}" FROM STDIN WITH CSV'
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
        return True
    except Exception as e:
        print(f"Errore creazione tabella {table_name}: {e}")
        return False


def download_and_parse_xml_file(url, file_name, conn):
    """
    Scarica (o riusa se già presente) il file XML per un codelist.
    - Se la tabella corrispondente esiste e ho *_import.xml, non fa nulla (rest. None).
    - Se la tabella non esiste ma ho *_import.xml, rileggo il file e restituisco l’albero XML.
    - Altrimenti, scarica ex novo.
    """
    base_name = os.path.splitext(file_name)[0]
    table_name_clean = sanitize_column_name(base_name)

    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    import_file_path = os.path.join(DOWNLOAD_DIR,
                                    f"{base_name}_import{os.path.splitext(file_name)[1]}")

    # 1) Se tabella esiste e file import esiste => nessuna azione
    if table_exists(conn, table_name_clean) and os.path.exists(import_file_path):
        print(f"Tabella {table_name_clean} esiste e {import_file_path} presente. Nessuna azione.")
        return None

    # 2) Tabella non esiste, ma ho file import => rileggo
    if (not table_exists(conn, table_name_clean)) and os.path.exists(import_file_path):
        print(f"Tabella {table_name_clean} non esiste, ma c'è {import_file_path}. Lo importo.")
        with open(import_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    # 3) Se ho già file locale .xml non importato => lo uso
    if os.path.exists(file_path):
        print(f"File {file_path} già presente, uso file locale.")
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    # 4) Scarico file
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"Errore download XML: {response.status_code}")
            return None

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024

        with open(file_path, 'wb') as f:
            with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {file_name}") as t:
                for data in response.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)

        # Parse in XML
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    except Exception as e:
        print(f"Errore download XML per {file_name}: {e}")
        return None


def download_and_save_classifications(conn, tables_to_download):
    """
    Data una lista di dataflow, trova enum_id e scarica codelist (XML) in tabelle separate.
    """
    enum_ids = set()
    with conn.cursor() as cur:
        for main_table in tables_to_download:
            query = """
            SELECT DISTINCT enum_id
            FROM datastructure_details
            WHERE enum_id IS NOT NULL 
              AND datastructure_id = (SELECT ref_id FROM dataflow WHERE id = %s)
              AND type = 'Dimension';
            """
            cur.execute(query, (main_table,))
            for row in cur.fetchall():
                if row[0]:
                    enum_ids.add(row[0])

    if not enum_ids:
        print("Nessun enum_id trovato per i dataflow selezionati.")
        return

    print(f"Scaricamento classificazioni per {len(enum_ids)} enum_id...")

    for enum_id in enum_ids:
        file_name = f"{enum_id}.xml"
        url = f"http://sdmx.istat.it/SDMXWS/rest/codelist/IT1/{enum_id}"
        print(f"\nProcesso codelist {enum_id} => {file_name}")

        root = download_and_parse_xml_file(url, file_name, conn)
        if root is None:
            print(f"ERRORE: Impossibile importare codelist {enum_id}")
            continue

        # Parsing dei Code
        data = []
        for codelist_elem in root.findall('.//structure:Codelist', namespaces=NAMESPACES):
            codelist_id = codelist_elem.attrib.get('id')
            print(f"  Trovato codelist: {codelist_id}")
            for code in codelist_elem.findall('.//structure:Code', namespaces=NAMESPACES):
                code_id = code.attrib.get('id')
                name_it_elem = code.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
                name_en_elem = code.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)
                row = {
                    'code_id': code_id,
                    'name_it': name_it_elem.text if name_it_elem is not None else None,
                    'name_en': name_en_elem.text if name_en_elem is not None else None
                }
                data.append(row)

        # Creazione tabella
        table_name_clean = sanitize_column_name(enum_id)
        with conn.cursor() as cur:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS "{table_name_clean}" (
                code_id VARCHAR PRIMARY KEY,
                name_it TEXT,
                name_en TEXT
            )
            """
            cur.execute(create_table_query)
            conn.commit()

            insert_query = f"""
            INSERT INTO "{table_name_clean}" (code_id, name_it, name_en)
            VALUES (%s, %s, %s)
            ON CONFLICT (code_id) DO NOTHING
            """
            from tqdm import tqdm
            with tqdm(total=len(data), desc=f"Inserimento codelist {table_name_clean}") as pbar:
                for row in data:
                    cur.execute(insert_query, (row['code_id'], row['name_it'], row['name_en']))
                    pbar.update(1)
            conn.commit()

        print(f"Classificazione {enum_id} salvata con successo.")
        rename_file_after_import(file_name)


def download_and_save_tables(conn, tables_to_download):
    """
    Scarica in CSV i dataflow selezionati e li carica in tabelle Postgres.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT ID FROM dataflow")
        known_dataflows = {r[0] for r in cur.fetchall()}

    for df_id in tables_to_download:
        if df_id not in known_dataflows:
            print(f"Dataflow {df_id} non trovato nei dataflow.")
            continue

        file_name = f"{df_id}.csv"
        url = f"https://sdmx.istat.it/SDMXWS/rest/data/{df_id}/ALL?format=csv"
        print(f"\nDownload CSV: {url}")
        file_path, content_type = download_content(url, file_name)
        if file_path and content_type == 'csv':
            df = extract_data_from_csv(file_path)
            if not df.empty:
                created = create_table_from_data(df_id, df, conn)
                if created:
                    print(f"Tabella {df_id} creata/salvata con successo.")
                    rename_file_after_import(file_name)
                else:
                    print(f"Errore nella creazione tabella {df_id}.")
            else:
                print(f"CSV {df_id} è vuoto.")
        else:
            print(f"Impossibile scaricare/elaborare CSV per {df_id}.")


def execute_part2(conn, tables_to_download):
    """
    Scarica e salva le codelist per i dataflow, e i CSV per i dataflow scelti.
    """
    print("\nEsecuzione Parte 2: scarico tabelle + codelist")
    download_and_save_classifications(conn, tables_to_download)
    download_and_save_tables(conn, tables_to_download)
    print("Parte 2 completata.\n")


# =============================================================================
# PARTE 3: Creazione Viste personalizzate
# =============================================================================

def get_dataflow_name(conn, dataflow_id):
    """
    Ritorna Nome_it del dataflow, se c'è. Altrimenti ID.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT Nome_it FROM dataflow WHERE ID = %s", (dataflow_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return dataflow_id

def sanitize_for_view_name(name_str):
    """
    Rimuove caratteri speciali, spazi, ecc. Tronca a 50 char.
    """
    name_str = re.sub(r'\W+', '_', name_str, flags=re.UNICODE)
    name_str = re.sub(r'__+', '_', name_str).strip('_').lower()
    if len(name_str) > 50:
        name_str = name_str[:50]
    return name_str

def get_enum_cl_mapping(conn, main_table):
    """
    Per un dataflow = main_table, enum_id -> tabella codelist.
    """
    with conn.cursor() as cur:
        query = """
        SELECT DISTINCT enum_id, detail_id
        FROM datastructure_details
        WHERE enum_id IS NOT NULL 
          AND datastructure_id = (SELECT ref_id FROM dataflow WHERE id = %s)
          AND type = 'Dimension';
        """
        cur.execute(query, (main_table,))
        results = cur.fetchall()

    mapping = {}
    for enum_id, detail_id in results:
        if enum_id:
            mapping[detail_id.lower()] = sanitize_column_name(enum_id)
    return mapping

def build_joins(main_table, enum_cl_mapping):
    """
    Costruisce i LEFT JOIN con le tabelle di codelist.
    """
    joins = []
    used = set()
    for detail_id, codelist_table in enum_cl_mapping.items():
        if codelist_table not in used:
            used.add(codelist_table)
            clause = f'LEFT JOIN "{codelist_table}" ON "{main_table}"."{detail_id}" = "{codelist_table}".code_id'
            joins.append(clause)
    return " ".join(joins)

def create_view_query(main_table, joins, enum_cl_mapping, view_name=None):
    if not view_name:
        view_name = f"{main_table}_view"

    # Aggiunge colonna "obs_value_converted" e le dimensioni in .name_it
    select_dimensions = []
    for detail_id, codelist_table in enum_cl_mapping.items():
        alias = f"{detail_id}_desc"
        select_dimensions.append(f'"{codelist_table}".name_it AS "{alias}"')

    extra_cols = ''
    if select_dimensions:
        extra_cols = ', ' + ', '.join(select_dimensions)

    obs_value_cast = f'"{main_table}".obs_value::float AS obs_value_converted'

    query = f"""
    CREATE OR REPLACE VIEW "{view_name}" AS
    SELECT
        "{main_table}".*,
        {obs_value_cast}{extra_cols}
    FROM "{main_table}"
    {joins};
    """
    return query

def execute_part3(conn, tables_to_download):
    print("\nEsecuzione Parte 3: creazione viste personalizzate.")
    with conn.cursor() as cur:
        for main_table in tables_to_download:
            df_name = get_dataflow_name(conn, main_table)
            sanitized_name = sanitize_for_view_name(df_name)
            view_name = f"{sanitized_name}_[{main_table}]"

            enum_cl_map = get_enum_cl_mapping(conn, main_table)
            if not enum_cl_map:
                print(f"Nessuna codelist per {main_table}.")
                continue

            joins = build_joins(main_table, enum_cl_map)
            view_query = create_view_query(main_table, joins, enum_cl_map, view_name=view_name)

            try:
                cur.execute(view_query)
                conn.commit()
                print(f"Vista creata: \"{view_name}\" (da {main_table})")
            except psycopg2.Error as e:
                print(f"Errore creazione vista per {main_table}: {e}")
    print("Parte 3 completata.\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Eventuale check/creazione DB (stub, se serve)
    ensure_database_exists()

    # Connessione + creazione schema se mancante
    conn = connect_to_database()

    # Aggiornamento dataflow?
    choice = input("Eseguire update dataflow/datastructure? (si/no): ").strip().lower()
    if choice == 'si':
        execute_part1(conn)
    else:
        print("Skip dataflow/datastructure update.")

    # Popola tabella categories e crea mapping
    populate_categories(conn)
    cats_db, df_cat_map = execute_category_mapping(conn)

    create_dataflow_category_view(conn)

    # Seleziona una categoria => la scegli digitando l'ID, non un indice
    cat_id = select_category(conn)
    if not cat_id:
        conn.close()
        return

    # Trova i dataflow associati a cat_id
    selected_dfs = df_cat_map.get(cat_id, [])
    if not selected_dfs:
        print(f"Nessun dataflow per la categoria {cat_id}")
        conn.close()
        return

    # Mostra i dataflow associati
    print("\nDataflow associati:")
    with conn.cursor() as cur:
        cur.execute("SELECT ID, Nome_it FROM dataflow WHERE ID IN %s", (tuple(selected_dfs),))
        rows = cur.fetchall()
        for r in rows:
            df_id, nome_it = r
            print(f"- {df_id}  NomeIt: {nome_it}")

    # Chiedi ID dataflow da scaricare
    inp = input("\nInserisci ID dataflow da scaricare (virgole), tra quelli elencati: ")
    df_to_dl = [x.strip() for x in inp.split(',')]
    # Filtra solo quelli validi (presenti in selected_dfs)
    df_to_dl = [x for x in df_to_dl if x in selected_dfs]
    if not df_to_dl:
        print("Nessun dataflow valido scelto.")
        conn.close()
        return

    # Parte 2
    execute_part2(conn, df_to_dl)

    # Parte 3
    execute_part3(conn, df_to_dl)

    conn.close()
    print("Script completato con successo!")


if __name__ == "__main__":
    main()
