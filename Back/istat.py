import os
import re
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
# FUNZIONI DI SUPPORTO
# =============================================================================


def ensure_database_exists():
    """
    Verifica se il database DB_NAME esiste.
    Se non esiste, lo crea, collegandosi prima al database di default 'postgres'.
    """
    print(f"Verifica se il database '{DB_NAME}' esiste...")
    conn = psycopg2.connect(
        dbname=
        'postgres',  # Ci colleghiamo al database di default per poter creare altri DB
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Verifica l'esistenza del database
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME, ))
    exists = cur.fetchone()
    if not exists:
        print(f"Il database '{DB_NAME}' non esiste. Lo creo ora...")
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        print(f"Database '{DB_NAME}' creato con successo.")
    else:
        print(
            f"Il database '{DB_NAME}' esiste già. Nessuna azione necessaria.")

    cur.close()
    conn.close()


def connect_to_database():
    """Crea una connessione al database PostgreSQL."""
    try:
        conn = psycopg2.connect(dbname=DB_NAME,
                                user=DB_USER,
                                password=DB_PASSWORD,
                                host=DB_HOST,
                                port=DB_PORT)
        print("Connessione al database avvenuta con successo!")
        return conn
    except psycopg2.Error as e:
        print(f"Errore durante la connessione al database: {e}")
        exit()


def table_exists(conn, table_name):
    """
    Verifica se una tabella `table_name` esiste già nel database.
    Restituisce True se esiste, False altrimenti.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name, ))
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


def download_and_parse_xml(url):
    """
    Scarica il file XML dall'URL e restituisce l'elemento root di ElementTree.
    """
    response = requests.get(url)
    if response.status_code == 200:
        return ET.fromstring(response.content)
    else:
        print(f"Errore nel download del file: {response.status_code}")
        exit()


def extract_data_from_dataflow(root):
    """
    Estrae i Dataflow dal file XML.
    """
    data = []
    for element in root.findall('.//structure:Dataflow',
                                namespaces=NAMESPACES):
        id_value = element.attrib.get('id')
        agencyID_value = element.attrib.get('agencyID')
        version_value = element.attrib.get('version')

        ref_element = element.find('.//structure:Structure/Ref',
                                   namespaces=NAMESPACES)
        ref_id_value = ref_element.attrib.get(
            'id') if ref_element is not None else None
        package_value = ref_element.attrib.get(
            'package') if ref_element is not None else None

        name_it_element = element.find('.//common:Name[@xml:lang="it"]',
                                       namespaces=NAMESPACES)
        name_it_value = name_it_element.text if name_it_element is not None else None

        name_en_element = element.find('.//common:Name[@xml:lang="en"]',
                                       namespaces=NAMESPACES)
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
    for element in root.findall('.//structure:DataStructure',
                                namespaces=NAMESPACES):
        id_value = element.attrib.get('id')
        agencyID_value = element.attrib.get('agencyID')
        version_value = element.attrib.get('version')

        name_it_element = element.find('.//common:Name[@xml:lang="it"]',
                                       namespaces=NAMESPACES)
        name_it_value = name_it_element.text if name_it_element is not None else None

        name_en_element = element.find('.//common:Name[@xml:lang="en"]',
                                       namespaces=NAMESPACES)
        name_en_value = name_en_element.text if name_en_element is not None else None

        row = {
            'ID': id_value,
            'Nome_it': name_it_value,
            'Nome_en': name_en_value,
            'version': version_value,
            'agencyID': agencyID_value
        }
        data.append(row)

        # Estrai i dettagli
        for detail in (element.findall('.//structure:Dimension',
                                       namespaces=NAMESPACES) +
                       element.findall('.//structure:Attribute',
                                       namespaces=NAMESPACES) +
                       element.findall('.//structure:Measure',
                                       namespaces=NAMESPACES)):
            detail_id = detail.attrib.get('id')
            concept_ref = detail.find('.//structure:ConceptIdentity/Ref',
                                      namespaces=NAMESPACES)
            concept_id = concept_ref.attrib.get(
                'id') if concept_ref is not None else None
            concept_agency = concept_ref.attrib.get(
                'agencyID') if concept_ref is not None else None
            maintainableParentID = concept_ref.attrib.get(
                'maintainableParentID') if concept_ref is not None else None
            maintainableParentVersion = concept_ref.attrib.get(
                'maintainableParentVersion'
            ) if concept_ref is not None else None
            concept_class = concept_ref.attrib.get(
                'class') if concept_ref is not None else None

            local_representation = detail.find(
                './/structure:LocalRepresentation/structure:Enumeration/Ref',
                namespaces=NAMESPACES)
            enum_id = local_representation.attrib.get(
                'id') if local_representation is not None else None
            enum_version = local_representation.attrib.get(
                'version') if local_representation is not None else None
            enum_agencyID = local_representation.attrib.get(
                'agencyID') if local_representation is not None else None
            enum_package = local_representation.attrib.get(
                'package') if local_representation is not None else None
            enum_class = local_representation.attrib.get(
                'class') if local_representation is not None else None

            detail_row = {
                'datastructure_id':
                id_value,
                'type':
                detail.tag.split('}')[-1],  # Dimension, Attribute o Measure
                'detail_id':
                detail_id,
                'concept_id':
                concept_id,
                'concept_agency':
                concept_agency,
                'maintainableParentID':
                maintainableParentID,
                'maintainableParentVersion':
                maintainableParentVersion,
                'concept_class':
                concept_class,
                'position':
                detail.attrib.get('position')
                if 'position' in detail.attrib else None,
                'codelist':
                detail.find('.//structure:LocalRepresentation/Ref',
                            namespaces=NAMESPACES).attrib.get('id')
                if detail.find('.//structure:LocalRepresentation/Ref',
                               namespaces=NAMESPACES) is not None else None,
                'enum_id':
                enum_id,
                'enum_version':
                enum_version,
                'enum_agencyID':
                enum_agencyID,
                'enum_package':
                enum_package,
                'enum_class':
                enum_class
            }
            details.append(detail_row)

        # Estrai i gruppi
        for group in element.findall('.//structure:Group',
                                     namespaces=NAMESPACES):
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

        # Inserisci/aggiorna i dati
        with tqdm(total=len(data),
                  desc=f"Inserimento dati in {table_name}") as pbar:
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
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

        with tqdm(total=len(details), desc="Inserimento dettagli") as pbar:
            for detail in details:
                cur.execute(insert_query, tuple(detail.values()))
                pbar.update(1)
        conn.commit()


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

        with tqdm(total=len(groups), desc="Inserimento gruppi") as pbar:
            for group in groups:
                cur.execute(insert_query, tuple(group.values()))
                pbar.update(1)
        conn.commit()


def execute_part1(conn):
    """
    Esecuzione Parte 1: Scaricamento e inserimento dei Dataflow e Datastructure
    """
    print(
        "\nEsecuzione Parte 1: Scaricamento e inserimento dei Dataflow e Datastructure"
    )
    # URL dei file XML
    dataflow_url = 'http://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/ALL/latest'
    datastructure_url = 'http://sdmx.istat.it/SDMXWS/rest/datastructure/IT1/ALL/latest'

    print("Scaricamento Dataflow...")
    dataflow_root = download_and_parse_xml(dataflow_url)
    print("Scaricamento Datastructure...")
    datastructure_root = download_and_parse_xml(datastructure_url)

    # Estrai i dati
    print("Estrazione dati da Dataflow...")
    dataflow_data = extract_data_from_dataflow(dataflow_root)
    print("Estrazione dati da Datastructure...")
    datastructure_data, datastructure_details, datastructure_groups = extract_data_from_datastructure(
        datastructure_root)

    # Salva i dati nel database
    save_to_postgresql(dataflow_data, 'dataflow', conn)
    save_to_postgresql(datastructure_data, 'datastructure', conn)
    save_details_to_postgresql(datastructure_details, conn)
    save_groups_to_postgresql(datastructure_groups, conn)
    print("Parte 1 completata.\n")


# =============================================================================
# PARTE 1-BIS: Popola le categorie e crea la mappatura Dataflow-Categorie
# =============================================================================


def populate_categories(conn):
    """
    Scarica lo schema categorie e popola la tabella 'categories'.
    """
    url = 'https://sdmx.istat.it/SDMXWS/rest/categoryscheme/IT1/ALL/latest'
    print("Scaricamento delle categorie da ISTAT...")

    root = download_and_parse_xml(url)
    categories = extract_categories(root)

    print(f"Salvataggio di {len(categories)} categorie nel database.")

    with conn.cursor() as cur:
        create_categories_table = """
        CREATE TABLE IF NOT EXISTS categories (
            category_id VARCHAR PRIMARY KEY,
            name_it VARCHAR,
            name_en VARCHAR
        );
        """
        cur.execute(create_categories_table)
        conn.commit()

        insert_category_query = """
        INSERT INTO categories (category_id, name_it, name_en)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_id) DO NOTHING;
        """
        for category_id, category_data in categories.items():
            cur.execute(insert_category_query,
                        (category_id, category_data['name_it'],
                         category_data['name_en']))
        conn.commit()

    print(f"{len(categories)} categorie inserite correttamente.")


def extract_categories(root):
    """
    Estrae le categorie (id, name_it, name_en) dal CategoryScheme XML.
    """
    categories = {}
    total_categories_found = 0
    for category_scheme in root.findall('.//structure:CategoryScheme',
                                        namespaces=NAMESPACES):
        for category in category_scheme.findall('.//structure:Category',
                                                namespaces=NAMESPACES):
            total_categories_found += 1
            category_id = category.attrib.get('id')
            name_it_element = category.find('.//common:Name[@xml:lang="it"]',
                                            namespaces=NAMESPACES)
            name_it = name_it_element.text if name_it_element is not None else None

            name_en_element = category.find('.//common:Name[@xml:lang="en"]',
                                            namespaces=NAMESPACES)
            name_en = name_en_element.text if name_en_element is not None else None

            categories[category_id] = {'name_it': name_it, 'name_en': name_en}
    print(f"Numero totale di categorie trovate: {total_categories_found}")
    return categories


def execute_category_mapping(conn):
    """
    Crea la mappatura dataflow->categoria nella tabella 'dataflow_categories',
    basandosi su un semplice prefisso (prima dell'underscore).
    """
    print("Esecuzione mappatura delle categorie...")

    # Recupera le categorie dal DB in un dizionario
    categories_db = {}
    with conn.cursor() as cur:
        cur.execute("SELECT category_id, name_it FROM categories")
        for category_id, name_it in cur.fetchall():
            categories_db[category_id] = {'name_it': name_it}

    # Recupera i dataflow dal DB
    dataflows = []
    with conn.cursor() as cur:
        cur.execute("SELECT ID FROM dataflow")
        dataflows = [row[0] for row in cur.fetchall()]

    # Mappatura (semplice) dataflow -> category_id, basata sul prefisso
    dataflow_category_map = {}
    for df_id in dataflows:
        if '_' in df_id:
            category_id = df_id.split('_')[0]
            if category_id in categories_db:
                dataflow_category_map.setdefault(category_id, []).append(df_id)

    # Crea tabella di mapping
    with conn.cursor() as cur:
        create_mapping_table = """
        CREATE TABLE IF NOT EXISTS dataflow_categories (
            dataflow_id VARCHAR,
            category_id VARCHAR,
            FOREIGN KEY (dataflow_id) REFERENCES dataflow(ID),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
        """
        cur.execute(create_mapping_table)
        conn.commit()

        insert_mapping_query = """
        INSERT INTO dataflow_categories (dataflow_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """
        total_insertions = sum(
            len(dfs) for dfs in dataflow_category_map.values())
        with tqdm(total=total_insertions,
                  desc="Inserimento mappature dataflow-categorie") as pbar:
            for cat_id, df_list in dataflow_category_map.items():
                for df_id in df_list:
                    cur.execute(insert_mapping_query, (df_id, cat_id))
                    pbar.update(1)
        conn.commit()

    print("Mappatura delle categorie completata.\n")
    return categories_db, dataflow_category_map


def create_dataflow_category_view(conn):
    """
    Crea (o sostituisce) la vista che mostra dataflow + categoria + nomi.
    """
    with conn.cursor() as cur:
        query = """
        CREATE OR REPLACE VIEW dataflow_category_view AS
        SELECT
            dc.dataflow_id,
            df.Nome_it AS dataflow_nome_it,
            dc.category_id,
            c.name_it AS category_nome_it
        FROM
            dataflow_categories dc
        INNER JOIN dataflow df ON dc.dataflow_id = df.ID
        INNER JOIN categories c ON dc.category_id = c.category_id;
        """
        cur.execute(query)
        conn.commit()
    print("Vista 'dataflow_category_view' creata con successo.")


def select_category(conn):
    """
    Mostra la lista di categorie e chiede all'utente quale vuole selezionare.
    Restituisce l'ID categoria scelto, o None se non valido.
    """
    print("\nElenco delle categorie disponibili:\n")

    categories = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT category_id, name_it, name_en FROM categories ORDER BY 1")
        rows = cur.fetchall()

        for row in rows:
            category_id = row[0]
            name_it = row[1]
            name_en = row[2]
            name_display = name_it or name_en or "Nome non disponibile"
            print(f"Categoria ID: {category_id}, Nome: {name_display}")
            categories[category_id] = name_display

    selected_category_id = input(
        "\nInserisci l'ID della categoria che desideri selezionare: ")
    if selected_category_id not in categories:
        print(f"Categoria ID {selected_category_id} non trovata.")
        return None
    return selected_category_id


# =============================================================================
# PARTE 2: Scaricare tabelle CSV e Classificazioni
# =============================================================================


def download_content(url, file_name):
    """
    Scarica un file generico (CSV/XML) e lo salva in DOWNLOAD_DIR.
    Se esiste già in versione non-importata, lo cancella e riscarica.
    Se esiste come *_import, ritorna (None, None).
    """
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    import_file_path = os.path.join(
        DOWNLOAD_DIR,
        f"{os.path.splitext(file_name)[0]}_import{os.path.splitext(file_name)[1]}"
    )

    # Se il file è già stato importato, niente da fare
    if os.path.exists(import_file_path):
        print(
            f"File {import_file_path} già importato, nessuna azione necessaria."
        )
        return None, None

    # Se il file esiste, lo rimuoviamo per riscaricarlo
    if os.path.exists(file_path):
        print(f"File {file_path} esistente. Verrà cancellato e riscaricato.")
        os.remove(file_path)

    # Scarica il file
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"Errore nel download del file: {response.status_code}")
        return None, None

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    with open(file_path, 'wb') as f:
        with tqdm(total=total_size,
                  unit='iB',
                  unit_scale=True,
                  desc=f"Downloading {file_name}") as t:
            for data in response.iter_content(block_size):
                t.update(len(data))
                f.write(data)

    # Ritorna path e tipo di contenuto
    return file_path, ('csv' if file_name.endswith('.csv') else 'xml')


def extract_data_from_csv(file_path):
    """
    Legge un CSV in chunk e rimuove le colonne in EXCLUDE_FIELDS.
    Ritorna un DataFrame concatenato.
    """
    print(f"Lettura del CSV da: {file_path}")
    try:
        chunks = pd.read_csv(file_path, chunksize=10000, delimiter=',')
        data_parts = []
        for chunk in tqdm(chunks, desc="Lettura del CSV", unit='chunk'):
            keep_cols = [
                c for c in chunk.columns if c.lower() not in EXCLUDE_FIELDS
            ]
            data_parts.append(chunk[keep_cols])
        data_combined = pd.concat(data_parts, ignore_index=True)
        if not data_combined.empty:
            print(f"Prime righe del CSV:\n{data_combined.head()}")
        else:
            print("Il file CSV non contiene dati.")
        return data_combined
    except Exception as e:
        print(f"Errore durante l'estrazione del CSV: {e}")
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
            # Elimina la tabella se esiste
            drop_table_query = f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
            print(f"Eliminazione tabella con query: {drop_table_query}")
            cur.execute(drop_table_query)

            # Sanitizza i nomi delle colonne
            columns = [sanitize_column_name(col) for col in data.columns]
            if len(columns) != len(set(columns)):
                print(
                    "Errore: Nomi di colonne duplicati dopo la sanitizzazione."
                )
                return False

            print(f"Nomi delle colonne nel CSV: {columns}")
            columns_str = ', '.join([f'"{col}" TEXT' for col in columns])

            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_str})'
            print(f"Creazione tabella con query: {create_table_query}")
            cur.execute(create_table_query)
            conn.commit()

            total_rows = len(data)
            chunk_size = 10000
            for i in tqdm(range(0, total_rows, chunk_size),
                          desc=f"Caricamento dati in {table_name}",
                          unit='rows'):
                buffer = StringIO()
                chunk = data.iloc[i:i + chunk_size]
                chunk.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                copy_sql = f'COPY "{table_name}" FROM STDIN WITH CSV'
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
        return True
    except Exception as e:
        print(f"Errore durante la creazione della tabella {table_name}: {e}")
        return False


def download_and_parse_xml_file(url, file_name, conn):
    """
    Scarica (o riusa se già presente) il file XML per un codelist.  
    - Se la tabella corrispondente esiste e ho *_import.xml, non fa nulla (rest. None).
    - Se la tabella non esiste ma ho *_import.xml, rileggo quello e restituisco l’albero XML.
    - Altrimenti scarico ex novo.
    """
    base_name = os.path.splitext(file_name)[0]  # es: CL_FREQ
    table_name_clean = sanitize_column_name(base_name)  # es: cl_freq

    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    import_file_path = os.path.join(
        DOWNLOAD_DIR, f"{base_name}_import{os.path.splitext(file_name)[1]}")

    # 1) Tabella esiste e file import esiste => nessuna azione
    if table_exists(conn,
                    table_name_clean) and os.path.exists(import_file_path):
        print(
            f"Tabella {table_name_clean} esiste e file {import_file_path} presente. Nessuna azione necessaria."
        )
        return None

    # 2) Tabella non esiste, ma ho file import => rileggo
    if (not table_exists(
            conn, table_name_clean)) and os.path.exists(import_file_path):
        print(
            f"Tabella {table_name_clean} non esiste, ma c'è {import_file_path}. Lo importo ora."
        )
        with open(import_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    # 3) Se ho già un file locale *.xml non importato, lo uso
    if os.path.exists(file_path):
        print(f"File {file_path} esistente, utilizzo file locale.")
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    # 4) Scarico il file
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"Errore nel download del file XML: {response.status_code}")
            return None

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024

        with open(file_path, 'wb') as f:
            with tqdm(total=total_size,
                      unit='iB',
                      unit_scale=True,
                      desc=f"Downloading {file_name}") as t:
                for data in response.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)

        # Letto correttamente, apri e parse in XML
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = ET.fromstring(content)
        return root

    except Exception as e:
        print(f"Errore durante il download del file XML per {file_name}: {e}")
        return None


def download_and_save_classifications(conn, tables_to_download):
    """
    Per ogni tabella nella lista, cerca gli enum_id associati (Dimension) 
    e scarica/importa i relativi codelist in tabelle separate.
    """
    enum_ids = set()

    with conn.cursor() as cur:
        for main_table in tables_to_download:
            query = """
            SELECT DISTINCT enum_id 
            FROM datastructure_details
            WHERE enum_id IS NOT NULL 
              AND datastructure_id = (SELECT ref_id FROM dataflow WHERE ID = %s)
              AND type = 'Dimension';
            """
            cur.execute(query, (main_table, ))
            results = cur.fetchall()
            for enum_id in results:
                if enum_id[0]:
                    enum_ids.add(enum_id[0])

    if not enum_ids:
        print("Nessun enum_id trovato per le tabelle selezionate.")
        return

    print(f"Scaricamento delle classificazioni per {len(enum_ids)} enum_id...")

    for enum_id in enum_ids:
        file_name = f"{enum_id}.xml"
        url = f"http://sdmx.istat.it/SDMXWS/rest/codelist/IT1/{enum_id}"
        print(f"\nProcessando classificazione {enum_id}...")
        print(f"- URL: {url}")

        root = download_and_parse_xml_file(url, file_name, conn)
        if root is None:
            print(
                f"ERRORE: Impossibile scaricare o importare il file XML per enum_id: {enum_id}"
            )
            continue

        # Parsing dei Code e creazione tabella
        data = []
        for item in root.findall('.//structure:Codelist',
                                 namespaces=NAMESPACES):
            codelist_id = item.attrib.get('id')
            print(f"  Trovato codelist: {codelist_id}")
            for code in item.findall('.//structure:Code',
                                     namespaces=NAMESPACES):
                code_id = code.attrib.get('id')
                name_it = code.find('.//common:Name[@xml:lang="it"]',
                                    namespaces=NAMESPACES)
                name_en = code.find('.//common:Name[@xml:lang="en"]',
                                    namespaces=NAMESPACES)
                row = {
                    'code_id': code_id,
                    'name_it': name_it.text if name_it is not None else None,
                    'name_en': name_en.text if name_en is not None else None
                }
                data.append(row)

        # Salviamo su tabella
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
            with tqdm(total=len(data),
                      desc=f"Inserimento classificazione {table_name_clean}"
                      ) as pbar:
                for row in data:
                    cur.execute(
                        insert_query,
                        (row['code_id'], row['name_it'], row['name_en']))
                    pbar.update(1)
            conn.commit()

        print(f"Classificazione {enum_id} scaricata e salvata con successo.")
        rename_file_after_import(file_name)


def download_and_save_tables(conn, tables_to_download):
    """
    Scarica in CSV le tabelle corrispondenti ai dataflow selezionati 
    e le carica in Postgres.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT ID FROM dataflow")
        all_dataflows = {row[0] for row in cur.fetchall()}

    for table_name in tables_to_download:
        if table_name not in all_dataflows:
            print(
                f"Tabella {table_name} non trovata nei dataflow disponibili.")
            continue

        file_name = f"{table_name}.csv"
        url = f"https://sdmx.istat.it/SDMXWS/rest/data/{table_name}/ALL?format=csv"
        print(f"\nDownloading: {url}")
        file_path, content_type = download_content(url, file_name)

        if file_path and content_type == 'csv':
            data = extract_data_from_csv(file_path)
            if not data.empty:
                if create_table_from_data(table_name, data, conn):
                    print(
                        f"Tabella {table_name} scaricata e salvata con successo."
                    )
                    rename_file_after_import(file_name)
                else:
                    print(
                        f"Errore durante la creazione della tabella {table_name}."
                    )
            else:
                print(f"Nessun dato trovato nel file CSV per {table_name}.")
        elif file_path and content_type == 'xml':
            print(f"File XML scaricato, ma non supportato per {table_name}.")
        else:
            print(f"Impossibile scaricare o processare il contenuto da {url}")


def execute_part2(conn, tables_to_download):
    """
    Esecuzione Parte 2: Scaricamento delle tabelle selezionate e relative classificazioni.
    1) Scarica e salva le classificazioni (codelist).
    2) Scarica e salva le tabelle CSV.
    """
    print(
        "\nEsecuzione Parte 2: Scaricamento delle tabelle selezionate e relative classificazioni"
    )
    download_and_save_classifications(conn, tables_to_download)
    download_and_save_tables(conn, tables_to_download)
    print("Parte 2 completata.\n")


# =============================================================================
# PARTE 3: Creazione Viste con Nome personalizzato
# =============================================================================


def get_dataflow_name(conn, dataflow_id):
    """
    Recupera il Nome_it di un dataflow dal DB.
    Se non trovato o nullo, restituisce l'ID stesso come fallback.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT Nome_it FROM dataflow WHERE ID = %s",
                    (dataflow_id, ))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        else:
            return dataflow_id


def sanitize_for_view_name(name_str):
    """
    Rimuove caratteri non alfanumerici (tranne underscore) e sostituisce spazi con underscore.
    Trasforma in minuscolo e tronca a 50 caratteri se serve (per sicurezza).
    """
    # Sostituiamo gruppi di caratteri non alfanumerici con underscore
    name_str = re.sub(r'\W+', '_', name_str, flags=re.UNICODE)
    # Rimuove eventuali underscore multipli
    name_str = re.sub(r'__+', '_', name_str)
    # Trim underscore iniziali/finali
    name_str = name_str.strip('_')
    # Trasforma in minuscolo
    name_str = name_str.lower()
    # Tronca a 50 caratteri, per evitare problemi di lunghezza in Postgres
    if len(name_str) > 50:
        name_str = name_str[:50]
    return name_str


def get_enum_cl_mapping(conn, main_table):
    """
    Per un determinato main_table (es: 150_915), recupera i mapping
    dimension_id -> enum_table (in minuscolo).
    Ritorna un dizionario { dimensione: tabella_classificazione_sanificata }.
    """
    with conn.cursor() as cur:
        query = """
        SELECT DISTINCT enum_id, detail_id
        FROM datastructure_details
        WHERE datastructure_id = (SELECT ref_id FROM dataflow WHERE id = %s)
          AND enum_id IS NOT NULL
          AND type = 'Dimension';
        """
        cur.execute(query, (main_table, ))
        results = cur.fetchall()

    mapping = {}
    for enum_id, detail_id in results:
        if enum_id:
            mapping[detail_id.lower()] = sanitize_column_name(enum_id)
    return mapping


def build_joins(main_table, enum_cl_mapping):
    """
    Costruisce la lista di LEFT JOIN per unire la main_table con le tabelle di classificazione.
    """
    joins = []
    used_tables = set()
    for detail_id, cl_table in enum_cl_mapping.items():
        if cl_table not in used_tables:
            used_tables.add(cl_table)
            join = f'LEFT JOIN "{cl_table}" ON "{main_table}"."{detail_id}" = "{cl_table}".code_id'
            joins.append(join)
    return " ".join(joins)


def create_view_query(main_table, joins, enum_cl_mapping, view_name=None):
    """
    Crea la query di CREATE VIEW.
    - Usa view_name se fornito, altrimenti "main_table_view".
    - Aggiunge colonna obs_value_converted (CAST a float).
    - Aggiunge le colonne .name_it come dimensione_desc.
    """
    if not view_name:
        view_name = f"{main_table}_view"

    select_columns_list = []
    for detail_id, cl_table in enum_cl_mapping.items():
        column_alias = f"{detail_id}_desc"
        select_columns_list.append(f'"{cl_table}".name_it AS "{column_alias}"')

    additional_cols = ''
    if select_columns_list:
        additional_cols = ', ' + ', '.join(select_columns_list)

    obs_value_conversion = f'"{main_table}".obs_value::float AS obs_value_converted'

    query = f"""
    CREATE OR REPLACE VIEW "{view_name}" AS
    SELECT
        "{main_table}".*,
        {obs_value_conversion}{additional_cols}
    FROM "{main_table}"
    {joins};
    """
    return query


def execute_part3(conn, tables_to_download):
    """
    Esecuzione Parte 3: Creazione delle viste, con nome = NomeIt_sanificato_[ID].
    """
    print("\nEsecuzione Parte 3: Creazione delle viste personalizzate")
    with conn.cursor() as cur:
        for main_table in tables_to_download:
            # Recupera l'eventuale Nome_it del dataflow
            dataflow_name = get_dataflow_name(conn, main_table)
            # Sanifichiamo
            sanitized_dataflow_name = sanitize_for_view_name(dataflow_name)
            # Nome vista: Tasso_di_occupazione_[150_915]
            custom_view_name = f"{sanitized_dataflow_name}_[{main_table}]"

            enum_cl_mapping = get_enum_cl_mapping(conn, main_table)
            if not enum_cl_mapping:
                print(
                    f"Nessuna classificazione trovata per la tabella {main_table}."
                )
                continue

            joins = build_joins(main_table, enum_cl_mapping)
            view_query = create_view_query(main_table,
                                           joins,
                                           enum_cl_mapping,
                                           view_name=custom_view_name)

            try:
                cur.execute(view_query)
                conn.commit()
                print(
                    f"Vista creata: \"{custom_view_name}\" (basata su {main_table})"
                )
            except psycopg2.Error as e:
                print(
                    f"Errore durante la creazione della vista per {main_table}: {e}"
                )

    print("Parte 3 completata. Viste create con successo!\n")


# =============================================================================
# MAIN
# =============================================================================


def main():
    # 1) Verifica se il database esiste, altrimenti crealo
    ensure_database_exists()

    # 2) Connessione al database
    conn = connect_to_database()

    # Chiedi se vuoi eseguire l’aggiornamento Dataflow/Datastructure
    update_dataflow = input(
        "Eseguo l'aggiornamento dei data flow? (si/no): ").strip().lower()
    if update_dataflow == 'si':
        execute_part1(conn)
    else:
        print("Aggiornamento dei data flow saltato.")

    # Popola tabella categories
    populate_categories(conn)

    # Esegui mapping dataflow -> categories
    categories, dataflow_category_map = execute_category_mapping(conn)

    # Crea vista di comodo
    create_dataflow_category_view(conn)

    # Seleziona una categoria
    selected_category_id = select_category(conn)
    if not selected_category_id:
        conn.close()
        return

    # Ottieni i dataflow associati alla categoria selezionata
    selected_dataflows = dataflow_category_map.get(selected_category_id, [])
    if not selected_dataflows:
        print(
            f"Nessun dataflow trovato per la categoria ID {selected_category_id}"
        )
        conn.close()
        return

    # Mostra i dataset associati
    print("\nElenco dei dataset associati alla categoria selezionata:\n")
    with conn.cursor() as cur:
        cur.execute("SELECT ID, Nome_it FROM dataflow WHERE ID IN %s",
                    (tuple(selected_dataflows), ))
        df_rows = cur.fetchall()
        for (df_id, nome_it) in df_rows:
            print(f"Dataflow ID: {df_id}, Nome: {nome_it}")

    # Scelta dei dataflow
    tables_input = input(
        "\nInserisci i dataflow IDs da scaricare, separati da virgola: ")
    tables_to_download = [t.strip() for t in tables_input.split(',')]
    # Filtra quelli validi
    tables_to_download = [
        t for t in tables_to_download if t in selected_dataflows
    ]

    if not tables_to_download:
        print("Nessun dataflow valido inserito.")
        conn.close()
        return

    # PARTE 2
    execute_part2(conn, tables_to_download)

    # PARTE 3
    execute_part3(conn, tables_to_download)

    conn.close()
    print("Script completato con successo!")


if __name__ == "__main__":
    main()
