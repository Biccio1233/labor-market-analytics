import os
import re
import sys
import requests
import xml.etree.ElementTree as ET
import psycopg2
import logging
import traceback
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

from eurostat import get_data_df, get_pars, get_dic, get_toc_df
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# CONFIGURAZIONI PRINCIPALI
# ------------------------------------------------------------------------------
DB_NAME = 'postgres'
DB_USER = 'postgres.djjawimszfspglkygynu'
DB_PASSWORD = "1233PippoFra!?"
DB_HOST = 'aws-0-eu-central-2.pooler.supabase.com'
DB_PORT = '6543'

# URL e percorso del file XML TOC
XML_URL = 'https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml'
XML_FILE_PATH = 'toc.xml'  # Percorso del file XML locale

# Namespace del file XML
NAMESPACE = {'nt': 'urn:eu.europa.ec.eurostat.navtree'}

# Nome dello schema dedicato in cui creeremo tabelle e viste
EUROSTAT_SCHEMA = "eurostat"


# ------------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO
# ------------------------------------------------------------------------------

def create_database_if_not_exists():
    """
    Verifica l'esistenza del database DB_NAME su Postgres; se non c'è, lo crea.
    """
    logger.info('Verifica se il database %s esiste', DB_NAME)
    conn = psycopg2.connect(
        dbname='postgres',  # Database di default per poter creare altri DB
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                   (DB_NAME,))
    exists = cursor.fetchone()
    if not exists:
        logger.info('Il database %s non esiste. Lo creo.', DB_NAME)
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
    else:
        logger.info('Il database %s esiste già.', DB_NAME)

    cursor.close()
    conn.close()


def connect_to_database():
    """
    Crea la connessione a DB_NAME. Se lo schema EUROSTAT non esiste, lo crea.
    Restituisce un engine SQLAlchemy.
    """
    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(db_url)

    # Creiamo lo schema eurostat se non c'è
    with engine.begin() as conn:
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {EUROSTAT_SCHEMA}"))
            logger.info(f"Schema '{EUROSTAT_SCHEMA}' creato (o già esistente).")
        except Exception as e:
            logger.error(f"Errore creazione schema {EUROSTAT_SCHEMA}: {e}")
    return engine


def sanitize_column_name(name):
    """
    Converte il nome colonna in minuscolo, sostituisce spazi con underscore,
    rimuove caratteri non alfanumerici. Se inizia con numero, lo prefissa.
    """
    import re
    name = name.lower().strip().replace(' ', '_')
    name = re.sub(r'\W|^(?=\d)', '', name)
    return name


def drop_table_if_exists(table_name, engine):
    """
    Elimina la tabella e le eventuali viste collegate.
    """
    qualified_table = f'{EUROSTAT_SCHEMA}."{table_name}"'
    with engine.begin() as connection:
        drop_views_query = text(f"""
            DO $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (
                    SELECT table_name 
                    FROM information_schema.views 
                    WHERE table_schema = '{EUROSTAT_SCHEMA}'
                    AND table_name LIKE 'view_{table_name}%'
                )
                LOOP
                    EXECUTE 'DROP VIEW IF EXISTS "{EUROSTAT_SCHEMA}.' || r.table_name || '" CASCADE';
                END LOOP;
            END $$;
        """)
        connection.execute(drop_views_query)

        drop_table_query = text(f'DROP TABLE IF EXISTS {qualified_table} CASCADE;')
        connection.execute(drop_table_query)

    logger.info(f"Tabella '{qualified_table}' (e viste collegate) rimossa (se esisteva).")


def dataframe_to_postgres(dataframe, table_name, engine):
    """
    Salva un DataFrame in Postgres (schema eurostat) come tabella "table_name".
    Sostituisce la tabella se già esiste.
    """
    qualified_table_name = f"{EUROSTAT_SCHEMA}.{table_name}"
    dataframe.to_sql(table_name, engine, schema=EUROSTAT_SCHEMA, if_exists='replace', index=False)
    logger.info(f"Tabella '{qualified_table_name}' creata/populata con successo.")


# ------------------------------------------------------------------------------
# DOWNLOAD & PARSING XML
# ------------------------------------------------------------------------------

def download_xml(url, file_path):
    logger.info('Scarico XML da %s', url)
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, 'wb') as file:
        file.write(response.content)
    logger.info('XML scaricato e salvato in %s', file_path)


def load_xml(file_path):
    logger.info('Carico XML da %s', file_path)
    tree = ET.parse(file_path)
    return tree.getroot()


def get_element_text(element):
    return element.text.strip() if element is not None and element.text else None


def extract_data_from_xml(xml_root):
    """
    Estrae la struttura gerarchica dal file TOC di Eurostat.
    """

    def parse_branch(branch_element, parent_path):
        code = get_element_text(branch_element.find('nt:code', namespaces=NAMESPACE))
        title_en = get_element_text(branch_element.find('nt:title[@language="en"]', namespaces=NAMESPACE))
        node_name = title_en if title_en else code
        current_path = parent_path + [node_name]

        children = []
        for child in branch_element.findall('nt:children/*', namespaces=NAMESPACE):
            tag_name = child.tag.split('}')[-1]
            if tag_name == 'branch':
                child_node = parse_branch(child, current_path)
                if child_node:
                    children.append(child_node)
            elif tag_name == 'leaf':
                leaf_node = parse_leaf(child, current_path)
                if leaf_node:
                    children.append(leaf_node)
        return {
            'type': 'branch',
            'name': node_name,
            'code': code,
            'path': current_path,
            'children': children
        }

    def parse_leaf(leaf_element, parent_path):
        code = get_element_text(leaf_element.find('nt:code', namespaces=NAMESPACE))
        title_en = get_element_text(leaf_element.find('nt:title[@language="en"]', namespaces=NAMESPACE))
        dataset_name = title_en if title_en else code
        current_path = parent_path + [dataset_name]
        return {
            'type': 'leaf',
            'name': dataset_name,
            'code': code,
            'path': current_path
        }

    root_branch = xml_root.find('.//nt:branch', namespaces=NAMESPACE)
    if root_branch is not None:
        return parse_branch(root_branch, [])
    else:
        return None


def display_menu(options):
    for idx, option in enumerate(options, 1):
        print(f"{idx}. {option['name']}")
    choice = input("\nInserisci il numero della tua scelta (0 per tornare indietro): ")
    if choice.isdigit():
        choice = int(choice)
        if 0 <= choice <= len(options):
            return choice
    print("Scelta non valida. Riprova.")
    return None


# ------------------------------------------------------------------------------
# LOG DOWNLOAD
# ------------------------------------------------------------------------------
def create_download_logs_table(engine):
    """
    Crea la tabella download_logs se non esiste.
    """
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {EUROSTAT_SCHEMA}.download_logs (
            dataset_code VARCHAR(255) PRIMARY KEY,
            last_download_date DATE
        );
    """
    with engine.begin() as connection:
        connection.execute(text(create_table_query))
    logger.info(f"Tabella '{EUROSTAT_SCHEMA}.download_logs' creata (o già esistente).")


def is_dataset_up_to_date(dataset_code, engine):
    """
    Controlla se un dataset 'dataset_code' è già aggiornato (logica semplificata).
    """
    dataset_code_upper = dataset_code.upper()
    query_logs = text(f"""
        SELECT last_download_date
        FROM {EUROSTAT_SCHEMA}.download_logs
        WHERE dataset_code = :dataset_code_upper
    """)
    with engine.connect() as conn:
        logs_result = conn.execute(query_logs, {'dataset_code_upper': dataset_code_upper}).fetchone()
        if logs_result:
            last_download_date = logs_result[0]
            logger.info(f"Ultimo download per {dataset_code_upper}: {last_download_date}")
            # Se l'ultimo download è "oggi" => considerato up-to-date (logica ipotetica)
            current_date = datetime.now().date()
            if last_download_date and last_download_date >= current_date:
                logger.info(f"Dataset '{dataset_code_upper}' up-to-date.")
                return True
        logger.info(f"Dataset '{dataset_code_upper}' non up-to-date.")
        return False


def update_last_download_date(dataset_code, engine):
    """
    Aggiorna o inserisce la data di scaricamento per un dataset in 'download_logs'.
    """
    current_date = datetime.now().date()
    dataset_code_upper = dataset_code.upper()
    query = text(f"""
        INSERT INTO {EUROSTAT_SCHEMA}.download_logs (dataset_code, last_download_date)
        VALUES (:dataset_code_upper, :current_date)
        ON CONFLICT (dataset_code)
        DO UPDATE SET last_download_date = EXCLUDED.last_download_date;
    """)
    with engine.begin() as conn:
        conn.execute(query, {'dataset_code_upper': dataset_code_upper, 'current_date': current_date})
    logger.info(f"Data di download aggiornata per '{dataset_code_upper}'.")


# ------------------------------------------------------------------------------
# GESTIONE CODELISTE
# ------------------------------------------------------------------------------
from eurostat import get_pars, get_dic

def fetch_and_save_codelists(dataset_code, engine):
    """
    Scarica e salva le codelist per ogni parametro del dataset.
    """
    pars = get_pars(dataset_code)
    if pars:
        for par in pars:
            dic_df = get_dic(dataset_code, par, frmt='df')
            if dic_df is not None and not dic_df.empty:
                # Standardizziamo le colonne
                if 'id' in dic_df.columns:
                    dic_df.rename(columns={'id': 'code'}, inplace=True)
                if 'label' in dic_df.columns:
                    dic_df.rename(columns={'label': 'description'}, inplace=True)
                if 'val' in dic_df.columns:
                    dic_df.rename(columns={'val': 'code'}, inplace=True)
                if 'descr' in dic_df.columns:
                    dic_df.rename(columns={'descr': 'description'}, inplace=True)

                codelist_table_name = f"{dataset_code.lower().replace('.', '_')}_{par.lower()}_codelist"
                drop_table_if_exists(codelist_table_name, engine)
                dataframe_to_postgres(dic_df, codelist_table_name, engine)
                logger.info(f"Codelist per '{par}' salvata in '{codelist_table_name}'.")
            else:
                logger.warning(f"Nessuna codelist trovata per '{par}' in '{dataset_code}'.")
    else:
        logger.warning(f"Nessun parametro trovato per '{dataset_code}'.")


# ------------------------------------------------------------------------------
# CREAZIONE VIEW: Nome con parte del titolo + [dataset_code]
# Catalogo: aggiorna se esiste
# ------------------------------------------------------------------------------
def sanitize_view_name_for_postgres(raw_name: str) -> str:
    """
    Trasforma il nome in minuscolo, sostituisce spazi con underscore,
    e mantiene i caratteri [ e ], rimuovendo tutti gli altri simboli non validi.
    """
    name = raw_name.lower().strip()
    # Sostituisce spazi con underscore
    name = re.sub(r'\s+', '_', name)
    # Manteniamo lettere, cifre, underscore, parentesi quadre
    name = re.sub(r'[^a-z0-9_\[\]]', '', name)
    return name


def create_view_catalog_table(conn):
    """
    Crea (se non esiste) una piccola tabella di log delle viste create:
    - view_name (PK)
    - dataset_code
    - dataset_title
    - created_at
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {EUROSTAT_SCHEMA}.view_catalog (
        view_name TEXT PRIMARY KEY,
        dataset_code TEXT,
        dataset_title TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()


def log_view_created(conn, view_name, dataset_code, dataset_title):
    """
    Inserisce o aggiorna (se la PK è la stessa) un record in eurostat.view_catalog
    """
    insert_sql = f"""
        INSERT INTO {EUROSTAT_SCHEMA}.view_catalog (view_name, dataset_code, dataset_title)
        VALUES (%s, %s, %s)
        ON CONFLICT (view_name)
        DO UPDATE
        SET dataset_code = EXCLUDED.dataset_code,
            dataset_title = EXCLUDED.dataset_title,
            created_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(insert_sql, (view_name, dataset_code, dataset_title))
    conn.commit()


def create_eurostat_dataset_view(conn, dataset_code, dataset_title, base_table_name):
    """
    Crea una vista nello schema eurostat con un JOIN a ogni codelist esistente.
    Esempio:
        dataset_title = "Social protection expenditure on disability by benefits - % of GDP"
        dataset_code  = "dsb_sprex01"
    Nome generato => "social_protection_expenditure_on_disability_by_benefits_of_gdp_[dsb_sprex01]"
    """
    # 1) Puliamo il titolo e limitiamolo a 80 caratteri
    max_len = 80
    temp_title = re.sub(r'[^a-zA-Z0-9 \-\(\)%_]', '', dataset_title).strip()
    temp_title = temp_title[:max_len]

    # 2) Costruiamo la stringa con "titolo parziale" + [codice]
    raw_view_name = f"{temp_title} [{dataset_code}]"

    # 3) Sanitizziamo
    view_name = sanitize_view_name_for_postgres(raw_view_name)

    # 4) Tabella di base e nome della vista con doppi apici
    qualified_table = f"{EUROSTAT_SCHEMA}.{base_table_name}"
    qualified_view = f'"{EUROSTAT_SCHEMA}"."{view_name}"'

    # 5) Parametri
    parameters = get_pars(dataset_code) or []

    # 6) Costruiamo i JOIN
    join_clauses = []
    select_clauses = [f"t.*"]
    for par in parameters:
        codelist_table = f"{dataset_code.lower().replace('.', '_')}_{par.lower()}_codelist"
        codelist_exists = False
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT EXISTS (
                  SELECT FROM information_schema.tables 
                  WHERE table_schema = '{EUROSTAT_SCHEMA}'
                    AND table_name = %s
                )
            """, (codelist_table,))
            codelist_exists = cur.fetchone()[0]

        if codelist_exists:
            join_clause = f"""
LEFT JOIN "{EUROSTAT_SCHEMA}"."{codelist_table}" AS c_{par}
    ON t."{par}" = c_{par}.code
"""
            join_clauses.append(join_clause)
            select_clauses.append(f'c_{par}.description AS {par}_desc')
        else:
            logger.warning(f"Tabella codelist {codelist_table} non trovata, param '{par}' rimarrà come codice.")

    select_part = ",\n       ".join(select_clauses)
    joins_part = "\n".join(join_clauses)

    dataset_link = f"https://ec.europa.eu/eurostat/dataset/{dataset_code}"

    create_view_sql = f"""
CREATE OR REPLACE VIEW {qualified_view} AS
SELECT
       {select_part},
       '{dataset_link}' AS dataset_link
FROM {qualified_table} t
{joins_part}
;
"""

    logger.info(f"Creo la vista {qualified_view} per dataset '{dataset_title}', code '{dataset_code}'")

    try:
        with conn.cursor() as cur:
            cur.execute(create_view_sql)
        conn.commit()
        logger.info(f"Vista '{qualified_view}' creata con successo.")

        # 7) Logghiamo la vista
        create_view_catalog_table(conn)
        log_view_created(conn, view_name, dataset_code, dataset_title)

    except Exception as e:
        conn.rollback()
        logger.error(f"Errore creazione vista {qualified_view}: {e}")


# ------------------------------------------------------------------------------
# SCARICAMENTO E SALVATAGGIO DATASET
# ------------------------------------------------------------------------------
def download_and_save_dataset(node, engine):
    """
    Scarica il dataset corrispondente al nodo (leaf) e lo salva in una tabella.
    Poi scarica le codelist, crea la view, ecc.
    """
    dataset_code = node['code']
    table_name = dataset_code.lower().replace('.', '_')
    dataset_title = node['name']

    logger.info(f"Scaricamento dataset '{dataset_title}' ({dataset_code})...")

    try:
        df = get_data_df(dataset_code)
        if df is None or df.empty:
            logger.warning(f"Dataset '{dataset_code}' vuoto o non trovato.")
            return

        # Esempio di rinomina colonne
        if 'geo\\TIME_PERIOD' in df.columns:
            df.rename(columns={'geo\\TIME_PERIOD': 'geo'}, inplace=True)
        elif 'geo\\TIME PERIOD' in df.columns:
            df.rename(columns={'geo\\TIME PERIOD': 'geo'}, inplace=True)

        # Drop e ricrea tabella
        drop_table_if_exists(table_name, engine)
        dataframe_to_postgres(df, table_name, engine)

        # Scarica codelist
        fetch_and_save_codelists(dataset_code, engine)

        # Creiamo la vista
        raw_conn = engine.raw_connection()
        try:
            create_eurostat_dataset_view(raw_conn, dataset_code, dataset_title, table_name)
        finally:
            raw_conn.close()

    except Exception as e:
        logger.error(f"Errore download/inserimento dataset '{dataset_code}': {e}")
        traceback.print_exc()


# ------------------------------------------------------------------------------
# NAVIGAZIONE
# ------------------------------------------------------------------------------
def navigate_tree(node, engine):
    """
    Permette di navigare ricorsivamente il catalogo Eurostat.
    Se leaf => chiede se scaricare dataset.
    """
    while True:
        if node['type'] == 'branch':
            print(f"\nCategoria: {' > '.join(node['path'])}")
            children = node['children']
            choice = display_menu(children)
            if choice is None:
                continue
            elif choice == 0:
                return  # Torna al livello superiore
            else:
                selected_node = children[choice - 1]
                navigate_tree(selected_node, engine)

        elif node['type'] == 'leaf':
            print(f"\nSei sul dataset: {node['name']} ({node['code']})")

            # Controllo se è up-to-date
            if is_dataset_up_to_date(node['code'], engine):
                print(f"Dataset '{node['code']}' è già aggiornato.")
                return
            else:
                ans = input(
                    f"Scaricare dataset '{node['name']}' ({node['code']})? (yes/no): "
                ).strip().lower()
                if ans == 'yes':
                    download_and_save_dataset(node, engine)
                    update_last_download_date(node['code'], engine)
                return


# ------------------------------------------------------------------------------
# FUNZIONE PER TERMINARE IL PROCESSO SU RICHIESTA
# ------------------------------------------------------------------------------
def ask_to_exit():
    """Chiede all'utente se vuole uscire, e termina il processo in caso affermativo."""
    ans = input("\nVuoi uscire dallo script? (yes/no): ").strip().lower()
    if ans == 'yes':
        print("Chiusura del processo...")
        sys.exit(0)


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    try:
        # 1) Verifica esistenza database
        create_database_if_not_exists()

        # 2) Connettiti e crea schema eurostat
        engine = connect_to_database()
        create_download_logs_table(engine)

        logger.info("Scarico la lista dataset (get_toc_df) da Eurostat...")
        datasets_df = get_toc_df()
        logger.info("Elenco dataset scaricato con successo.")

        # Salviamo la lista dataset in eurostat.eurostat_datasets
        table_list_name = "eurostat_datasets"
        drop_table_if_exists(table_list_name, engine)
        dataframe_to_postgres(datasets_df, table_list_name, engine)

        # Se non abbiamo ancora il file XML, lo scarichiamo
        if not os.path.isfile(XML_FILE_PATH):
            download_xml(XML_URL, XML_FILE_PATH)

        xml_root = load_xml(XML_FILE_PATH)
        eurostat_tree = extract_data_from_xml(xml_root)
        if not eurostat_tree:
            logger.error("Impossibile estrarre la struttura dataset da XML.")
            return

        print("Benvenuto nel catalogo Eurostat.")
        navigate_tree(eurostat_tree, engine)

        # Chiedi se vuoi uscire
        ask_to_exit()

        print("\nFatto: dataset selezionati elaborati.")
    except Exception as e:
        logger.error(f"Errore durante lo script: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    main()

