import os
import requests
import xml.etree.ElementTree as ET
import psycopg2
import logging
import traceback
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from eurostat import get_data_df, get_pars, get_dic, get_toc_df
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
from datetime import datetime

# Configurazione del logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurazione del database PostgreSQL
DB_NAME = 'postgres'
DB_USER = 'postgres.djjawimszfspglkygynu'
DB_PASSWORD = "1233PippoFra!?"
DB_HOST = 'aws-0-eu-central-2.pooler.supabase.com'
DB_PORT = '6543'

# URL e percorso del file XML TOC
XML_URL = 'https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml'
XML_FILE_PATH = 'toc.xml'  # Percorso del file XML locale

# Namespace utilizzato nel file XML
NAMESPACE = {'nt': 'urn:eu.europa.ec.eurostat.navtree'}


def create_database_if_not_exists():
    logger.info('Verifica se il database %s esiste', DB_NAME)
    # Connettiti al database predefinito
    conn = psycopg2.connect(
        dbname='postgres',  # Connettiti al database predefinito
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT)
    conn.set_isolation_level(
        ISOLATION_LEVEL_AUTOCOMMIT)  # Necessario per creare database
    cursor = conn.cursor()

    # Verifica se il database esiste
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                   (DB_NAME, ))
    exists = cursor.fetchone()
    if not exists:
        logger.info('Il database %s non esiste. Lo creo.', DB_NAME)
        cursor.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
    else:
        logger.info('Il database %s esiste già.', DB_NAME)

    cursor.close()
    conn.close()


def create_download_logs_table(engine):
    """
    Crea la tabella download_logs se non esiste.
    """
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS download_logs (
                dataset_code VARCHAR(255) PRIMARY KEY,
                last_download_date DATE
            );
        """
        # logger.info(f"Esecuzione della query SQL: {create_table_query}")  # Log della query
        with engine.begin() as connection:
            connection.execute(text(create_table_query))
        logger.info("Tabella 'download_logs' creata o già esistente.")
    except Exception as e:
        logger.error(
            f"Errore durante la creazione della tabella 'download_logs': {e}")
        traceback.print_exc()


def drop_table_if_exists(table_name, engine):
    """
    Cancella le viste associate e poi elimina la tabella in PostgreSQL se esiste.
    """
    with engine.begin() as connection:
        drop_views_query = f"""
            DO $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT table_name FROM information_schema.views WHERE table_name LIKE 'view_{table_name}_%')
                LOOP
                    EXECUTE 'DROP VIEW IF EXISTS ' || r.table_name || ' CASCADE';
                END LOOP;
            END $$;
        """
        connection.execute(text(drop_views_query))
        drop_table_query = f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'
        connection.execute(text(drop_table_query))
        logger.info(
            f"Tabella '{table_name}' e le sue viste associate sono state cancellate."
        )


def dataframe_to_postgres(dataframe, table_name, engine):
    """
    Crea una nuova tabella in PostgreSQL usando le colonne del dataframe
    e popola la tabella con i dati del dataframe.
    """
    dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    logger.info(
        f"Tabella '{table_name}' creata e popolata con successo in PostgreSQL."
    )


def download_xml(url, file_path):
    logger.info('Downloading XML from %s', url)
    response = requests.get(url)
    response.raise_for_status()  # Controlla se ci sono errori HTTP
    with open(file_path, 'wb') as file:
        file.write(response.content)
    logger.info('XML scaricato e salvato in %s', file_path)


def load_xml(file_path):
    logger.info('Loading XML from %s', file_path)
    tree = ET.parse(file_path)
    return tree.getroot()


def get_element_text(element):
    return element.text.strip(
    ) if element is not None and element.text else None


def extract_data_from_xml(xml_root):
    """
    Estrae la struttura gerarchica completa dal file XML.
    """

    def parse_branch(branch_element, parent_path):
        code = get_element_text(
            branch_element.find('nt:code', namespaces=NAMESPACE))
        title_en = get_element_text(
            branch_element.find('nt:title[@language="en"]',
                                namespaces=NAMESPACE))
        node_name = title_en if title_en else code
        current_path = parent_path + [node_name]
        children = []
        for child in branch_element.findall('nt:children/*',
                                            namespaces=NAMESPACE):
            if child.tag.endswith('branch'):
                child_node = parse_branch(child, current_path)
                if child_node:
                    children.append(child_node)
            elif child.tag.endswith('leaf'):
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
        code = get_element_text(
            leaf_element.find('nt:code', namespaces=NAMESPACE))
        title_en = get_element_text(
            leaf_element.find('nt:title[@language="en"]',
                              namespaces=NAMESPACE))
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
    choice = input(
        "\nInserisci il numero della tua scelta (0 per tornare indietro): ")
    if choice.isdigit():
        choice = int(choice)
        if 0 <= choice <= len(options):
            return choice
    print("Scelta non valida. Per favore, riprova.")
    return None


from datetime import datetime
from dateutil import parser  # Per gestire stringhe di data con fuso orario


def is_dataset_up_to_date(dataset_code, engine):
    """
    Verifica se il dataset è già aggiornato in base alle colonne che includono last_update_of_data e last_table_structure_change.
    """
    try:
        dataset_code_upper = dataset_code.upper()

        # Query per ottenere le informazioni dal database
        query_eurostat = text(f"""
            SELECT "last update of data", "last table structure change"
            FROM eurostat_datasets
            WHERE code = :dataset_code_upper
        """)

        query_logs = text(f"""
            SELECT last_download_date
            FROM download_logs
            WHERE dataset_code = :dataset_code_upper
        """)

        with engine.connect() as connection:
            eurostat_result = connection.execute(
                query_eurostat, {
                    'dataset_code_upper': dataset_code_upper
                }).fetchone()
            logs_result = connection.execute(
                query_logs, {
                    'dataset_code_upper': dataset_code_upper
                }).fetchone()

            if eurostat_result:
                last_update_of_data, last_table_structure_change = eurostat_result

                # Se sono stringhe, convertili in datetime; se sono già date o datetime, mantienili come sono
                if isinstance(last_update_of_data, str):
                    last_update_of_data = parser.isoparse(last_update_of_data)
                if isinstance(last_table_structure_change, str):
                    last_table_structure_change = parser.isoparse(
                        last_table_structure_change)

                current_date = datetime.now().date()

                if logs_result:
                    last_download_date = logs_result[0]

                    # Se la data di download è una stringa, convertila in un oggetto datetime
                    if isinstance(last_download_date, str):
                        last_download_date = parser.isoparse(
                            last_download_date)

                    # Se last_download_date è un oggetto datetime, usiamo solo la parte date
                    if isinstance(last_download_date, datetime):
                        last_download_date = last_download_date.date()

                    # Confronta solo le date
                    if (last_update_of_data
                            and isinstance(last_update_of_data, datetime)):
                        last_update_of_data = last_update_of_data.date()

                    if (last_table_structure_change and isinstance(
                            last_table_structure_change, datetime)):
                        last_table_structure_change = last_table_structure_change.date(
                        )

                    if (last_update_of_data and last_download_date >= last_update_of_data) and \
                       (last_table_structure_change and last_download_date >= last_table_structure_change):
                        logger.info(
                            f"Il dataset '{dataset_code_upper}' è già aggiornato. Nessun download necessario."
                        )
                        return True
                    else:
                        logger.info(
                            f"Il dataset '{dataset_code_upper}' non è aggiornato rispetto a Eurostat. Procedo con il download."
                        )
                        return False
                else:
                    logger.info(
                        f"Nessuna data di scaricamento trovata per il dataset '{dataset_code_upper}'. Procedo con il download."
                    )
                    return False
            else:
                logger.warning(
                    f"Dataset '{dataset_code_upper}' non trovato nella tabella 'eurostat_datasets'."
                )
                return False
    except Exception as e:
        logger.error(
            f"Errore durante la verifica dell'aggiornamento del dataset '{dataset_code}': {e}"
        )
        traceback.print_exc()


def update_last_download_date(dataset_code, engine):
    """
    Aggiorna o inserisce il record della data di scaricamento per un dataset nella tabella 'download_logs'.
    """
    try:
        current_date = datetime.now().date()
        dataset_code_upper = dataset_code.upper(
        )  # Convertiamo il codice in maiuscolo

        # Query per inserire o aggiornare la data di scaricamento
        query = text(f"""
            INSERT INTO download_logs (dataset_code, last_download_date)
            VALUES (:dataset_code_upper, :current_date)
            ON CONFLICT (dataset_code)
            DO UPDATE SET last_download_date = EXCLUDED.last_download_date;
        """)

        #logger.info(f"Esecuzione della query SQL per l'aggiornamento del log: {query}")

        # Utilizza una transazione esplicita per garantire il commit
        with engine.begin() as connection:
            connection.execute(
                query, {
                    'dataset_code_upper': dataset_code_upper,
                    'current_date': current_date
                })

        logger.info(
            f"Data di scaricamento aggiornata per il dataset '{dataset_code_upper}' nella tabella 'download_logs'."
        )
    except Exception as e:
        logger.error(
            f"Errore durante l'aggiornamento della data di scaricamento per il dataset '{dataset_code}': {e}"
        )
        traceback.print_exc()


def create_view_with_codelists(dataset_code, dataframe, engine):
    """
    Crea una vista in PostgreSQL che unisce i dati del dataset con le sue codelist.
    """
    try:
        pars = get_pars(
            dataset_code)  # Ottieni i parametri (dimensioni) del dataset
        logger.info(
            f"Parametri ottenuti per il dataset '{dataset_code}': {pars}")

        dataset_table_name = dataset_code.lower().replace('.', '_')
        select_columns = []
        join_clauses = []
        from_clause = f'"{dataset_table_name}"'

        for par in pars:
            if par in dataframe.columns:
                codelist_table_name = f"codelist_{dataset_code.lower()}_{par.lower()}"
                # Controlla se la codelist esiste nel database
                inspector = inspect(engine)
                if inspector.has_table(codelist_table_name):
                    select_columns.append(
                        f'"{codelist_table_name}".description AS {par}_description'
                    )
                    join_clause = f'LEFT JOIN "{codelist_table_name}" ON "{dataset_table_name}"."{par}" = "{codelist_table_name}"."code"'
                    join_clauses.append(join_clause)
                else:
                    select_columns.append(f'"{dataset_table_name}"."{par}"')
            else:
                logger.warning(
                    f"Parametro '{par}' non presente nel dataframe, ignorato.")

        # Aggiungi le colonne dei dati
        data_columns = [col for col in dataframe.columns if col not in pars]
        for col in data_columns:
            select_columns.append(f'"{dataset_table_name}"."{col}"')

        # Costruisci la query SQL per creare la vista
        select_query = f'SELECT {", ".join(select_columns)} FROM {from_clause} {" ".join(join_clauses)}'

        view_name = f'view_{dataset_table_name}'
        create_view_query = f'CREATE OR REPLACE VIEW "{view_name}" AS {select_query};'

        logger.info(f"Creazione della vista: {view_name}")
        with engine.begin() as connection:
            connection.execute(text(create_view_query))
        logger.info(f"Vista '{view_name}' creata con successo.")
    except Exception as e:
        logger.error(
            f"Errore durante la creazione della vista per il dataset '{dataset_code}': {e}"
        )
        traceback.print_exc()


def create_normalized_view_with_codelists(dataset_code, dataframe, engine):
    """
    Crea una vista normalizzata in PostgreSQL che collega i dati del dataset alle codelist
    in modo generico, adattabile a qualsiasi dataset scaricato.
    """
    try:
        # Ottieni i parametri del dataset (es: freq, geo, sex, ecc.)
        pars = get_pars(dataset_code)
        logger.info(
            f"Parametri ottenuti per il dataset '{dataset_code}': {pars}")

        dataset_table_name = dataset_code.lower().replace('.', '_')

        # Colonne temporali nel dataset (che non sono parametri)
        time_columns = [col for col in dataframe.columns if col not in pars]

        # Creazione dinamica degli array per le colonne temporali
        time_column_array = "ARRAY[" + ", ".join(
            [f"'{col}'" for col in time_columns]) + "]"
        value_array = "ARRAY[" + ", ".join(
            [f'{dataset_table_name}."{col}"' for col in time_columns]) + "]"

        # Creazione dinamica dei join con le codelist
        join_clauses = []
        select_columns = []

        for par in pars:
            codelist_table_name = f"codelist_{dataset_table_name}_{par.lower()}"
            inspector = inspect(engine)
            if inspector.has_table(codelist_table_name):
                join_clauses.append(
                    f'LEFT JOIN {codelist_table_name} ON {dataset_table_name}.{par} = {codelist_table_name}.code'
                )
                select_columns.append(
                    f'{codelist_table_name}.description AS {par}_description')
            else:
                logger.warning(
                    f"Nessuna codelist trovata per il parametro '{par}' nel dataset '{dataset_code}'."
                )

        # Costruzione della query finale per la vista normalizzata
        select_query = f"""
            SELECT
                {', '.join(select_columns)},
                unnest({time_column_array}) AS year,
                unnest({value_array}) AS value
            FROM {dataset_table_name}
            {" ".join(join_clauses)}
        """

        # Nome della vista normalizzata
        view_name = f'view_{dataset_table_name}_normalized'
        create_view_query = f'CREATE OR REPLACE VIEW "{view_name}" AS {select_query};'

        logger.info(
            f"Creazione della vista normalizzata per il dataset '{dataset_code}': {view_name}"
        )

        # Esegui la query per creare la vista
        with engine.begin() as connection:
            connection.execute(text(create_view_query))

        logger.info(f"Vista normalizzata '{view_name}' creata con successo.")
    except Exception as e:
        logger.error(
            f"Errore durante la creazione della vista normalizzata per il dataset '{dataset_code}': {e}"
        )
        traceback.print_exc()


def download_and_save_dataset(node, engine):
    table_name = node['code'].lower().replace('.', '_')
    print(f"\nScaricamento del dataset '{node['name']}' ({node['code']})...")
    try:
        dataframe = get_data_df(node['code'])
        if dataframe is None or dataframe.empty:
            print(f"Dataset '{node['code']}' non trovato o vuoto.")
            return

        dataframe.rename(columns={'geo\\TIME_PERIOD': 'geo'}, inplace=True)
        dataframe.rename(columns={'geo\\TIME PERIOD': 'geo'}, inplace=True)

        print("Colonne del DataFrame scaricato:")
        print(dataframe.columns.tolist())

        drop_table_if_exists(table_name, engine)
        dataframe_to_postgres(dataframe, table_name, engine)

        fetch_and_save_codelists(node['code'], engine)
        create_view_with_codelists(
            node['code'], dataframe,
            engine)  # Correggi il richiamo alla funzione
        create_normalized_view_with_codelists(node['code'], dataframe, engine)
    except Exception as e:
        print(
            f"Errore durante il download o l'inserimento del dataset '{node['code']}': {e}"
        )
        traceback.print_exc()


def fetch_and_save_codelists(dataset_code, engine):
    try:
        pars = get_pars(dataset_code)
        if pars:
            for par in pars:
                dic_df = get_dic(dataset_code, par, frmt='df')
                if dic_df is not None and not dic_df.empty:
                    if 'id' in dic_df.columns:
                        dic_df.rename(columns={'id': 'code'}, inplace=True)
                    if 'label' in dic_df.columns:
                        dic_df.rename(columns={'label': 'description'},
                                      inplace=True)
                    if 'val' in dic_df.columns:
                        dic_df.rename(columns={'val': 'code'}, inplace=True)
                    if 'descr' in dic_df.columns:
                        dic_df.rename(columns={'descr': 'description'},
                                      inplace=True)
                    codelist_table_name = f"codelist_{dataset_code.lower()}_{par.lower()}"
                    drop_table_if_exists(codelist_table_name, engine)
                    dataframe_to_postgres(dic_df, codelist_table_name, engine)
                    print(
                        f"Codelist per il parametro '{par}' salvata nella tabella '{codelist_table_name}'."
                    )
                else:
                    print(
                        f"Nessuna codelist trovata per il parametro '{par}'.")
        else:
            print(f"Nessun parametro trovato per il dataset '{dataset_code}'.")
    except Exception as e:
        print(
            f"Errore durante il download delle codelist per il dataset '{dataset_code}': {e}"
        )
        traceback.print_exc()


def navigate_tree(node, engine):
    while True:
        if node['type'] == 'branch':
            print(f"\nCategoria: {' > '.join(node['path'])}")
            options = node['children']
            choice = display_menu(options)
            if choice is None:
                continue
            elif choice == 0:
                return  # Torna al livello superiore
            else:
                selected_node = options[choice - 1]
                navigate_tree(selected_node, engine)
        elif node['type'] == 'leaf':
            print(
                f"\nSei arrivato al dataset: {node['name']} ({node['code']})")

            if is_dataset_up_to_date(node['code'], engine):
                print(f"Il dataset '{node['name']}' è già aggiornato.")
                return
            else:
                download_choice = input(
                    f"Vuoi scaricare il dataset '{node['name']}' e le sue codelist? (yes/no): "
                ).strip().lower()
                if download_choice == 'yes':
                    download_and_save_dataset(node, engine)
                    update_last_download_date(node['code'], engine)
                return


def main():
    try:
        create_database_if_not_exists()
        db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(db_url)

        create_download_logs_table(engine)

        logger.info("Scaricamento della lista dei dataset da Eurostat.")
        datasets_df = get_toc_df()
        logger.info("Lista dei dataset scaricata con successo.")

        dataframe_to_postgres(datasets_df, 'eurostat_datasets', engine)
        logger.info("Lista dei dataset salvata nel database.")

        if not os.path.isfile(XML_FILE_PATH):
            download_xml(XML_URL, XML_FILE_PATH)

        xml_root = load_xml(XML_FILE_PATH)
        tree = extract_data_from_xml(xml_root)
        if not tree:
            print("Impossibile estrarre la struttura dei dataset.")
            return

        print("Benvenuto nel catalogo dei dataset di Eurostat.")
        navigate_tree(tree, engine)
        print("\nTutti i dataset selezionati sono stati elaborati.")

    except Exception as e:
        logger.error('Errore durante l\'esecuzione dello script: %s', e)
        traceback.print_exc()


if __name__ == '__main__':
    main()
