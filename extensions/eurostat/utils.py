import os
import pandas as pd
from sqlalchemy import create_engine, text
from flask import current_app
from superset import db
import eurostat
from datetime import datetime

def get_db_engine():
    """Ottiene l'engine del database configurato in Superset"""
    return create_engine(db.get_sqla_engine().url)

def download_dataset(dataset_code):
    """
    Scarica un dataset da Eurostat e lo salva nel database
    """
    try:
        # Scarica il dataset usando la libreria eurostat
        df = eurostat.get_data_df(dataset_code)
        if df is None or df.empty:
            return False
            
        # Ottieni i metadati del dataset
        pars = eurostat.get_pars(dataset_code)
        
        # Connessione al database
        engine = get_db_engine()
        
        # Salva il dataset grezzo
        table_name = f"{dataset_code}_raw"
        df.to_sql(
            table_name,
            engine,
            schema='eurostat',
            if_exists='replace',
            index=True
        )
        
        # Salva le codelist
        for param, values in pars.items():
            codelist_df = pd.DataFrame(values.items(), columns=['code', 'description'])
            codelist_df.to_sql(
                f"{dataset_code}_{param}_codelist",
                engine,
                schema='eurostat',
                if_exists='replace',
                index=False
            )
            
        # Aggiorna il log dei download
        update_download_log(dataset_code, engine)
        
        return True
        
    except Exception as e:
        current_app.logger.error(f"Error downloading dataset {dataset_code}: {str(e)}")
        return False

def update_download_log(dataset_code, engine):
    """
    Aggiorna il log dei download
    """
    query = text("""
        INSERT INTO eurostat.download_logs (dataset_code, last_download_date)
        VALUES (:code, :date)
        ON CONFLICT (dataset_code) 
        DO UPDATE SET last_download_date = :date
    """)
    
    with engine.connect() as conn:
        conn.execute(
            query,
            {"code": dataset_code, "date": datetime.utcnow()}
        )
        conn.commit()

def create_dataset_view(dataset_code):
    """
    Crea una vista per il dataset con join alle codelist
    """
    engine = get_db_engine()
    
    # Ottieni i metadati
    pars = eurostat.get_pars(dataset_code)
    
    # Costruisci la query per la vista
    base_table = f"{dataset_code}_raw"
    joins = []
    select_cols = []
    
    # Aggiungi le colonne della tabella base
    select_cols.append(f"{base_table}.*")
    
    # Aggiungi i join per ogni codelist
    for param in pars.keys():
        codelist_table = f"{dataset_code}_{param}_codelist"
        joins.append(f"""
            LEFT JOIN eurostat.{codelist_table} AS {param}_list 
            ON {base_table}.{param} = {param}_list.code
        """)
        select_cols.append(f"{param}_list.description AS {param}_description")
    
    # Costruisci la query finale
    view_query = f"""
        CREATE OR REPLACE VIEW eurostat.{dataset_code}_view AS
        SELECT {', '.join(select_cols)}
        FROM eurostat.{base_table}
        {' '.join(joins)}
    """
    
    # Esegui la query
    with engine.connect() as conn:
        conn.execute(text(view_query))
        conn.commit()
