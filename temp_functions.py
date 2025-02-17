def get_table_metadata(conn, table_name):
    """
    Recupera i metadati della tabella dal dataflow corrispondente.
    Restituisce una tupla (descrizione_it, descrizione_en).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT df.nome_it, df.nome_en
            FROM dataflow df
            WHERE df.id = %s
        """, (table_name.split('_')[0],))
        result = cur.fetchone()
        if result:
            return result
        return None, None

def create_available_views_view(conn):
    """
    Crea o aggiorna la vista available_views che mostra tutte le viste disponibili
    con le loro descrizioni.
    """
    query = """
    CREATE OR REPLACE VIEW available_views AS
    SELECT 
        views.viewname as view_name,
        views.definition as view_definition,
        pg_description.description as view_description
    FROM pg_views views
    LEFT JOIN pg_description ON 
        pg_description.objoid = (SELECT oid FROM pg_class WHERE relname = views.viewname)
    WHERE views.schemaname = 'public'
    ORDER BY views.viewname;
    """
    
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            conn.commit()
            print("Vista available_views creata/aggiornata con successo!")
        except Exception as e:
            print(f"Errore nella creazione della vista available_views: {e}")
            conn.rollback()

def execute_part3(conn, tables_to_download):
    """
    Esecuzione Parte 3: Creazione delle viste, con nome = NomeIt_sanificato_[ID].
    """
    print("\nParte 3: Creazione delle viste...")

    for table_id in tables_to_download:
        print(f"\nProcesso la tabella {table_id}...")

        # Recupera il nome italiano del dataflow e i metadati
        nome_it = get_dataflow_name(conn, table_id.split('_')[0])
        desc_it, desc_en = get_table_metadata(conn, table_id)
        
        # Crea il nome vista: nome_sanificato_ID
        view_name = f"{sanitize_for_view_name(nome_it)}_{table_id}"
        print(f"Nome vista: {view_name}")

        # Recupera il mapping dimension -> enum_table
        enum_cl_mapping = get_enum_cl_mapping(conn, table_id)
        print(f"Mapping classificazioni: {enum_cl_mapping}")

        # Costruisci i JOIN
        joins = build_joins(table_id, enum_cl_mapping)

        # Crea e esegui la query
        query = create_view_query(table_id, joins, enum_cl_mapping, view_name)
        comment_query = f"""
        COMMENT ON VIEW {view_name} IS E'Contenuto: {desc_it}\nContent: {desc_en}';
        """
        
        print("\nCreo la vista...")
        
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                if desc_it or desc_en:
                    cur.execute(comment_query)
                conn.commit()
                print("Vista creata con successo!")
            except Exception as e:
                print(f"Errore nella creazione della vista: {e}")
                conn.rollback()
    
    # Aggiorna la vista available_views dopo aver creato tutte le nuove viste
    create_available_views_view(conn)

def main():
    # Assicurati che il database esista
    ensure_database_exists()

    # Connettiti al database
    conn = connect_to_database()

    try:
        # Parte 1: Scarica e salva Dataflow e Datastructure
        execute_part1(conn)

        # Parte 1-bis: Popola le categorie
        populate_categories(conn)
        execute_category_mapping(conn)
        create_dataflow_category_view(conn)

        # Crea la vista delle viste disponibili
        create_available_views_view(conn)

        # Chiedi all'utente di selezionare una categoria
        category_id = select_category(conn)
        if not category_id:
            print("\nCategoria non valida o non selezionata. Esco.")
            return

        # Recupera le tabelle della categoria selezionata
        with conn.cursor() as cur:
            cur.execute("""
                SELECT df.id || '_' || df.ref_id as table_id
                FROM dataflow_categories dc
                JOIN dataflow df ON df.id = dc.dataflow_id
                WHERE dc.category_id = %s
                ORDER BY df.id
            """, (category_id,))
            tables_to_download = [row[0] for row in cur.fetchall()]

        if not tables_to_download:
            print("\nNessuna tabella trovata per la categoria selezionata. Esco.")
            return

        print(f"\nTabelle da scaricare: {tables_to_download}")

        # Parte 2: Scarica le tabelle CSV e le classificazioni
        execute_part2(conn, tables_to_download)

        # Parte 3: Crea le viste
        execute_part3(conn, tables_to_download)

        # Mostra le viste disponibili
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM available_views")
            views = cur.fetchall()
            print("\nViste disponibili:")
            for view in views:
                print(f"\nNome vista: {view[0]}")
                if view[2]:  # se c'è una descrizione
                    print(f"Descrizione: {view[2]}")

    except Exception as e:
        print(f"\nErrore durante l'esecuzione: {e}")

    finally:
        conn.close()
