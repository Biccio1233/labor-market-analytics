#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm

########################################
# CONFIGURAZIONI
########################################

# Cartella di destinazione per eventuali file scaricati (se necessario)
DOWNLOAD_DIR = os.path.join(os.getcwd(), "istat_albero")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# File di output testuale con la struttura
OUTPUT_FILE = "struttura_istat.txt"

# Namespace per XML SDMX
NAMESPACES = {
    'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'xml': 'http://www.w3.org/XML/1998/namespace'
}

# Endpoint ISTAT (SDMX)
CATEGORYSCHEME_URL = 'http://sdmx.istat.it/SDMXWS/rest/categoryscheme/IT1/ALL/latest'
DATAFLOW_URL = 'http://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/ALL/latest'
DATASTRUCTURE_URL = 'http://sdmx.istat.it/SDMXWS/rest/datastructure/IT1/ALL/latest'

########################################
# 1) SCARICA E PARSA IL FILE XML (con barra di avanzamento)
########################################

def download_xml(url):
    """
    Scarica il contenuto XML da un URL, mostrando una barra di avanzamento,
    e restituisce la root di ElementTree.
    """
    print(f"\nDownload XML da: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers.get("content-length", 0))
    content = bytearray()

    # Nome di base da mostrare nella barra
    file_name = os.path.basename(url) if os.path.basename(url) else "ISTAT_XML"

    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Scaricamento {file_name}") as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            content.extend(chunk)
            pbar.update(len(chunk))

    return ET.fromstring(content)

########################################
# 2) PARSE CATEGORYSCHEME
########################################

def parse_categories(root):
    """
    Estrae le categorie (id, name_it, name_en) dal CategoryScheme XML.
    Ritorna un dizionario { 'cat_id' : { 'name_it':..., 'name_en':... }, ... }
    """
    categories = {}
    for cat_scheme in root.findall('.//structure:CategoryScheme', namespaces=NAMESPACES):
        for cat_el in cat_scheme.findall('.//structure:Category', namespaces=NAMESPACES):
            cat_id = cat_el.attrib.get('id', '')
            name_it_el = cat_el.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
            name_en_el = cat_el.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)

            categories[cat_id] = {
                'name_it': name_it_el.text if name_it_el is not None else None,
                'name_en': name_en_el.text if name_en_el is not None else None
            }
    return categories

########################################
# 3) PARSE DATAFLOW
########################################

def parse_dataflow(root):
    """
    Estrae i dataflow e ritorna una lista di dict, ciascuno con:
      {
         'id': ...,
         'name_it': ...,
         'name_en': ...,
         'ref_id': ... (datastructure ID),
         ...
      }
    """
    dataflows = []
    for el in root.findall('.//structure:Dataflow', namespaces=NAMESPACES):
        df_id = el.attrib.get('id')
        agency = el.attrib.get('agencyID')
        version = el.attrib.get('version')

        ref_el = el.find('.//structure:Structure/Ref', namespaces=NAMESPACES)
        ref_id = ref_el.attrib.get('id') if ref_el is not None else None
        package = ref_el.attrib.get('package') if ref_el is not None else None

        name_it_el = el.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
        name_en_el = el.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)

        name_it = name_it_el.text if name_it_el is not None else None
        name_en = name_en_el.text if name_en_el is not None else None

        dataflows.append({
            'id': df_id,
            'name_it': name_it,
            'name_en': name_en,
            'agency': agency,
            'version': version,
            'ref_id': ref_id,
            'package': package
        })
    return dataflows

########################################
# 4) PARSE DATASTRUCTURE + DETTAGLI
########################################

def parse_datastructure(root):
    """
    Estrae:
      - un dict "datastructures" con { ds_id: { 'id':..., 'name_it':..., ... } }
      - una lista "details" con i record di dimensioni (enum_id) ecc.
    """
    datastructures = {}
    details = []

    for ds_el in root.findall('.//structure:DataStructure', namespaces=NAMESPACES):
        ds_id = ds_el.attrib.get('id')
        agency = ds_el.attrib.get('agencyID')
        version = ds_el.attrib.get('version')

        name_it_el = ds_el.find('.//common:Name[@xml:lang="it"]', namespaces=NAMESPACES)
        name_en_el = ds_el.find('.//common:Name[@xml:lang="en"]', namespaces=NAMESPACES)
        name_it = name_it_el.text if name_it_el is not None else None
        name_en = name_en_el.text if name_en_el is not None else None

        datastructures[ds_id] = {
            'id': ds_id,
            'agency': agency,
            'version': version,
            'name_it': name_it,
            'name_en': name_en
        }

        # Ora estraiamo dimension, attribute, measure
        for detail_el in (ds_el.findall('.//structure:Dimension', namespaces=NAMESPACES) +
                          ds_el.findall('.//structure:Attribute', namespaces=NAMESPACES) +
                          ds_el.findall('.//structure:Measure', namespaces=NAMESPACES)):
            detail_id = detail_el.attrib.get('id')
            detail_type = detail_el.tag.split('}')[-1]

            concept_ref = detail_el.find('.//structure:ConceptIdentity/Ref', namespaces=NAMESPACES)
            concept_id = concept_ref.attrib.get('id') if concept_ref is not None else None

            local_rep = detail_el.find('.//structure:LocalRepresentation/structure:Enumeration/Ref', namespaces=NAMESPACES)
            enum_id = local_rep.attrib.get('id') if local_rep is not None else None

            details.append({
                'datastructure_id': ds_id,
                'type': detail_type,   # 'Dimension' / 'Attribute' / 'Measure'
                'detail_id': detail_id,
                'concept_id': concept_id,
                'enum_id': enum_id
            })

    return datastructures, details

########################################
# 5) COSTRUZIONE ALBERO E SALVATAGGIO
########################################

def build_tree_and_write_file(categories, dataflows, datastructures, details, output_file=OUTPUT_FILE):
    """
    Crea la "fotografia" ad albero:
      * Categoria (cat_id) - name
         - Dataflow (df_id) con nome
            -> dimension: detail_id, enum_id (se esiste) ...
    
    E salva tutto in un file di testo formattato ad albero.
    """

    # 5.1) Costruiamo un mapping dataflow->category
    cat_to_dataflows = {}  # { cat_id: [ dataflow_dict, ... ] }

    for df in dataflows:
        df_id = df['id']
        # Cat prefix
        if '_' in df_id:
            cat_prefix = df_id.split('_')[0]
            if cat_prefix in categories:
                cat_to_dataflows.setdefault(cat_prefix, []).append(df)
            else:
                cat_to_dataflows.setdefault('NO_CAT', []).append(df)
        else:
            cat_to_dataflows.setdefault('NO_CAT', []).append(df)

    # 5.2) Creiamo un mapping da datastructure -> [ details... ] (solo dimensioni con enum_id)
    ds_to_dimensions = {}
    for d in details:
        ds_id = d['datastructure_id']
        if d['type'] == 'Dimension':
            ds_to_dimensions.setdefault(ds_id, []).append(d)

    # 5.3) Costruiamo le linee di testo
    lines = []
    lines.append("STRUTTURA ISTAT ALBERO - ESTRAZIONE DA SDMX WS (con barra di avanzamento download)")
    lines.append("====================================================================")
    lines.append("")

    # Ordiniamo le categorie in base al cat_id
    sorted_cat_ids = sorted(categories.keys())

    for cat_id in sorted_cat_ids:
        cat_data = categories[cat_id]
        cat_name = cat_data['name_it'] or cat_data['name_en'] or "N/A"

        lines.append(f"* Categoria: {cat_id}  (Nome: {cat_name})")

        df_list = cat_to_dataflows.get(cat_id, [])
        if not df_list:
            lines.append("   - Nessun dataflow associato a questa categoria.")
            lines.append("")
            continue

        # Ordiniamo i dataflow per ID
        df_list_sorted = sorted(df_list, key=lambda x: x['id'])

        for df_item in df_list_sorted:
            df_id = df_item['id']
            df_name = df_item['name_it'] or df_item['name_en'] or "N/A"
            ref_id = df_item['ref_id']

            lines.append(f"   - Dataflow: {df_id}  (Nome: {df_name})")

            # Troviamo la datastructure corrispondente
            if not ref_id or ref_id not in datastructures:
                lines.append("       (DataStructure non trovata o ref_id mancante)")
                lines.append("")
                continue

            ds_item = datastructures[ref_id]
            ds_name = ds_item['name_it'] or ds_item['name_en'] or "N/A"

            lines.append(f"       -> DataStructure: {ref_id} (Nome: {ds_name})")

            # Dimensioni
            dim_list = ds_to_dimensions.get(ref_id, [])
            if not dim_list:
                lines.append("           Nessuna dimensione con codelist associata trovata.")
                lines.append("")
                continue

            for dim in dim_list:
                detail_id = dim['detail_id']
                enum_id = dim['enum_id']
                if enum_id:
                    lines.append(f"           -> Dimensione: {detail_id}, codelist = {enum_id}")
                else:
                    lines.append(f"           -> Dimensione: {detail_id} (no codelist)")

            lines.append("")

        lines.append("")

    # 5.4) Potrebbe esserci la "categoria" fittizia 'NO_CAT'
    if 'NO_CAT' in cat_to_dataflows:
        lines.append("* CATEGORIA SCONOSCIUTA (Dataflow senza prefisso underscore)")
        df_list = cat_to_dataflows['NO_CAT']
        for df_item in sorted(df_list, key=lambda x: x['id']):
            df_id = df_item['id']
            df_name = df_item['name_it'] or df_item['name_en'] or "N/A"
            lines.append(f"   - Dataflow: {df_id} (Nome: {df_name})")
        lines.append("")

    # 5.5) Salviamo lines su file
    with open(output_file, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"\nStruttura ISTAT salvata in '{output_file}' con successo!")


########################################
# MAIN
########################################

def main():
    print("=== Scaricamento CategoryScheme ===")
    cat_root = download_xml(CATEGORYSCHEME_URL)

    print("=== Scaricamento Dataflow ===")
    df_root = download_xml(DATAFLOW_URL)

    print("=== Scaricamento DataStructure ===")
    ds_root = download_xml(DATASTRUCTURE_URL)

    # Parse
    print("\nParsing CategoryScheme...")
    categories = parse_categories(cat_root)
    print(f"  -> Trovate {len(categories)} categorie.\n")

    print("Parsing Dataflow...")
    dataflows = parse_dataflow(df_root)
    print(f"  -> Trovati {len(dataflows)} dataflow.\n")

    print("Parsing DataStructure...")
    datastructures, details = parse_datastructure(ds_root)
    print(f"  -> Trovate {len(datastructures)} datastructure e {len(details)} dettagli.\n")

    # Costruisci "albero" e salvalo in OUTPUT_FILE
    build_tree_and_write_file(categories, dataflows, datastructures, details, OUTPUT_FILE)
    print("\nFine script.")

if __name__ == "__main__":
    main()
