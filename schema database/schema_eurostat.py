#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm

###########################
# CONFIGURAZIONI
###########################

TOC_XML_URL = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml"
OUTPUT_FILE = "struttura_eurostat.txt"

# Namespace per parse del TOC Eurostat
NAMESPACE = {'nt': 'urn:eu.europa.ec.eurostat.navtree'}

###########################
# FUNZIONI DI SUPPORTO
###########################

def download_xml_with_progress(url):
    """
    Scarica l'XML da un URL, mostrando una barra di avanzamento.
    Restituisce la root XML (ElementTree).
    """
    print(f"\nDownload XML da: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers.get('content-length', 0))
    content = bytearray()

    # Ricaviamo un "nome" per la barra di avanzamento
    file_name = os.path.basename(url) or "Eurostat_TOC_XML"

    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Scaricamento {file_name}") as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            content.extend(chunk)
            pbar.update(len(chunk))

    # Parse del contenuto come XML
    root = ET.fromstring(content)
    return root

def get_element_text(el):
    """ Estrae il testo di un elemento (se esiste), strip degli spazi. """
    return el.text.strip() if el is not None and el.text else None

###########################
# PARSING GERARCHIA
###########################

def parse_toc_xml(xml_root):
    """
    Esegue il parse del TOC (catalogue/toc/xml).
    Ciascun <branch> ha:
      <nt:code>, <nt:title language="en">, <nt:children>
    Ciascun <leaf> ha:
      <nt:code>, <nt:title language="en">
    Restituisce una struttura gerarchica (dizionario ricorsivo)
    """
    def parse_branch(branch_el, parent_path):
        code = get_element_text(branch_el.find('nt:code', namespaces=NAMESPACE))
        title_en_el = branch_el.find('nt:title[@language="en"]', namespaces=NAMESPACE)
        title_en = get_element_text(title_en_el)
        node_name = title_en if title_en else code

        current_path = parent_path + [node_name]

        children_data = []
        children_el = branch_el.findall('nt:children/*', namespaces=NAMESPACE)
        for child in children_el:
            if child.tag.endswith('branch'):
                b = parse_branch(child, current_path)
                if b: 
                    children_data.append(b)
            elif child.tag.endswith('leaf'):
                l = parse_leaf(child, current_path)
                if l:
                    children_data.append(l)

        return {
            'type': 'branch',
            'code': code,
            'name': node_name,
            'path': current_path,
            'children': children_data
        }

    def parse_leaf(leaf_el, parent_path):
        code = get_element_text(leaf_el.find('nt:code', namespaces=NAMESPACE))
        title_en_el = leaf_el.find('nt:title[@language="en"]', namespaces=NAMESPACE)
        title_en = get_element_text(title_en_el)
        dataset_name = title_en if title_en else code
        current_path = parent_path + [dataset_name]

        return {
            'type': 'leaf',
            'code': code,
            'name': dataset_name,
            'path': current_path
        }

    # Il root branch dovrebbe essere qualcosa come <nt:branch>...
    branch_root = xml_root.find('.//nt:branch', namespaces=NAMESPACE)
    if not branch_root:
        return None

    return parse_branch(branch_root, [])

###########################
# COSTRUZIONE TESTO E SALVATAGGIO
###########################

def build_text_lines(tree_obj, indent_level=0):
    """
    Trasforma un nodo (branch/leaf) in una lista di righe di testo.
    - "branch": scende ricorsivamente
    - "leaf": rappresenta un dataset
    """
    lines = []
    prefix = "  " * indent_level  # indentazione base

    node_type = tree_obj.get('type')
    node_name = tree_obj.get('name')
    node_code = tree_obj.get('code')

    if node_type == 'branch':
        lines.append(f"{prefix}* {node_name} (codice: {node_code})")
        for child in tree_obj.get('children', []):
            child_lines = build_text_lines(child, indent_level + 1)
            lines.extend(child_lines)
    elif node_type == 'leaf':
        lines.append(f"{prefix}- {node_name} (codice: {node_code})")

    return lines

def export_tree_to_file(tree_obj, output_file):
    """
    Converte l'intero albero in righe di testo e le salva su un file.
    """
    lines = []
    lines.append("Struttura gerarchica Eurostat (TOC) - generata da XML")
    lines.append("=====================================================")
    lines.append("")

    # Se tree_obj è la "branch" radice, la parsi ricorsivamente
    if not tree_obj:
        lines.append("(Nessuna struttura trovata)")
    else:
        lines.extend(build_text_lines(tree_obj, indent_level=0))

    # Salviamo su file
    with open(output_file, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"\nStruttura salvata in '{output_file}' con successo!")

###########################
# MAIN
###########################

def main():
    print("=== Scaricamento e parse TOC (Eurostat) ===")
    xml_root = download_xml_with_progress(TOC_XML_URL)

    print("\n=== Parsing gerarchia TOC ===")
    tree_obj = parse_toc_xml(xml_root)
    if not tree_obj:
        print("Impossibile estrarre la struttura del TOC.")
        return

    print("=== Costruzione file testuale ad albero ===")
    export_tree_to_file(tree_obj, OUTPUT_FILE)

    print("\nScript completato. Puoi aprire il file:", OUTPUT_FILE)

if __name__ == "__main__":
    main()
