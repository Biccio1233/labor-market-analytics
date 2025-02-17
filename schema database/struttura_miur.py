#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import traceback
from collections import defaultdict

# URL base delle API CKAN
CKAN_API_URL = 'https://dati-ustat.mur.gov.it/api/3/action'

# Nome del file di output
OUTPUT_FILE = 'struttura_miur.txt'

def get_all_datasets():
    """
    Recupera tutti i dataset (nomi) da CKAN usando package_list,
    poi per ogni dataset_name effettua package_show per ottenere i dettagli.
    Restituisce una lista di 'dataset_dict'.
    """
    url_list = f'{CKAN_API_URL}/package_list'
    print(f"Recupero elenco dataset da: {url_list} ...")
    resp = requests.get(url_list)
    if resp.status_code != 200:
        print(f"ERRORE: Status code {resp.status_code}")
        print(f"Risposta:\n{resp.text[:400]}")
        return []

    try:
        data_list = resp.json()
    except Exception as e:
        print("La risposta non era JSON valido:")
        print(resp.text[:400])
        return []

    if not data_list.get('success'):
        print("ERRORE: la risposta non indica success=True.")
        return []

    dataset_names = data_list.get('result', [])
    if not dataset_names:
        print("Nessun dataset trovato (lista vuota).")
        return []

    print(f"  -> Trovati {len(dataset_names)} dataset_name. Ora chiamo package_show per ciascuno...")
    all_datasets = []
    for i, ds_name in enumerate(dataset_names, start=1):
        show_url = f'{CKAN_API_URL}/package_show?id={ds_name}'
        print(f"[{i}/{len(dataset_names)}] Ottengo dettagli per dataset '{ds_name}' ...")

        r_show = requests.get(show_url)
        if r_show.status_code != 200:
            print(f"  - ERRORE su dataset '{ds_name}': Status code {r_show.status_code}")
            continue

        try:
            data_show = r_show.json()
        except Exception as e:
            print(f"  - ERRORE parse JSON su dataset '{ds_name}': {e}")
            continue

        if not data_show.get('success'):
            print(f"  - ERRORE package_show: success=False su dataset '{ds_name}'")
            continue

        # Salva i dettagli
        result_show = data_show['result']
        all_datasets.append(result_show)

    print(f"\nScaricati con successo i metadati di {len(all_datasets)} dataset.")
    return all_datasets

def build_dataset_list(datasets):
    """
    Raggruppa i dataset per tag, come dict:
      { tag_name : [ { 'type':'dataset', 'name':..., 'id':..., 'notes':... }, ...], ... }
    """
    dataset_by_tag = defaultdict(list)
    for ds in datasets:
        title = ds.get('title') or ds.get('name')
        ds_id = ds.get('name')  # in CKAN, 'name' è lo slug univoco
        notes = ds.get('notes', '')

        tags = ds.get('tags', [])
        tag_names = [tag['name'] for tag in tags] if tags else []
        if not tag_names:
            tag_names = ['Senza Tag']

        for tag in tag_names:
            dataset_by_tag[tag].append({
                'type': 'dataset',
                'name': title,
                'id': ds_id,
                'notes': notes
            })

    return dataset_by_tag

def export_tree_to_txt(dataset_by_tag, output_file=OUTPUT_FILE):
    """
    Produce un file di testo con la struttura:
      Tag: ...
        - dataset: ...
          (notes)
    """
    lines = []
    lines.append("Struttura dataset MUR (CKAN) - raggruppati per Tag")
    lines.append("=================================================")
    lines.append("")

    # Ordina i tag
    sorted_tags = sorted(dataset_by_tag.keys())

    for tag in sorted_tags:
        lines.append(f"TAG: {tag}")
        ds_list = dataset_by_tag[tag]
        # Ordiniamo i dataset per 'name'
        ds_list_sorted = sorted(ds_list, key=lambda x: x['name'].lower())
        for ds_item in ds_list_sorted:
            lines.append(f"   - dataset: {ds_item['name']} (ID: {ds_item['id']})")
            if ds_item['notes']:
                # Se vuoi stampare note multilinea, potresti fare un wrap, ma qui manteniamo testo grezzo
                lines.append(f"       descrizione: {ds_item['notes'][:200]}...")
        lines.append("")

    with open(output_file, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"\nStruttura salvata su file '{output_file}' con successo!")

def main():
    try:
        # 1) Ottieni tutti i dataset
        datasets = get_all_datasets()
        if not datasets:
            print("Nessun dataset disponibile, esco.")
            return

        # 2) Raggruppali per tag
        dataset_by_tag = build_dataset_list(datasets)

        # 3) Esporta la struttura in un file di testo
        export_tree_to_txt(dataset_by_tag, OUTPUT_FILE)

        print("\nFINITO! Controlla il file:", OUTPUT_FILE)
    except Exception as e:
        print("Si è verificato un errore:", e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
