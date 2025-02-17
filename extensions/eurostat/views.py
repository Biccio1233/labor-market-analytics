from flask_appbuilder import BaseView, expose
from flask import flash, redirect, request, url_for, session
from sqlalchemy import create_engine, text
import pandas as pd
from eurostat import get_toc_df
import xml.etree.ElementTree as ET

from superset import db
from .utils import download_dataset, create_dataset_view

# Namespace del file XML
NAMESPACE = {'nt': 'urn:eu.europa.ec.eurostat.navtree'}

class EurostatViewsManager(BaseView):
    route_base = "/eurostat/views"
    
    def get_node_children(self, node):
        """Estrae i nodi figli da un nodo XML"""
        children = []
        for child in node.findall('nt:children/nt:node', NAMESPACE):
            title = child.find('nt:title', NAMESPACE).text
            code = child.find('nt:code', NAMESPACE).text if child.find('nt:code', NAMESPACE) is not None else None
            children.append({
                'title': title,
                'code': code,
                'is_leaf': child.find('nt:code', NAMESPACE) is not None
            })
        return children
    
    @expose('/')
    def list(self):
        # Ottieni la lista delle viste dal view_catalog
        engine = create_engine(db.get_sqla_engine().url)
        query = text("""
            SELECT view_name, dataset_code, dataset_title, created_at 
            FROM eurostat.view_catalog 
            ORDER BY created_at DESC
        """)
        
        try:
            with engine.connect() as conn:
                views = pd.read_sql(query, conn)
                
                # Ottieni la struttura del TOC
                toc_df = get_toc_df()
                root_categories = toc_df[toc_df['parent_code'].isna()]
                
                return self.render_template(
                    'eurostat/views_list.html',
                    views=views.to_dict('records'),
                    categories=root_categories.to_dict('records')
                )
        except Exception as e:
            flash(f"Error loading views: {str(e)}", "error")
            return redirect('/')
    
    @expose('/browse/<path:category_path>')
    def browse(self, category_path):
        try:
            # Decodifica il percorso della categoria
            path_parts = category_path.split('/')
            
            # Ottieni il TOC
            toc_df = get_toc_df()
            
            # Filtra per il percorso corrente
            current_level = toc_df
            for part in path_parts:
                current_level = current_level[current_level['parent_code'] == part]
            
            # Prepara il breadcrumb
            breadcrumb = []
            current_path = ''
            for part in path_parts:
                current_path = f"{current_path}/{part}" if current_path else part
                title = toc_df[toc_df['code'] == part]['title'].iloc[0]
                breadcrumb.append({
                    'code': part,
                    'title': title,
                    'path': current_path
                })
            
            return self.render_template(
                'eurostat/browse.html',
                categories=current_level.to_dict('records'),
                breadcrumb=breadcrumb,
                current_path=category_path
            )
            
        except Exception as e:
            flash(f"Error browsing category: {str(e)}", "error")
            return redirect(url_for('.list'))
    
    @expose('/refresh/<dataset_code>')
    def refresh(self, dataset_code):
        try:
            # Aggiorna il dataset
            success = download_dataset(dataset_code)
            if success:
                flash(f"Dataset {dataset_code} updated successfully", "success")
            else:
                flash(f"Error updating dataset {dataset_code}", "error")
                
        except Exception as e:
            flash(f"Error: {str(e)}", "error")
            
        return redirect(url_for('.list'))

class EurostatDatasetManager(BaseView):
    route_base = "/eurostat/datasets"
    
    @expose('/')
    def list(self):
        # Ottieni la lista dei dataset dal view_catalog
        engine = create_engine(db.get_sqla_engine().url)
        query = text("""
            SELECT DISTINCT dataset_code, dataset_title, created_at
            FROM eurostat.view_catalog 
            ORDER BY created_at DESC
        """)
        
        try:
            with engine.connect() as conn:
                datasets = pd.read_sql(query, conn)
                return self.render_template(
                    'eurostat/datasets.html',
                    datasets=datasets.to_dict('records')
                )
        except Exception as e:
            flash(f"Error loading datasets: {str(e)}", "error")
            return redirect('/')
    
    @expose('/download/<dataset_code>')
    def download(self, dataset_code):
        if not dataset_code:
            flash("Dataset code is required", "error")
            return redirect(url_for('.list'))
            
        try:
            # Verifica se il dataset è già aggiornato
            engine = create_engine(db.get_sqla_engine().url)
            query = text("""
                SELECT last_download_date 
                FROM eurostat.download_logs 
                WHERE dataset_code = :code
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {"code": dataset_code}).fetchone()
                if result:
                    flash(f"Dataset {dataset_code} is already up to date (last download: {result[0]})", "info")
                    return redirect(url_for('.list'))
            
            # Scarica il dataset e le codelist
            success = download_dataset(dataset_code)
            if success:
                flash(f"Dataset {dataset_code} and its codelists downloaded successfully", "success")
            else:
                flash(f"Error downloading dataset {dataset_code}", "error")
                
        except Exception as e:
            flash(f"Error: {str(e)}", "error")
            
        return redirect(url_for('.list'))
