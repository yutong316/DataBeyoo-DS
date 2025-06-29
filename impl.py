# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import json
import sqlite3
import pandas as pd
import requests
from urllib.parse import quote

class IdentifiableEntity:
    """Abstract base class representing an entity with a unique identifier."""
    def __init__(self, id):
        self.id = id
    
    def getIds(self):
        return [self.id]

class Journal(IdentifiableEntity):
    """Represents a journal with metadata including title, languages, publisher, license, APC status, categories, and areas."""
    def __init__(self, id, title, languages=None, publisher=None, seal=False, licence=None, apc=False, categories=None, areas=None):
        super().__init__(id)
        self.title = title
        self.languages = languages if languages is not None else []
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
        self._categories = categories if categories is not None else []
        self._areas = areas if areas is not None else []
    
    def getTitle(self):
        return self.title
    
    def getLanguages(self):
        return self.languages
    
    def getPublisher(self):
        return self.publisher
    
    def hasDOAJSeal(self):
        return self.seal
    
    def getLicence(self):
        return self.licence
    
    def hasAPC(self):
        return self.apc
    
    def getCategories(self):
        return self._categories
    
    def getAreas(self):
        return self._areas

class Category(IdentifiableEntity):
    def __init__(self, id, quartile=None):
        super().__init__(id)
        self.quartile = quartile
    
    def getQuartile(self):
        return self.quartile

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, path_or_url):
        self.dbPathOrUrl = path_or_url
        return True

class UploadHandler(Handler):
    def pushDataToDb(self, file_path):
        pass

class JournalUploadHandler(UploadHandler):
    def pushDataToDb(self, file_path):
        if not os.path.exists(file_path):
            return False
        
        journals = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                journals.append(row)
        
        for journal in journals:
            # Generate a unique ID based on the title
            journal_id = journal['Journal title'].replace(' ', '_') if journal['Journal title'] else ""
            if not journal_id:
                continue
            
            # Process ISSN numbers
            issn_p = journal.get('Journal ISSN (print version)', "").strip()
            issn_e = journal.get('Journal EISSN (online version)', "").strip()
            issns = []
            if issn_p:
                issns.append(issn_p)
            if issn_e:
                issns.append(issn_e)
            
            # Process the language field (may contain multiple languages, separated by ", ")
            languages = journal.get('Languages in which the journal accepts manuscripts', "").split(", ")
            languages = [lang.strip() for lang in languages if lang.strip()]
            
            # Process DOAJ Seal and APC fields
            has_seal = journal.get('DOAJ Seal', "No").strip().lower() == "yes"
            has_apc = journal.get('APC', "No").strip().lower() == "yes"
            
            # Build SPARQL query
            query = f"""
            PREFIX schema: <http://schema.org/>
            PREFIX dc: <http://purl.org/dc/terms/>
            
            INSERT DATA {{
                <http://example.org/journal/{quote(journal_id)}> a schema:Periodical ;
                    dc:title {json.dumps(journal['Journal title'])} ;
            """
            
            for issn in issns:
                query += f'    dc:identifier {json.dumps(issn)} ;\n'
            
            for lang in languages:
                query += f'    dc:language {json.dumps(lang)} ;\n'
            
            if journal.get('Publisher'):
                query += f'    dc:publisher {json.dumps(journal["Publisher"])} ;\n'
            
            if journal.get('Journal license'):
                query += f'    dc:license {json.dumps(journal["Journal license"])} ;\n'

            # Add DOAJ Seal and APC properties
            query += f'    schema:doajSeal "{has_seal}" ;\n'
            query += f'    schema:apc "{has_apc}" .\n'
            query += '}'
            
            # Send SPARQL query to Blazegraph
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            payload = {'update': query}
            try:
                response = requests.post(self.dbPathOrUrl, headers=headers, data=payload)
                if response.status_code != 200:
                    print(f"Error uploading journal {journal_id}: {response.text}")
            except Exception as e:
                print(f"Error during SPARQL query execution: {e}")
        
        return True

class CategoryUploadHandler(UploadHandler):
    def pushDataToDb(self, file_path):
        if not os.path.exists(file_path):
            return False

        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Connect to SQLite database
        conn = sqlite3.connect(self.dbPathOrUrl)
        cursor = conn.cursor()

        # Create necessary tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS journals (
            id TEXT PRIMARY KEY,
            identifier TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            id TEXT PRIMARY KEY
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS areas (
            id TEXT PRIMARY KEY
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS journal_categories (
            journal_id TEXT,
            category_id TEXT,
            quartile TEXT,
            PRIMARY KEY (journal_id, category_id),
            FOREIGN KEY (journal_id) REFERENCES journals(id),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS category_areas (
            category_id TEXT,
            area_id TEXT,
            PRIMARY KEY (category_id, area_id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            FOREIGN KEY (area_id) REFERENCES areas(id)
        )
        ''')
        
        # Process and upload data
        for item in data:
            identifiers = item.get('identifiers', [])
            categories = item.get('categories', [])
            areas = item.get('areas', [])

            # Process each ISSN
            for identifier in identifiers:
                # Add journal
                cursor.execute("INSERT OR IGNORE INTO journals (id, identifier) VALUES (?, ?)",
                               (identifier, identifier))

                # Add categories and journal-category relationships
                for category in categories:
                    cat_id = category.get('id')
                    quartile = category.get('quartile')
                    
                    if cat_id:
                        # Add category
                        cursor.execute("INSERT OR IGNORE INTO categories (id) VALUES (?)", 
                                      (cat_id,))

                        # Add journal-category relationship
                        cursor.execute(
                            "INSERT OR IGNORE INTO journal_categories (journal_id, category_id, quartile) VALUES (?, ?, ?)", 
                            (identifier, cat_id, quartile))

                # Add areas and category-area relationships
                for area_id in areas:
                    if area_id:
                        # Add area
                        cursor.execute("INSERT OR IGNORE INTO areas (id) VALUES (?)",
                                      (area_id,))

                        # Add category-area relationship for each category
                        for category in categories:
                            cat_id = category.get('id')
                            if cat_id:
                                cursor.execute(
                                    "INSERT OR IGNORE INTO category_areas (category_id, area_id) VALUES (?, ?)", 
                                    (cat_id, area_id))

        # Commit transaction and close connection
        conn.commit()
        conn.close()
        
        return True

class QueryHandler(Handler):
    """Handles generic SQL query operations for a given database."""
    def getById(self, id):
        pass

class JournalQueryHandler(QueryHandler):
    """Executes journal-related SQL queries and returns results as DataFrames."""
    def getById(self, id):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {{
            ?journal dc:identifier "{id}" .
            ?journal dc:title ?title .
            OPTIONAL {{ ?journal dc:language ?language }}
            OPTIONAL {{ ?journal dc:publisher ?publisher }}
            OPTIONAL {{ ?journal dc:license ?license }}
            OPTIONAL {{ ?journal schema:doajSeal ?seal }}
            OPTIONAL {{ ?journal schema:apc ?apc }}
        }}
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getAllJournals(self):
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            OPTIONAL { ?journal dc:language ?language }
            OPTIONAL { ?journal dc:publisher ?publisher }
            OPTIONAL { ?journal dc:license ?license }
            OPTIONAL { ?journal schema:doajSeal ?seal }
            OPTIONAL { ?journal schema:apc ?apc }
        }
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getJournalsWithTitle(self, partial_title):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {{
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            FILTER(CONTAINS(LCASE(?title), LCASE("{partial_title}")))
            OPTIONAL {{ ?journal dc:language ?language }}
            OPTIONAL {{ ?journal dc:publisher ?publisher }}
            OPTIONAL {{ ?journal dc:license ?license }}
            OPTIONAL {{ ?journal schema:doajSeal ?seal }}
            OPTIONAL {{ ?journal schema:apc ?apc }}
        }}
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getJournalsPublishedBy(self, partial_name):
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {{
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            ?journal dc:publisher ?publisher .
            FILTER(CONTAINS(LCASE(?publisher), LCASE("{partial_name}")))
            OPTIONAL {{ ?journal dc:language ?language }}
            OPTIONAL {{ ?journal dc:license ?license }}
            OPTIONAL {{ ?journal schema:doajSeal ?seal }}
            OPTIONAL {{ ?journal schema:apc ?apc }}
        }}
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getJournalsWithLicense(self, licenses):
        # Build FILTER conditions
        license_conditions = " || ".join([f'?license = "{license}"' for license in licenses])
        
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {{
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            ?journal dc:license ?license .
            FILTER({license_conditions})
            OPTIONAL {{ ?journal dc:language ?language }}
            OPTIONAL {{ ?journal dc:publisher ?publisher }}
            OPTIONAL {{ ?journal schema:doajSeal ?seal }}
            OPTIONAL {{ ?journal schema:apc ?apc }}
        }}
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getJournalsWithAPC(self):
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            ?journal schema:apc "true" .
            OPTIONAL { ?journal dc:language ?language }
            OPTIONAL { ?journal dc:publisher ?publisher }
            OPTIONAL { ?journal dc:license ?license }
            OPTIONAL { ?journal schema:doajSeal ?seal }
        }
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self):
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX dc: <http://purl.org/dc/terms/>
        
        SELECT ?journal ?title ?language ?publisher ?license ?seal ?apc
        WHERE {
            ?journal a schema:Periodical .
            ?journal dc:title ?title .
            ?journal schema:doajSeal "true" .
            OPTIONAL { ?journal dc:language ?language }
            OPTIONAL { ?journal dc:publisher ?publisher }
            OPTIONAL { ?journal dc:license ?license }
            OPTIONAL { ?journal schema:apc ?apc }
        }
        """
        
        headers = {'Accept': 'application/json'}
        payload = {'query': query}
        response = requests.get(self.dbPathOrUrl, headers=headers, params=payload)
        
        if response.status_code == 200:
            results = response.json()
            if 'results' in results and 'bindings' in results['results']:
                df = pd.json_normalize(results['results']['bindings'])
                if not df.empty:
                    # Rename columns and process values
                    df_processed = pd.DataFrame()
                    for col in df.columns:
                        col_name = col.split('.')[0]
                        df_processed[col_name] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else None)
                    return df_processed
        
        return pd.DataFrame()

class CategoryQueryHandler(QueryHandler):
    """Executes category-related SQL queries and returns results as DataFrames."""
    def getById(self, id):
        conn = sqlite3.connect(self.dbPathOrUrl)

        # Try to query from the categories table
        query = """
        SELECT c.id as category_id, jc.quartile, a.id as area_id
        FROM categories c
        LEFT JOIN journal_categories jc ON c.id = jc.category_id
        LEFT JOIN category_areas ca ON c.id = ca.category_id
        LEFT JOIN areas a ON ca.area_id = a.id
        WHERE c.id = ?
        """
        df_category = pd.read_sql_query(query, conn, params=(id,))

        # If not found in categories table, try querying from areas table
        if df_category.empty or df_category['category_id'].iloc[0] is None:
            query = """
            SELECT a.id as area_id, c.id as category_id, jc.quartile
            FROM areas a
            LEFT JOIN category_areas ca ON a.id = ca.area_id
            LEFT JOIN categories c ON ca.category_id = c.id
            LEFT JOIN journal_categories jc ON c.id = jc.category_id
            WHERE a.id = ?
            """
            df_area = pd.read_sql_query(query, conn, params=(id,))
            conn.close()
            return df_area
        
        conn.close()
        return df_category
    
    def getAllCategories(self):
        conn = sqlite3.connect(self.dbPathOrUrl)
        query = """
        SELECT DISTINCT c.id as category_id, jc.quartile
        FROM categories c
        LEFT JOIN journal_categories jc ON c.id = jc.category_id
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def getAllAreas(self):
        conn = sqlite3.connect(self.dbPathOrUrl)
        query = """
        SELECT DISTINCT id as area_id
        FROM areas
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def getCategoriesWithQuartile(self, quartiles):
        conn = sqlite3.connect(self.dbPathOrUrl)
        
        if quartiles:
            placeholders = ', '.join(['?' for _ in quartiles])
            query = f"""
            SELECT DISTINCT c.id as category_id, jc.quartile
            FROM categories c
            JOIN journal_categories jc ON c.id = jc.category_id
            WHERE jc.quartile IN ({placeholders})
            """
            df = pd.read_sql_query(query, conn, params=tuple(quartiles))
        else:
            query = """
            SELECT DISTINCT c.id as category_id, jc.quartile
            FROM categories c
            LEFT JOIN journal_categories jc ON c.id = jc.category_id
            """
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    
    def getCategoriesAssignedToAreas(self, area_ids):
        conn = sqlite3.connect(self.dbPathOrUrl)
        
        if area_ids:
            placeholders = ', '.join(['?' for _ in area_ids])
            query = f"""
            SELECT DISTINCT c.id as category_id, a.id as area_id, jc.quartile
            FROM categories c
            JOIN category_areas ca ON c.id = ca.category_id
            JOIN areas a ON ca.area_id = a.id
            LEFT JOIN journal_categories jc ON c.id = jc.category_id
            WHERE a.id IN ({placeholders})
            """
            df = pd.read_sql_query(query, conn, params=tuple(area_ids))
        else:
            query = """
            SELECT DISTINCT c.id as category_id, a.id as area_id, jc.quartile
            FROM categories c
            JOIN category_areas ca ON c.id = ca.category_id
            JOIN areas a ON ca.area_id = a.id
            LEFT JOIN journal_categories jc ON c.id = jc.category_id
            """
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    
    def getAreasAssignedToCategories(self, category_ids):
        conn = sqlite3.connect(self.dbPathOrUrl)
        
        if category_ids:
            placeholders = ', '.join(['?' for _ in category_ids])
            query = f"""
            SELECT DISTINCT a.id as area_id, c.id as category_id
            FROM areas a
            JOIN category_areas ca ON a.id = ca.area_id
            JOIN categories c ON ca.category_id = c.id
            WHERE c.id IN ({placeholders})
            """
            df = pd.read_sql_query(query, conn, params=tuple(category_ids))
        else:
            query = """
            SELECT DISTINCT a.id as area_id, c.id as category_id
            FROM areas a
            JOIN category_areas ca ON a.id = ca.area_id
            JOIN categories c ON ca.category_id = c.id
            """
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df

class BasicQueryEngine:
    """Coordinates query handlers to fetch and merge data from SPARQL and SQL sources."""
    def __init__(self):
        self.journalQuery = []
        self.categoryQuery = []
    
    def cleanJournalHandlers(self):
        self.journalQuery = []
        return True
    
    def cleanCategoryHandlers(self):
        self.categoryQuery = []
        return True
    
    def addJournalHandler(self, handler):
        if isinstance(handler, JournalQueryHandler):
            self.journalQuery.append(handler)
            return True
        return False
    
    def addCategoryHandler(self, handler):
        if isinstance(handler, CategoryQueryHandler):
            self.categoryQuery.append(handler)
            return True
        return False
    
    def getEntityById(self, id):
        # First, try to query as a journal ID
        for handler in self.journalQuery:
            df = handler.getById(id)
            if not df.empty:
                # Create and return Journal object
                journal_data = df.iloc[0]
                title = journal_data.get('title', '')
                publisher = journal_data.get('publisher', None)
                licence = journal_data.get('license', None)
                seal_str = str(journal_data.get('seal', 'false')).lower()
                apc_str = str(journal_data.get('apc', 'false')).lower()
                
                # Process languages
                languages = []
                if 'language' in journal_data and journal_data['language']:
                    languages = [journal_data['language']]
                
                # Set boolean values
                seal = seal_str == 'true'
                apc = apc_str == 'true'
                
                # Get categories and areas
                categories = self._getCategoriesForJournal(id)
                areas = self._getAreasForJournal(id)
                
                return Journal(id, title, languages, publisher, seal, licence, apc, categories, areas)
        
        # Try querying as a category ID
        for handler in self.categoryQuery:
            df = handler.getById(id)
            if not df.empty and 'category_id' in df.columns and df['category_id'].iloc[0] == id:
                # Create and return Category object
                quartile = None
                if 'quartile' in df.columns and not df['quartile'].isna().all():
                    quartile = df['quartile'].iloc[0]
                return Category(id, quartile)

        # Try querying as an area ID
        for handler in self.categoryQuery:
            df = handler.getById(id)
            if not df.empty and 'area_id' in df.columns and df['area_id'].iloc[0] == id:
                # Create and return Area object
                return Area(id)

        # If not found, return None
        return None
    
    def _getCategoriesForJournal(self, journal_id):
        categories = []
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            query = """
            SELECT DISTINCT c.id, jc.quartile
            FROM categories c
            JOIN journal_categories jc ON c.id = jc.category_id
            WHERE jc.journal_id = ?
            """
            df = pd.read_sql_query(query, conn, params=(journal_id,))
            conn.close()
            
            for _, row in df.iterrows():
                category = Category(row['id'], row.get('quartile', None))
                categories.append(category)
        
        return categories
    
    def _getAreasForJournal(self, journal_id):
        areas = []
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            query = """
            SELECT DISTINCT a.id
            FROM areas a
            JOIN category_areas ca ON a.id = ca.area_id
            JOIN categories c ON ca.category_id = c.id
            JOIN journal_categories jc ON c.id = jc.category_id
            WHERE jc.journal_id = ?
            """
            df = pd.read_sql_query(query, conn, params=(journal_id,))
            conn.close()
            
            for _, row in df.iterrows():
                area = Area(row['id'])
                areas.append(area)
        
        return areas
    
    def getAllJournals(self):
        journals = []
        for handler in self.journalQuery:
            df = handler.getAllJournals()
            for _, row in df.iterrows():
                journal_uri = row.get('journal')
                if not journal_uri or not isinstance(journal_uri, str):
                    continue  # Skip invalid entries
                journal_id = journal_uri.split('/')[-1]

                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                seal_str = str(row.get('seal', 'false')).lower()
                apc_str = str(row.get('apc', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]
                
                seal = seal_str == 'true'
                apc = apc_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, seal, licence, apc, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getJournalsWithTitle(self, partial_title):
        journals = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithTitle(partial_title)
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                seal_str = str(row.get('seal', 'false')).lower()
                apc_str = str(row.get('apc', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]

                # Set boolean values
                seal = seal_str == 'true'
                apc = apc_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, seal, licence, apc, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getJournalsPublishedBy(self, partial_name):
        journals = []
        for handler in self.journalQuery:
            df = handler.getJournalsPublishedBy(partial_name)
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                seal_str = str(row.get('seal', 'false')).lower()
                apc_str = str(row.get('apc', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]

                # Set boolean values
                seal = seal_str == 'true'
                apc = apc_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, seal, licence, apc, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getJournalsWithLicense(self, licenses):
        journals = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses)
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                seal_str = str(row.get('seal', 'false')).lower()
                apc_str = str(row.get('apc', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]

                # Set boolean values
                seal = seal_str == 'true'
                apc = apc_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, seal, licence, apc, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getJournalsWithAPC(self):
        journals = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithAPC()
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                seal_str = str(row.get('seal', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]

                # Set boolean values
                seal = seal_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, seal, licence, True, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getJournalsWithDOAJSeal(self):
        journals = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithDOAJSeal()
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                title = row.get('title', '')
                publisher = row.get('publisher', None)
                licence = row.get('license', None)
                apc_str = str(row.get('apc', 'false')).lower()

                # Process languages
                languages = []
                if 'language' in row and row['language']:
                    languages = [row['language']]

                # Set boolean values
                apc = apc_str == 'true'

                # Get categories and areas
                categories = self._getCategoriesForJournal(journal_id)
                areas = self._getAreasForJournal(journal_id)
                
                journal = Journal(journal_id, title, languages, publisher, True, licence, apc, categories, areas)
                journals.append(journal)
        
        return journals
    
    def getAllCategories(self):
        categories = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()
            for _, row in df.iterrows():
                category_id = row['category_id']
                quartile = row.get('quartile', None)
                category = Category(category_id, quartile)
                categories.append(category)
        
        return categories
    
    def getAllAreas(self):
        areas = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            for _, row in df.iterrows():
                area_id = row['area_id']
                area = Area(area_id)
                areas.append(area)
        
        return areas
    
    def getCategoriesWithQuartile(self, quartiles):
        categories = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesWithQuartile(quartiles)
            for _, row in df.iterrows():
                category_id = row['category_id']
                quartile = row.get('quartile', None)
                category = Category(category_id, quartile)
                categories.append(category)
        
        return categories
    
    def getCategoriesAssignedToAreas(self, area_ids):
        categories = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedToAreas(area_ids)
            for _, row in df.iterrows():
                category_id = row['category_id']
                quartile = row.get('quartile', None)
                category = Category(category_id, quartile)
                categories.append(category)
        
        return categories
    
    def getAreasAssignedToCategories(self, category_ids):
        areas = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids)
            for _, row in df.iterrows():
                area_id = row['area_id']
                area = Area(area_id)
                areas.append(area)
        
        return areas

class FullQueryEngine(BasicQueryEngine):
    """Extends BasicQueryEngine to provide integrated query methods across data sources."""
    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        # If not specified, use all categories and quartiles
        all_categories = set()
        all_quartiles = set()

        # Get all associated category IDs and quartiles for journals
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())

            # Get all category IDs
            query = "SELECT DISTINCT category_id FROM journal_categories"
            df = pd.read_sql_query(query, conn)
            all_categories.update(df['category_id'].tolist())

            # Get all quartiles
            query = "SELECT DISTINCT quartile FROM journal_categories WHERE quartile IS NOT NULL"
            df = pd.read_sql_query(query, conn)
            all_quartiles.update(df['quartile'].tolist())
            
            conn.close()

        # If not specified, use all categories and quartiles
        if not category_ids:
            category_ids = all_categories
        if not quartiles:
            quartiles = all_quartiles

        # Get journal IDs that match the criteria
        journal_ids = set()
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            
            placeholders_categories = ', '.join(['?' for _ in category_ids])
            placeholders_quartiles = ', '.join(['?' for _ in quartiles])
            
            query = f"""
            SELECT DISTINCT journal_id
            FROM journal_categories
            WHERE category_id IN ({placeholders_categories})
            AND quartile IN ({placeholders_quartiles})
            """
            
            params = list(category_ids) + list(quartiles)
            df = pd.read_sql_query(query, conn, params=params)
            journal_ids.update(df['journal_id'].tolist())
            
            conn.close()

        # Get journal objects
        journals = []
        for journal_id in journal_ids:
            journal = self.getEntityById(journal_id)
            if journal and isinstance(journal, Journal):
                journals.append(journal)
        
        return journals

    def getJournalsInAreasWithLicense(self, area_ids, licenses):      # If not specified, use all areas and licenses
        all_areas = set()
        all_licenses = set()

        # Get all areas
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            all_areas.update(df['area_id'].tolist())

        # Get all licenses
        for handler in self.journalQuery:
            query = """
            PREFIX dc: <http://purl.org/dc/terms/>
            
            SELECT DISTINCT ?license
            WHERE {
                ?journal dc:license ?license .
            }
            """
            
            headers = {'Accept': 'application/json'}
            payload = {'query': query}
            response = requests.get(handler.getDbPathOrUrl(), headers=headers, params=payload)
            
            if response.status_code == 200:
                results = response.json()
                if 'results' in results and 'bindings' in results['results']:
                    for binding in results['results']['bindings']:
                        if 'license' in binding and 'value' in binding['license']:
                            all_licenses.add(binding['license']['value'])

        # If not specified, use all areas and licenses
        if not area_ids:
            area_ids = all_areas
        if not licenses:
            licenses = all_licenses

        # Get journal IDs in specific areas
        journal_ids_in_areas = set()
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            
            placeholders = ', '.join(['?' for _ in area_ids])
            
            query = f"""
            SELECT DISTINCT jc.journal_id
            FROM journal_categories jc
            JOIN categories c ON jc.category_id = c.id
            JOIN category_areas ca ON c.id = ca.category_id
            JOIN areas a ON ca.area_id = a.id
            WHERE a.id IN ({placeholders})
            """
            
            df = pd.read_sql_query(query, conn, params=tuple(area_ids))
            journal_ids_in_areas.update(df['journal_id'].tolist())
            
            conn.close()

        # Get journals with specific licenses
        journals_with_license = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses)
            for _, row in df.iterrows():
                journal_id = row['journal'].split('/')[-1]
                if journal_id in journal_ids_in_areas:
                    title = row.get('title', '')
                    publisher = row.get('publisher', None)
                    licence = row.get('license', None)
                    seal_str = str(row.get('seal', 'false')).lower()
                    apc_str = str(row.get('apc', 'false')).lower()

                    # Process languages
                    languages = []
                    if 'language' in row and row['language']:
                        languages = [row['language']]

                    # Set boolean values
                    seal = seal_str == 'true'
                    apc = apc_str == 'true'

                    # Get categories and areas
                    categories = self._getCategoriesForJournal(journal_id)
                    areas = self._getAreasForJournal(journal_id)
                    
                    journal = Journal(journal_id, title, languages, publisher, seal, licence, apc, categories, areas)
                    journals_with_license.append(journal)
        
        return journals_with_license
    
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids, category_ids, quartiles):
        # If not specified, use all areas, categories, and quartiles
        all_areas = set()
        all_categories = set()
        all_quartiles = set()

        # Get all areas
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            all_areas.update(df['area_id'].tolist())

        # Get all categories and quartiles
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())

            # Get all categories
            query = "SELECT DISTINCT id FROM categories"
            df = pd.read_sql_query(query, conn)
            all_categories.update(df['id'].tolist())

            # Get all quartiles
            query = "SELECT DISTINCT quartile FROM journal_categories WHERE quartile IS NOT NULL"
            df = pd.read_sql_query(query, conn)
            all_quartiles.update(df['quartile'].tolist())
            
            conn.close()

        # If not specified, use all areas
        if not area_ids:
            area_ids = all_areas
        if not category_ids:
            category_ids = all_categories
        if not quartiles:
            quartiles = all_quartiles

        # Get journal IDs that match the criteria
        journal_ids = set()
        for handler in self.categoryQuery:
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            
            placeholders_areas = ', '.join(['?' for _ in area_ids])
            placeholders_categories = ', '.join(['?' for _ in category_ids])
            placeholders_quartiles = ', '.join(['?' for _ in quartiles])
            
            query = f"""
            SELECT DISTINCT jc.journal_id
            FROM journal_categories jc
            JOIN categories c ON jc.category_id = c.id
            JOIN category_areas ca ON c.id = ca.category_id
            JOIN areas a ON ca.area_id = a.id
            WHERE a.id IN ({placeholders_areas})
            AND c.id IN ({placeholders_categories})
            AND jc.quartile IN ({placeholders_quartiles})
            """
            
            params = list(area_ids) + list(category_ids) + list(quartiles)
            df = pd.read_sql_query(query, conn, params=params)
            journal_ids.update(df['journal_id'].tolist())
            
            conn.close()

        # Get journals without APC (diamond journals)
        diamond_journals = []
        for journal_id in journal_ids:
            journal = self.getEntityById(journal_id)
            if journal and isinstance(journal, Journal) and not journal.hasAPC():
                diamond_journals.append(journal)
        
        return diamond_journals