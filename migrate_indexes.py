#!/usr/bin/env python3
"""
INDEXES MIGRATION: Informix → PostgreSQL
Sicherheits-Update: Nutzt zentrale db_config.py
"""

import os
import json
import re
import sys
from datetime import datetime
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres

# Lokale Pfade für Logs
LOG_DIR = r"C:\postgres\migration"
LOG_FILE = os.path.join(LOG_DIR, f"index_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "index_checkpoint.json")

os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'

def log(message, level="INFO"):
    """Log message to file and console"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] [{level}] {message}"
    print(log_line)
    
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_line + '\n')

def normalize_index_name(index_name, table_name):
    """Normalize index name for PostgreSQL"""
    if not index_name: return None
    index_name = index_name.strip()
    if index_name and index_name[0].isdigit():
        index_name = f"idx_{index_name}"
    index_name = re.sub(r'[^a-zA-Z0-9_]', '_', index_name)
    index_name = re.sub(r'_+', '_', index_name).strip('_')
    if not index_name: index_name = f"{table_name}_idx"
    return index_name.lower()[:63]

def get_indexes(ifx_conn):
    log("Fetching Indexes from Informix...")
    cursor = ifx_conn.cursor()
    cursor.execute("""
        SELECT 
            t.tabname, i.idxname, i.idxtype,
            i.part1, i.part2, i.part3, i.part4, i.part5,
            i.part6, i.part7, i.part8, i.part9, i.part10,
            i.part11, i.part12, i.part13, i.part14, i.part15, i.part16
        FROM sysindexes i
        JOIN systables t ON i.tabid = t.tabid
        WHERE t.tabid > 99 AND t.tabtype = 'T'
          AND i.idxname NOT IN (
              SELECT c.idxname FROM sysconstraints c 
              WHERE c.constrtype = 'P' AND c.tabid = i.tabid
          )
        ORDER BY t.tabname, i.idxname
    """)
    
    indexes = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        columns_info = [{'col_num': abs(row[i]), 'desc': row[i] < 0} 
                        for i in range(3, 19) if row[i] and row[i] != 0]
        
        if columns_info:
            indexes.append({
                'table_name': row[0],
                'index_name': row[1],
                'is_unique': row[2] == 'U',
                'columns_info': columns_info
            })
    cursor.close()
    return indexes

def get_column_names_with_order(ifx_conn, table_name, columns_info):
    cursor = ifx_conn.cursor()
    col_numbers = [col['col_num'] for col in columns_info]
    placeholders = ','.join(['?'] * len(col_numbers))
    
    query = f"SELECT colno, colname FROM syscolumns c JOIN systables t ON c.tabid = t.tabid WHERE t.tabname = ? AND c.colno IN ({placeholders})"
    cursor.execute(query, [table_name] + col_numbers)
    
    col_mapping = {}
    while True:
        row = cursor.fetchone()
        if row is None: break
        col_mapping[row[0]] = row[1]
    cursor.close()
    
    columns = []
    for col_info in columns_info:
        col_name = col_mapping.get(col_info['col_num'], f"col_{col_info['col_num']}")
        if col_name.lower() in ['user', 'order', 'group', 'table', 'select']:
            col_name = f'"{col_name}"'
        columns.append(f"{col_name} DESC" if col_info['desc'] else col_name)
    return columns

def create_index(pg_conn, index_info, columns):
    table_name = index_info['table_name']
    normalized_name = normalize_index_name(index_info['index_name'], table_name)
    cols_str = ', '.join(columns)
    unique_clause = "UNIQUE " if index_info['is_unique'] else ""
    create_sql = f"CREATE {unique_clause}INDEX {normalized_name} ON {table_name} ({cols_str})"
    
    try:
        cursor = pg_conn.cursor()
        cursor.execute(create_sql)
        pg_conn.commit()
        cursor.close()
        return True, None, normalized_name
    except Exception as e:
        pg_conn.rollback()
        return False, str(e), normalized_name

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f: return json.load(f)
    return {'completed': [], 'failed': [], 'start_time': datetime.now().isoformat()}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, 'w') as f: json.dump(checkpoint, f, indent=2)

def main():
    log("=" * 80)
    log("INDEXES MIGRATION: Informix → PostgreSQL (Secure Mode)")
    log("=" * 80)
    
    start_time = datetime.now()
    try:
        ifx_conn = connect_informix()
        pg_conn = connect_postgres()
        
        checkpoint = load_checkpoint()
        indexes = get_indexes(ifx_conn)
        completed_keys = set(checkpoint['completed'])
        pending_indexes = [idx for idx in indexes if f"{idx['table_name']}.{idx['index_name']}" not in completed_keys]
        
        log(f"Total: {len(indexes)} | Pending: {len(pending_indexes)}")
        
        for i, index_info in enumerate(pending_indexes, 1):
            key = f"{index_info['table_name']}.{index_info['index_name']}"
            columns = get_column_names_with_order(ifx_conn, index_info['table_name'], index_info['columns_info'])
            success, error, normalized_name = create_index(pg_conn, index_info, columns)
            
            if success:
                checkpoint['completed'].append(key)
                if i % 50 == 0: log(f"[{i}/{len(pending_indexes)}] Created: {normalized_name}")
            else:
                log(f"✗ FAILED {key}: {error}", "ERROR")
                checkpoint['failed'].append({'table': index_info['table_name'], 'index': index_info['index_name'], 'error': error})
            
            if i % 100 == 0: save_checkpoint(checkpoint)
        
        save_checkpoint(checkpoint)
        log(f"Duration: {(datetime.now() - start_time).total_seconds() / 60:.1f} minutes")
        log("=" * 80)
        
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    main()