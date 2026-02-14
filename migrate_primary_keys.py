#!/usr/bin/env python3
"""
PRIMARY KEYS MIGRATION: Informix → PostgreSQL
Liest PKs aus Informix und erstellt sie in PostgreSQL
Sicherheits-Update: Nutzt zentrale db_config.py
"""

import os
import json
import sys
from datetime import datetime
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres

# Log-Konfiguration bleibt lokal, da sie spezifisch für dieses Skript ist
LOG_DIR = r"C:\postgres\migration"
LOG_FILE = os.path.join(LOG_DIR, f"pk_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "pk_checkpoint.json")

# Java Home wird für jaydebeapi benötigt
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

# Die alten Funktionen connect_informix() und connect_postgres() 
# wurden entfernt, da sie jetzt aus db_config importiert werden.

def get_primary_keys(ifx_conn):
    log("Fetching Primary Keys from Informix...")
    cursor = ifx_conn.cursor()
    cursor.execute("""
        SELECT 
            t.tabname,
            c.constrname,
            i.idxname,
            i.part1, i.part2, i.part3, i.part4, i.part5,
            i.part6, i.part7, i.part8, i.part9, i.part10,
            i.part11, i.part12, i.part13, i.part14, i.part15, i.part16
        FROM sysconstraints c
        JOIN systables t ON c.tabid = t.tabid
        JOIN sysindexes i ON c.idxname = i.idxname AND c.tabid = i.tabid
        WHERE c.constrtype = 'P'
          AND t.tabid > 99
          AND t.tabtype = 'T'
        ORDER BY t.tabname
    """)
    
    primary_keys = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        col_numbers = [abs(row[i]) for i in range(3, 19) if row[i] and row[i] != 0]
        
        if col_numbers:
            primary_keys.append({
                'table_name': row[0],
                'constraint_name': row[1],
                'index_name': row[2],
                'column_numbers': col_numbers
            })
    cursor.close()
    log(f"Found {len(primary_keys)} Primary Keys")
    return primary_keys

def get_column_names(ifx_conn, table_name, col_numbers):
    cursor = ifx_conn.cursor()
    placeholders = ','.join(['?'] * len(col_numbers))
    query = f"""
        SELECT colno, colname
        FROM syscolumns c
        JOIN systables t ON c.tabid = t.tabid
        WHERE t.tabname = ?
          AND c.colno IN ({placeholders})
        ORDER BY c.colno
    """
    params = [table_name] + col_numbers
    cursor.execute(query, params)
    col_mapping = {}
    while True:
        row = cursor.fetchone()
        if row is None: break
        col_mapping[row[0]] = row[1]
    cursor.close()
    return [col_mapping.get(num, f"col_{num}") for num in col_numbers]

def create_primary_key(pg_conn, pk_info, column_names):
    table_name = pk_info['table_name']
    escaped_cols = [f'"{col}"' if col.lower() in ['user', 'order', 'group', 'select'] else col for col in column_names]
    cols_str = ', '.join(escaped_cols)
    pg_constraint_name = f"{table_name}_pkey"
    alter_sql = f"ALTER TABLE {table_name} ADD CONSTRAINT {pg_constraint_name} PRIMARY KEY ({cols_str})"
    
    try:
        cursor = pg_conn.cursor()
        cursor.execute(alter_sql)
        pg_conn.commit()
        cursor.close()
        return True, None
    except Exception as e:
        pg_conn.rollback()
        return False, str(e)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f: return json.load(f)
    return {'completed': [], 'failed': [], 'start_time': datetime.now().isoformat()}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, 'w') as f: json.dump(checkpoint, f, indent=2)

def main():
    log("=" * 80)
    log("PRIMARY KEYS MIGRATION: Informix → PostgreSQL (Secure Mode)")
    log("=" * 80)
    
    try:
        # Nutzung der importierten Verbindungsfunktionen
        ifx_conn = connect_informix()
        log("✓ Informix connected")
        
        pg_conn = connect_postgres()
        log("✓ PostgreSQL connected")
        
        checkpoint = load_checkpoint()
        primary_keys = get_primary_keys(ifx_conn)
        pending_pks = [pk for pk in primary_keys if pk['table_name'] not in checkpoint['completed']]
        
        log(f"Pending: {len(pending_pks)}")
        
        for i, pk_info in enumerate(pending_pks, 1):
            table_name = pk_info['table_name']
            log(f"[{i}/{len(pending_pks)}] Processing: {table_name}")
            column_names = get_column_names(ifx_conn, table_name, pk_info['column_numbers'])
            success, error = create_primary_key(pg_conn, pk_info, column_names)
            
            if success:
                checkpoint['completed'].append(table_name)
            else:
                log(f"  ✗ FAILED: {error}", "ERROR")
                checkpoint['failed'].append({'table': table_name, 'error': error})
            
            if i % 10 == 0: save_checkpoint(checkpoint)
        
        save_checkpoint(checkpoint)
        log("=" * 80)
        log("PRIMARY KEYS MIGRATION COMPLETED!")
        log("=" * 80)
        
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    main()