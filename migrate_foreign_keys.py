#!/usr/bin/env python3
"""
FOREIGN KEYS MIGRATION: Informix → PostgreSQL
Sicherheits-Update: Nutzt zentrale db_config.py
"""

import os
import json
import sys
from datetime import datetime
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres

# Lokale Pfade für Logs
LOG_DIR = r"C:\postgres\migration"
LOG_FILE = os.path.join(LOG_DIR, f"fk_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "fk_checkpoint.json")

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

def get_foreign_keys(ifx_conn):
    log("Fetching Foreign Keys from Informix...")
    cursor = ifx_conn.cursor()
    cursor.execute("""
        SELECT 
            child_tab.tabname AS child_table,
            child_cons.constrname AS fk_name,
            parent_tab.tabname AS parent_table,
            child_idx.part1, child_idx.part2, child_idx.part3, 
            child_idx.part4, child_idx.part5, child_idx.part6,
            child_idx.part7, child_idx.part8, child_idx.part9,
            child_idx.part10, child_idx.part11, child_idx.part12,
            child_idx.part13, child_idx.part14, child_idx.part15, child_idx.part16,
            parent_idx.part1, parent_idx.part2, parent_idx.part3,
            parent_idx.part4, parent_idx.part5, parent_idx.part6,
            parent_idx.part7, parent_idx.part8, parent_idx.part9,
            parent_idx.part10, parent_idx.part11, parent_idx.part12,
            parent_idx.part13, parent_idx.part14, parent_idx.part15, parent_idx.part16,
            ref.delrule, ref.updrule
        FROM sysconstraints child_cons
        JOIN systables child_tab ON child_cons.tabid = child_tab.tabid
        JOIN sysreferences ref ON child_cons.constrid = ref.constrid
        JOIN sysconstraints parent_cons ON ref.primary = parent_cons.constrid
        JOIN systables parent_tab ON parent_cons.tabid = parent_tab.tabid
        JOIN sysindexes child_idx ON child_cons.idxname = child_idx.idxname 
            AND child_cons.tabid = child_idx.tabid
        JOIN sysindexes parent_idx ON parent_cons.idxname = parent_idx.idxname 
            AND parent_cons.tabid = parent_idx.tabid
        WHERE child_cons.constrtype = 'R'
          AND child_tab.tabid > 99 AND parent_tab.tabid > 99
        ORDER BY child_tab.tabname, child_cons.constrname
    """)
    
    foreign_keys = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        child_cols = [abs(row[i]) for i in range(3, 19) if row[i] and row[i] != 0]
        parent_cols = [abs(row[i]) for i in range(19, 35) if row[i] and row[i] != 0]
        
        if child_cols and parent_cols:
            foreign_keys.append({
                'child_table': row[0], 'fk_name': row[1], 'parent_table': row[2],
                'child_col_numbers': child_cols, 'parent_col_numbers': parent_cols,
                'delete_rule': row[35], 'update_rule': row[36]
            })
    cursor.close()
    return foreign_keys

def get_column_names(ifx_conn, table_name, col_numbers):
    cursor = ifx_conn.cursor()
    placeholders = ','.join(['?'] * len(col_numbers))
    query = f"SELECT colno, colname FROM syscolumns c JOIN systables t ON c.tabid = t.tabid WHERE t.tabname = ? AND c.colno IN ({placeholders}) ORDER BY c.colno"
    cursor.execute(query, [table_name] + col_numbers)
    col_mapping = {row[0]: row[1] for row in iter(cursor.fetchone, None)}
    cursor.close()
    return [col_mapping.get(num, f"col_{num}") for num in col_numbers]

def create_foreign_key(pg_conn, fk_info, child_cols, parent_cols):
    def escape_col(col):
        return f'"{col}"' if col.lower() in ['user', 'order', 'group', 'select', 'table'] else col
    
    child_cols_str = ', '.join([escape_col(c) for c in child_cols])
    parent_cols_str = ', '.join([escape_col(c) for c in parent_cols])
    
    rules = {"C": "CASCADE", "R": "RESTRICT"}
    on_delete = f" ON DELETE {rules[fk_info['delete_rule']]}" if fk_info['delete_rule'] in rules else ""
    on_update = f" ON UPDATE {rules[fk_info['update_rule']]}" if fk_info['update_rule'] in rules else ""
    
    pg_fk_name = f"{fk_info['child_table']}_{fk_info['fk_name']}_fkey".lower()[:63]
    
    alter_sql = f"ALTER TABLE {fk_info['child_table']} ADD CONSTRAINT {pg_fk_name} FOREIGN KEY ({child_cols_str}) REFERENCES {fk_info['parent_table']} ({parent_cols_str}){on_delete}{on_update}"
    
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
    log("FOREIGN KEYS MIGRATION: Informix → PostgreSQL (Secure Mode)")
    log("=" * 80)
    
    start_time = datetime.now()
    try:
        ifx_conn = connect_informix()
        pg_conn = connect_postgres()
        
        checkpoint = load_checkpoint()
        fks = get_foreign_keys(ifx_conn)
        completed_keys = set(checkpoint['completed'])
        pending_fks = [fk for fk in fks if f"{fk['child_table']}.{fk['fk_name']}" not in completed_keys]
        
        log(f"Total: {len(fks)} | Pending: {len(pending_fks)}")
        
        for i, fk_info in enumerate(pending_fks, 1):
            key = f"{fk_info['child_table']}.{fk_info['fk_name']}"
            child_cols = get_column_names(ifx_conn, fk_info['child_table'], fk_info['child_col_numbers'])
            parent_cols = get_column_names(ifx_conn, fk_info['parent_table'], fk_info['parent_col_numbers'])
            
            success, error = create_foreign_key(pg_conn, fk_info, child_cols, parent_cols)
            if success:
                checkpoint['completed'].append(key)
                if i % 20 == 0: log(f"[{i}/{len(pending_fks)}] Created FK for {fk_info['child_table']}")
            else:
                log(f"✗ FAILED {key}: {error}", "ERROR")
                checkpoint['failed'].append({'table': fk_info['child_table'], 'fk': fk_info['fk_name'], 'error': error})
            
            if i % 50 == 0: save_checkpoint(checkpoint)
            
        save_checkpoint(checkpoint)
        log(f"Duration: {(datetime.now() - start_time).total_seconds() / 60:.1f} minutes")
        
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    main()