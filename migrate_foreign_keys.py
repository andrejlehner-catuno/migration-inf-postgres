#!/usr/bin/env python3
"""
FOREIGN KEYS MIGRATION: Informix → PostgreSQL
Liest Foreign Keys aus Informix und erstellt sie in PostgreSQL
"""

import jaydebeapi
import psycopg2
import os
import json
from datetime import datetime

# Konfiguration
INFORMIX_JDBC_URL = "jdbc:informix-sqli://localhost:9095/unostdtest:INFORMIXSERVER=ol_catuno_utf8en;CLIENT_LOCALE=en_US.utf8;DB_LOCALE=en_US.utf8;DBDATE=DMY4.;DBMONEY=.;DBDELIMITER=|"
INFORMIX_JDBC_DRIVER = "com.informix.jdbc.IfxDriver"
INFORMIX_JDBC_JAR = [
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\jdbc-4.50.11.jar",
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\bson-3.8.0.jar"
]
INFORMIX_USER = "informix"
INFORMIX_PASSWORD = "informix"

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'catuno_production',
    'user': 'catuno',
    'password': 'start12345'
}

LOG_DIR = r"C:\postgres\migration"
LOG_FILE = os.path.join(LOG_DIR, f"fk_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "fk_checkpoint.json")

os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'

def log(message, level="INFO"):
    """Log message to file and console"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] [{level}] {message}"
    print(log_line)
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_line + '\n')

def connect_informix():
    """Connect to Informix"""
    conn = jaydebeapi.connect(
        INFORMIX_JDBC_DRIVER,
        INFORMIX_JDBC_URL,
        [INFORMIX_USER, INFORMIX_PASSWORD],
        INFORMIX_JDBC_JAR
    )
    return conn

def connect_postgres():
    """Connect to PostgreSQL"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    return conn

def get_foreign_keys(ifx_conn):
    """
    Get all Foreign Keys from Informix
    
    Informix stores FKs in:
    - sysconstraints: constrtype='R' for Referential
    - sysreferences: FK details
    - sysindexes: columns
    """
    log("Fetching Foreign Keys from Informix...")
    
    cursor = ifx_conn.cursor()
    
    # Query to get FK information
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
            ref.delrule,
            ref.updrule
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
          AND child_tab.tabid > 99
          AND parent_tab.tabid > 99
        ORDER BY child_tab.tabname, child_cons.constrname
    """)
    
    foreign_keys = []
    
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        
        child_table = row[0]
        fk_name = row[1]
        parent_table = row[2]
        
        # Get child column numbers (part1 to part16)
        child_col_numbers = []
        for i in range(3, 19):
            if row[i] and row[i] != 0:
                child_col_numbers.append(abs(row[i]))
        
        # Get parent column numbers
        parent_col_numbers = []
        for i in range(19, 35):
            if row[i] and row[i] != 0:
                parent_col_numbers.append(abs(row[i]))
        
        # Delete rule: C=CASCADE, R=RESTRICT
        delete_rule = row[35]
        # Update rule: C=CASCADE, R=RESTRICT
        update_rule = row[36]
        
        if child_col_numbers and parent_col_numbers:
            foreign_keys.append({
                'child_table': child_table,
                'fk_name': fk_name,
                'parent_table': parent_table,
                'child_col_numbers': child_col_numbers,
                'parent_col_numbers': parent_col_numbers,
                'delete_rule': delete_rule,
                'update_rule': update_rule
            })
    
    cursor.close()
    
    log(f"Found {len(foreign_keys)} Foreign Keys")
    return foreign_keys

def get_column_names(ifx_conn, table_name, col_numbers):
    """Get column names for given column numbers"""
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
    
    # Create mapping: colno -> colname
    col_mapping = {}
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        col_mapping[row[0]] = row[1]
    
    cursor.close()
    
    # Return column names in order
    return [col_mapping.get(num, f"col_{num}") for num in col_numbers]

def create_foreign_key(pg_conn, fk_info, child_cols, parent_cols):
    """Create Foreign Key in PostgreSQL"""
    
    child_table = fk_info['child_table']
    parent_table = fk_info['parent_table']
    fk_name = fk_info['fk_name']
    delete_rule = fk_info['delete_rule']
    update_rule = fk_info['update_rule']
    
    # Escape column names if needed
    def escape_col(col):
        return f'"{col}"' if col.lower() in ['user', 'order', 'group'] else col
    
    child_cols_str = ', '.join([escape_col(c) for c in child_cols])
    parent_cols_str = ', '.join([escape_col(c) for c in parent_cols])
    
    # Map Informix rules to PostgreSQL
    on_delete = ""
    if delete_rule == 'C':
        on_delete = " ON DELETE CASCADE"
    elif delete_rule == 'R':
        on_delete = " ON DELETE RESTRICT"
    
    on_update = ""
    if update_rule == 'C':
        on_update = " ON UPDATE CASCADE"
    elif update_rule == 'R':
        on_update = " ON UPDATE RESTRICT"
    
    # Build FK name (PostgreSQL convention)
    pg_fk_name = f"{child_table}_{fk_name}_fkey".lower()
    if len(pg_fk_name) > 63:
        pg_fk_name = pg_fk_name[:63]
    
    alter_sql = f"""ALTER TABLE {child_table} 
                    ADD CONSTRAINT {pg_fk_name} 
                    FOREIGN KEY ({child_cols_str}) 
                    REFERENCES {parent_table} ({parent_cols_str})
                    {on_delete}{on_update}"""
    
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
    """Load checkpoint data"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {
        'completed': [],
        'failed': [],
        'start_time': datetime.now().isoformat()
    }

def save_checkpoint(checkpoint):
    """Save checkpoint data"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def main():
    """Main migration function"""
    
    log("=" * 80)
    log("FOREIGN KEYS MIGRATION: Informix → PostgreSQL")
    log("=" * 80)
    log(f"Log file: {LOG_FILE}")
    log(f"Checkpoint: {CHECKPOINT_FILE}")
    log("")
    
    start_time = datetime.now()
    
    # Connect
    log("Connecting to databases...")
    ifx_conn = connect_informix()
    log("✓ Informix connected")
    
    pg_conn = connect_postgres()
    log("✓ PostgreSQL connected")
    log("")
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    
    try:
        # Get Foreign Keys
        foreign_keys = get_foreign_keys(ifx_conn)
        
        # Filter out already completed
        completed_keys = set(checkpoint['completed'])
        pending_fks = []
        for fk in foreign_keys:
            key = f"{fk['child_table']}.{fk['fk_name']}"
            if key not in completed_keys:
                pending_fks.append(fk)
        
        log(f"Total FKs: {len(foreign_keys)}")
        log(f"Completed: {len(checkpoint['completed'])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log(f"Pending: {len(pending_fks)}")
        log("")
        
        # Process each FK
        success_count = 0
        failed_count = 0
        
        for i, fk_info in enumerate(pending_fks, 1):
            child_table = fk_info['child_table']
            fk_name = fk_info['fk_name']
            parent_table = fk_info['parent_table']
            key = f"{child_table}.{fk_name}"
            
            if i % 20 == 0:
                log(f"[{i}/{len(pending_fks)}] {child_table} → {parent_table}")
            
            # Get column names
            child_cols = get_column_names(ifx_conn, child_table, fk_info['child_col_numbers'])
            parent_cols = get_column_names(ifx_conn, parent_table, fk_info['parent_col_numbers'])
            
            # Create FK in PostgreSQL
            success, error = create_foreign_key(pg_conn, fk_info, child_cols, parent_cols)
            
            if success:
                checkpoint['completed'].append(key)
                success_count += 1
                
                if i % 20 == 0:
                    log(f"  ✓ FK created")
            else:
                log(f"  ✗ FAILED {key}: {error}", "ERROR")
                checkpoint['failed'].append({
                    'child_table': child_table,
                    'fk_name': fk_name,
                    'parent_table': parent_table,
                    'error': error
                })
                failed_count += 1
            
            # Save checkpoint every 50 FKs
            if i % 50 == 0:
                save_checkpoint(checkpoint)
        
        # Final save
        save_checkpoint(checkpoint)
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds() / 60
        
        # Summary
        log("")
        log("=" * 80)
        log("FOREIGN KEYS MIGRATION COMPLETED!")
        log("=" * 80)
        log(f"Duration: {duration:.1f} minutes")
        log(f"Total FKs: {len(foreign_keys)}")
        log(f"Successfully created: {len(checkpoint['completed'])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log("")
        
        if checkpoint['failed']:
            log("Failed Foreign Keys:", "WARN")
            for failed in checkpoint['failed'][:20]:  # Show first 20
                log(f"  - {failed['child_table']}.{failed['fk_name']} → {failed['parent_table']}", "WARN")
                log(f"    Error: {failed['error']}", "WARN")
            if len(checkpoint['failed']) > 20:
                log(f"  ... and {len(checkpoint['failed']) - 20} more", "WARN")
        else:
            log("✓✓✓ ALL FOREIGN KEYS CREATED SUCCESSFULLY! ✓✓✓", "SUCCESS")
        
        log("")
        log(f"Detailed log: {LOG_FILE}")
        log(f"Checkpoint: {CHECKPOINT_FILE}")
        log("=" * 80)
        
    finally:
        ifx_conn.close()
        pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        raise
