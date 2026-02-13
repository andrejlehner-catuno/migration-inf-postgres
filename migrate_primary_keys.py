#!/usr/bin/env python3
"""
PRIMARY KEYS MIGRATION: Informix → PostgreSQL
Liest PKs aus Informix und erstellt sie in PostgreSQL
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
LOG_FILE = os.path.join(LOG_DIR, f"pk_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "pk_checkpoint.json")

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

def get_primary_keys(ifx_conn):
    """
    Get all Primary Keys from Informix
    
    Informix stores PKs in:
    - sysconstraints: constrtype='P' for Primary Key
    - sysindexes: indexed columns
    - syscolumns: column names
    """
    log("Fetching Primary Keys from Informix...")
    
    cursor = ifx_conn.cursor()
    
    # Query to get PK information
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
        if row is None:
            break
        
        table_name = row[0]
        constraint_name = row[1]
        index_name = row[2]
        
        # Get column numbers (part1 to part16, 0 means not used)
        col_numbers = []
        for i in range(3, 19):  # part1 to part16
            if row[i] and row[i] != 0:
                # Informix stores column numbers, we need to get column names
                col_numbers.append(abs(row[i]))  # abs() because negative means DESC
        
        if col_numbers:
            primary_keys.append({
                'table_name': table_name,
                'constraint_name': constraint_name,
                'index_name': index_name,
                'column_numbers': col_numbers
            })
    
    cursor.close()
    
    log(f"Found {len(primary_keys)} Primary Keys")
    return primary_keys

def get_column_names(ifx_conn, table_name, col_numbers):
    """Get column names for given column numbers"""
    cursor = ifx_conn.cursor()
    
    # Build placeholders for IN clause
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
    
    # Return column names in order of col_numbers
    return [col_mapping.get(num, f"col_{num}") for num in col_numbers]

def create_primary_key(pg_conn, pk_info, column_names):
    """Create Primary Key in PostgreSQL"""
    
    table_name = pk_info['table_name']
    constraint_name = pk_info['constraint_name']
    
    # Escape column names if they're reserved keywords
    escaped_cols = [f'"{col}"' if col.lower() in ['user', 'order', 'group'] else col 
                    for col in column_names]
    
    # Build ALTER TABLE statement
    cols_str = ', '.join(escaped_cols)
    
    # Use table_name + _pkey as constraint name (PostgreSQL convention)
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
    log("PRIMARY KEYS MIGRATION: Informix → PostgreSQL")
    log("=" * 80)
    log(f"Log file: {LOG_FILE}")
    log(f"Checkpoint: {CHECKPOINT_FILE}")
    log("")
    
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
        # Get Primary Keys
        primary_keys = get_primary_keys(ifx_conn)
        
        # Filter out already completed
        pending_pks = [pk for pk in primary_keys 
                       if pk['table_name'] not in checkpoint['completed']]
        
        log(f"Total PKs: {len(primary_keys)}")
        log(f"Completed: {len(checkpoint['completed'])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log(f"Pending: {len(pending_pks)}")
        log("")
        
        # Process each PK
        success_count = 0
        failed_count = 0
        
        for i, pk_info in enumerate(pending_pks, 1):
            table_name = pk_info['table_name']
            
            log(f"[{i}/{len(pending_pks)}] Processing: {table_name}")
            
            # Get column names
            column_names = get_column_names(ifx_conn, table_name, pk_info['column_numbers'])
            
            log(f"  PK Columns: {', '.join(column_names)}")
            
            # Create PK in PostgreSQL
            success, error = create_primary_key(pg_conn, pk_info, column_names)
            
            if success:
                log(f"  ✓ PRIMARY KEY created")
                checkpoint['completed'].append(table_name)
                success_count += 1
            else:
                log(f"  ✗ FAILED: {error}", "ERROR")
                checkpoint['failed'].append({
                    'table': table_name,
                    'error': error
                })
                failed_count += 1
            
            # Save checkpoint every 10 PKs
            if i % 10 == 0:
                save_checkpoint(checkpoint)
        
        # Final save
        save_checkpoint(checkpoint)
        
        # Summary
        log("")
        log("=" * 80)
        log("PRIMARY KEYS MIGRATION COMPLETED!")
        log("=" * 80)
        log(f"Total PKs: {len(primary_keys)}")
        log(f"Successfully created: {success_count + len([t for t in checkpoint['completed'] if t not in [f['table'] for f in checkpoint['failed']]])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log("")
        
        if checkpoint['failed']:
            log("Failed tables:", "WARN")
            for failed in checkpoint['failed']:
                log(f"  - {failed['table']}: {failed['error']}", "WARN")
        else:
            log("✓✓✓ ALL PRIMARY KEYS CREATED SUCCESSFULLY! ✓✓✓", "SUCCESS")
        
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
