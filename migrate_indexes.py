#!/usr/bin/env python3
"""
INDEXES MIGRATION: Informix → PostgreSQL
Liest Indizes aus Informix und erstellt sie in PostgreSQL
Mit Fix für numerische Index-Namen
"""

import jaydebeapi
import psycopg2
import os
import json
import re
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
LOG_FILE = os.path.join(LOG_DIR, f"index_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
CHECKPOINT_FILE = os.path.join(LOG_DIR, "index_checkpoint.json")

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

def normalize_index_name(index_name, table_name):
    """
    Normalize index name for PostgreSQL
    - Must not start with digit
    - Replace invalid characters
    - Ensure uniqueness
    """
    if not index_name:
        return None
    
    # Remove whitespace
    index_name = index_name.strip()
    
    # If starts with digit, prefix with idx_
    if index_name and index_name[0].isdigit():
        index_name = f"idx_{index_name}"
    
    # Replace invalid characters
    index_name = re.sub(r'[^a-zA-Z0-9_]', '_', index_name)
    
    # Remove consecutive underscores
    index_name = re.sub(r'_+', '_', index_name)
    
    # Remove trailing/leading underscores
    index_name = index_name.strip('_')
    
    # Ensure not empty
    if not index_name:
        index_name = f"{table_name}_idx"
    
    # PostgreSQL limit: 63 characters
    if len(index_name) > 63:
        index_name = index_name[:63]
    
    return index_name.lower()

def get_indexes(ifx_conn):
    """
    Get all Indexes from Informix
    
    Informix stores indexes in sysindexes:
    - idxtype: U=Unique, D=Duplicate allowed
    - part1-part16: column numbers (0=not used, negative=DESC)
    """
    log("Fetching Indexes from Informix...")
    
    cursor = ifx_conn.cursor()
    
    # Query to get index information
    # Exclude indexes created by PRIMARY KEY constraints (they're already there)
    cursor.execute("""
        SELECT 
            t.tabname,
            i.idxname,
            i.idxtype,
            i.part1, i.part2, i.part3, i.part4, i.part5,
            i.part6, i.part7, i.part8, i.part9, i.part10,
            i.part11, i.part12, i.part13, i.part14, i.part15, i.part16
        FROM sysindexes i
        JOIN systables t ON i.tabid = t.tabid
        WHERE t.tabid > 99
          AND t.tabtype = 'T'
          AND i.idxname NOT IN (
              SELECT c.idxname 
              FROM sysconstraints c 
              WHERE c.constrtype = 'P' 
                AND c.tabid = i.tabid
          )
        ORDER BY t.tabname, i.idxname
    """)
    
    indexes = []
    
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        
        table_name = row[0]
        index_name = row[1]
        index_type = row[2]  # U=Unique, D=Duplicate
        
        # Get column numbers and their sort order
        columns_info = []
        for i in range(3, 19):  # part1 to part16
            if row[i] and row[i] != 0:
                col_num = abs(row[i])
                is_desc = row[i] < 0
                columns_info.append({
                    'col_num': col_num,
                    'desc': is_desc
                })
        
        if columns_info:
            indexes.append({
                'table_name': table_name,
                'index_name': index_name,
                'is_unique': index_type == 'U',
                'columns_info': columns_info
            })
    
    cursor.close()
    
    log(f"Found {len(indexes)} Indexes (excluding PK indexes)")
    return indexes

def get_column_names_with_order(ifx_conn, table_name, columns_info):
    """Get column names for given column numbers with sort order"""
    cursor = ifx_conn.cursor()
    
    # Get all column numbers
    col_numbers = [col['col_num'] for col in columns_info]
    
    # Build placeholders
    placeholders = ','.join(['?'] * len(col_numbers))
    
    query = f"""
        SELECT colno, colname
        FROM syscolumns c
        JOIN systables t ON c.tabid = t.tabid
        WHERE t.tabname = ?
          AND c.colno IN ({placeholders})
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
    
    # Build column definitions with order
    columns = []
    for col_info in columns_info:
        col_name = col_mapping.get(col_info['col_num'], f"col_{col_info['col_num']}")
        
        # Escape reserved keywords
        if col_name.lower() in ['user', 'order', 'group', 'table']:
            col_name = f'"{col_name}"'
        
        if col_info['desc']:
            columns.append(f"{col_name} DESC")
        else:
            columns.append(col_name)
    
    return columns

def create_index(pg_conn, index_info, columns):
    """Create Index in PostgreSQL"""
    
    table_name = index_info['table_name']
    original_index_name = index_info['index_name']
    is_unique = index_info['is_unique']
    
    # Normalize index name
    normalized_name = normalize_index_name(original_index_name, table_name)
    
    # Build CREATE INDEX statement
    cols_str = ', '.join(columns)
    
    unique_clause = "UNIQUE " if is_unique else ""
    
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
    log("INDEXES MIGRATION: Informix → PostgreSQL")
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
        # Get Indexes
        indexes = get_indexes(ifx_conn)
        
        # Create unique key for each index
        completed_keys = set(checkpoint['completed'])
        
        # Filter out already completed
        pending_indexes = []
        for idx in indexes:
            key = f"{idx['table_name']}.{idx['index_name']}"
            if key not in completed_keys:
                pending_indexes.append(idx)
        
        log(f"Total Indexes: {len(indexes)}")
        log(f"Completed: {len(checkpoint['completed'])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log(f"Pending: {len(pending_indexes)}")
        log("")
        
        # Process each index
        success_count = 0
        failed_count = 0
        
        for i, index_info in enumerate(pending_indexes, 1):
            table_name = index_info['table_name']
            index_name = index_info['index_name']
            key = f"{table_name}.{index_name}"
            
            if i % 50 == 0:
                log(f"[{i}/{len(pending_indexes)}] Processing: {table_name}.{index_name}")
            
            # Get column names with sort order
            columns = get_column_names_with_order(ifx_conn, table_name, index_info['columns_info'])
            
            # Create index in PostgreSQL
            success, error, normalized_name = create_index(pg_conn, index_info, columns)
            
            if success:
                checkpoint['completed'].append(key)
                success_count += 1
                
                if i % 50 == 0:
                    log(f"  ✓ INDEX created: {normalized_name}")
            else:
                log(f"  ✗ FAILED {key}: {error}", "ERROR")
                checkpoint['failed'].append({
                    'table': table_name,
                    'index': index_name,
                    'normalized': normalized_name,
                    'error': error
                })
                failed_count += 1
            
            # Save checkpoint every 100 indexes
            if i % 100 == 0:
                save_checkpoint(checkpoint)
        
        # Final save
        save_checkpoint(checkpoint)
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds() / 60
        
        # Summary
        log("")
        log("=" * 80)
        log("INDEXES MIGRATION COMPLETED!")
        log("=" * 80)
        log(f"Duration: {duration:.1f} minutes")
        log(f"Total Indexes: {len(indexes)}")
        log(f"Successfully created: {len(checkpoint['completed'])}")
        log(f"Failed: {len(checkpoint['failed'])}")
        log("")
        
        if checkpoint['failed']:
            log("Failed indexes:", "WARN")
            for failed in checkpoint['failed']:
                log(f"  - {failed['table']}.{failed['index']}: {failed['error']}", "WARN")
        else:
            log("✓✓✓ ALL INDEXES CREATED SUCCESSFULLY! ✓✓✓", "SUCCESS")
        
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
