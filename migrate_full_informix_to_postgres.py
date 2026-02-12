#!/usr/bin/env python3
"""
VOLLSTÄNDIGE MIGRATION: Informix → PostgreSQL
Alle Tabellen mit automatischer Schema-Konvertierung
Informix: READ-ONLY
PostgreSQL: WRITE
"""

import jaydebeapi
import psycopg2
from datetime import datetime
import sys
import os
import json
import traceback

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

BATCH_SIZE = 500
LOG_DIR = r"C:\postgres\migration"
CHECKPOINT_FILE = os.path.join(LOG_DIR, "checkpoint.json")
LOG_FILE = os.path.join(LOG_DIR, f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Datentyp-Mapping: Informix → PostgreSQL
TYPE_MAPPING = {
    0: 'CHAR',           # CHAR
    1: 'SMALLINT',       # SMALLINT
    2: 'INTEGER',        # INTEGER
    3: 'FLOAT',          # FLOAT
    4: 'SMALLFLOAT',     # SMALLFLOAT
    5: 'DECIMAL',        # DECIMAL
    6: 'SERIAL',         # SERIAL
    7: 'DATE',           # DATE
    8: 'MONEY',          # MONEY
    9: 'NULL',           # NULL
    10: 'DATETIME',      # DATETIME
    11: 'BYTE',          # BYTE
    12: 'TEXT',          # TEXT
    13: 'VARCHAR',       # VARCHAR
    14: 'INTERVAL',      # INTERVAL
    15: 'NCHAR',         # NCHAR
    16: 'NVARCHAR',      # NVARCHAR
    17: 'INT8',          # INT8
    18: 'SERIAL8',       # SERIAL8
    19: 'SET',           # SET
    20: 'MULTISET',      # MULTISET
    21: 'LIST',          # LIST
    22: 'ROW',           # ROW
    23: 'COLLECTION',    # COLLECTION
    40: 'LVARCHAR',      # LVARCHAR
    41: 'BLOB',          # BLOB
    43: 'BOOLEAN',       # BOOLEAN
    52: 'BIGINT',        # BIGINT
    53: 'BIGSERIAL',     # BIGSERIAL
}

POSTGRES_TYPE_MAPPING = {
    'CHAR': 'CHAR',
    'SMALLINT': 'SMALLINT',
    'INTEGER': 'INTEGER',
    'FLOAT': 'DOUBLE PRECISION',
    'SMALLFLOAT': 'REAL',
    'DECIMAL': 'NUMERIC',
    'SERIAL': 'SERIAL',
    'DATE': 'DATE',
    'MONEY': 'NUMERIC(12,2)',
    'DATETIME': 'TIMESTAMP',
    'BYTE': 'BYTEA',
    'TEXT': 'TEXT',
    'VARCHAR': 'VARCHAR',
    'LVARCHAR': 'TEXT',
    'INTERVAL': 'INTERVAL',
    'NCHAR': 'CHAR',
    'NVARCHAR': 'VARCHAR',
    'INT8': 'BIGINT',
    'SERIAL8': 'BIGSERIAL',
    'BLOB': 'BYTEA',
    'BOOLEAN': 'BOOLEAN',
    'BIGINT': 'BIGINT',
    'BIGSERIAL': 'BIGSERIAL',
}

# PostgreSQL reserved keywords that need escaping
POSTGRES_RESERVED_KEYWORDS = {
    'user', 'order', 'select', 'from', 'where', 'insert', 'update', 'delete',
    'group', 'having', 'create', 'drop', 'alter', 'table', 'index', 'view',
    'union', 'all', 'and', 'or', 'not', 'null', 'default', 'primary', 'foreign',
    'key', 'check', 'unique', 'references', 'on', 'to', 'as', 'is', 'in',
    'exists', 'like', 'between', 'distinct', 'case', 'when', 'then', 'else',
    'end', 'cast', 'extract', 'interval', 'timestamp', 'date', 'time'
}

def escape_identifier(name):
    """Escape PostgreSQL identifiers if they are reserved keywords"""
    if name.lower() in POSTGRES_RESERVED_KEYWORDS:
        return f'"{name}"'
    return name

class MigrationLogger:
    """Logger for migration process"""
    
    def __init__(self, log_file):
        self.log_file = log_file
        self.start_time = datetime.now()
        
    def log(self, message, level="INFO"):
        """Write log message"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] [{level}] {message}"
        print(log_line)
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_line + '\n')
    
    def error(self, message):
        self.log(message, "ERROR")
    
    def warning(self, message):
        self.log(message, "WARN")
    
    def success(self, message):
        self.log(message, "SUCCESS")

class Checkpoint:
    """Checkpoint system for resumable migration"""
    
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.data = self.load()
    
    def load(self):
        """Load checkpoint data"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {
            'completed_tables': [],
            'failed_tables': [],
            'last_table': None,
            'start_time': datetime.now().isoformat(),
            'stats': {}
        }
    
    def save(self):
        """Save checkpoint data"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_completed(self, table_name, row_count, duration):
        """Mark table as completed"""
        self.data['completed_tables'].append(table_name)
        self.data['last_table'] = table_name
        self.data['stats'][table_name] = {
            'rows': row_count,
            'duration': duration,
            'status': 'completed'
        }
        self.save()
    
    def mark_failed(self, table_name, error):
        """Mark table as failed"""
        self.data['failed_tables'].append(table_name)
        self.data['stats'][table_name] = {
            'status': 'failed',
            'error': str(error)
        }
        self.save()
    
    def is_completed(self, table_name):
        """Check if table already completed"""
        return table_name in self.data['completed_tables']

def connect_informix():
    """Connect to Informix via JDBC (READ-ONLY)"""
    try:
        conn = jaydebeapi.connect(
            INFORMIX_JDBC_DRIVER,
            INFORMIX_JDBC_URL,
            [INFORMIX_USER, INFORMIX_PASSWORD],
            INFORMIX_JDBC_JAR
        )
        return conn
    except Exception as e:
        raise Exception(f"Informix connection failed: {e}")

def connect_postgres():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        raise Exception(f"PostgreSQL connection failed: {e}")

def get_all_tables(ifx_conn, logger):
    """Get all user tables from Informix"""
    logger.log("Fetching table list from Informix...")
    
    cursor = ifx_conn.cursor()
    cursor.execute("""
        SELECT tabname, nrows
        FROM systables
        WHERE tabid > 99
          AND tabtype = 'T'
        ORDER BY nrows ASC
    """)
    
    tables = []
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        tables.append({'name': row[0], 'rows': row[1] if row[1] else 0})
    
    cursor.close()
    logger.log(f"Found {len(tables)} tables")
    return tables

def get_table_schema(ifx_conn, table_name, logger):
    """Get schema for a specific table"""
    cursor = ifx_conn.cursor()
    
    cursor.execute(f"""
        SELECT 
            c.colname,
            c.coltype,
            MOD(c.coltype, 256) as base_type,
            c.collength,
            CASE WHEN c.coltype >= 256 THEN 1 ELSE 0 END as not_null
        FROM syscolumns c
        JOIN systables t ON c.tabid = t.tabid
        WHERE t.tabname = '{table_name}'
        ORDER BY c.colno
    """)
    
    columns = []
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        
        col_name = row[0]
        base_type_code = row[2]
        col_length = row[3]
        not_null = row[4] == 1
        
        # Map Informix type to PostgreSQL
        ifx_type = TYPE_MAPPING.get(base_type_code, 'VARCHAR')
        pg_type = POSTGRES_TYPE_MAPPING.get(ifx_type, 'TEXT')
        
        # Add length for CHAR/VARCHAR
        if ifx_type in ['CHAR', 'NCHAR'] and col_length:
            pg_type = f"CHAR({col_length})"
        elif ifx_type in ['VARCHAR', 'NVARCHAR'] and col_length:
            pg_type = f"VARCHAR({col_length})"
        elif ifx_type == 'DECIMAL' and col_length:
            # CRITICAL FIX: Informix encodes DECIMAL as (precision << 8) | scale
            precision = (col_length >> 8) & 0xFF
            scale = col_length & 0xFF
            if precision > 0 and precision <= 1000:
                pg_type = f"NUMERIC({precision},{scale})"
            else:
                # Fallback for invalid values
                pg_type = "NUMERIC(12,2)"
        
        columns.append({
            'name': col_name,
            'type': pg_type,
            'not_null': not_null
        })
    
    cursor.close()
    return columns

def create_table_postgres(pg_conn, table_name, columns, logger):
    """Create table in PostgreSQL"""
    
    # Escape table name if needed
    escaped_table_name = escape_identifier(table_name)
    
    # Build CREATE TABLE statement
    col_defs = []
    for col in columns:
        escaped_col_name = escape_identifier(col['name'])
        col_def = f"{escaped_col_name} {col['type']}"
        if col['not_null']:
            col_def += " NOT NULL"
        col_defs.append(col_def)
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {escaped_table_name} (\n  "
    create_sql += ",\n  ".join(col_defs)
    create_sql += "\n)"
    
    try:
        cursor = pg_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {escaped_table_name} CASCADE")
        cursor.execute(create_sql)
        pg_conn.commit()
        cursor.close()
        logger.log(f"  ✓ Table created: {table_name}")
        return True
    except Exception as e:
        logger.error(f"  ✗ Failed to create table {table_name}: {e}")
        pg_conn.rollback()
        return False

def migrate_table_data(ifx_conn, pg_conn, table_name, columns, total_rows, logger):
    """Migrate data for a single table"""
    
    # Escape identifiers
    escaped_table_name = escape_identifier(table_name)
    escaped_col_names = [escape_identifier(col['name']) for col in columns]
    
    # Build INSERT statement
    placeholders = ', '.join(['%s'] * len(escaped_col_names))
    insert_sql = f"INSERT INTO {escaped_table_name} ({', '.join(escaped_col_names)}) VALUES ({placeholders})"
    
    # Read from Informix
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute(f"SELECT * FROM {table_name}")
    
    # Insert into PostgreSQL in batches
    pg_cursor = pg_conn.cursor()
    
    rows_migrated = 0
    batch = []
    
    while True:
        row = ifx_cursor.fetchone()
        if row is None:
            break
        
        batch.append(tuple(row))
        
        if len(batch) >= BATCH_SIZE:
            pg_cursor.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_migrated += len(batch)
            
            if total_rows > 0:
                pct = 100 * rows_migrated // total_rows
                print(f"\r  Progress: {rows_migrated}/{total_rows} rows ({pct}%)", end='')
            
            batch = []
    
    # Insert remaining rows
    if batch:
        pg_cursor.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_migrated += len(batch)
    
    if total_rows > 0:
        print(f"\r  Progress: {rows_migrated}/{total_rows} rows (100%)")
    
    ifx_cursor.close()
    pg_cursor.close()
    
    return rows_migrated

def migrate_single_table(ifx_conn, pg_conn, table_info, logger, checkpoint):
    """Migrate a single table"""
    
    table_name = table_info['name']
    total_rows = table_info['rows']
    
    logger.log("="*80)
    logger.log(f"Migrating table: {table_name} ({total_rows} rows)")
    
    start_time = datetime.now()
    
    try:
        # Get schema
        columns = get_table_schema(ifx_conn, table_name, logger)
        if not columns:
            raise Exception("No columns found")
        
        logger.log(f"  Schema: {len(columns)} columns")
        
        # Create table
        if not create_table_postgres(pg_conn, table_name, columns, logger):
            raise Exception("Table creation failed")
        
        # Migrate data
        if total_rows > 0:
            rows_migrated = migrate_table_data(ifx_conn, pg_conn, table_name, columns, total_rows, logger)
            
            duration = (datetime.now() - start_time).total_seconds()
            speed = rows_migrated / duration if duration > 0 else 0
            
            logger.success(f"  ✓ Migrated {rows_migrated} rows in {duration:.2f}s ({speed:.0f} rows/sec)")
            
            # Verify
            pg_cursor = pg_conn.cursor()
            escaped_table_name = escape_identifier(table_name)
            pg_cursor.execute(f"SELECT COUNT(*) FROM {escaped_table_name}")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            
            if pg_count != rows_migrated:
                raise Exception(f"Row count mismatch: expected {rows_migrated}, got {pg_count}")
            
            checkpoint.mark_completed(table_name, rows_migrated, duration)
        else:
            logger.log(f"  ✓ Empty table created")
            checkpoint.mark_completed(table_name, 0, 0)
        
        return True
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"  ✗ Migration failed after {duration:.2f}s: {e}")
        checkpoint.mark_failed(table_name, str(e))
        return False

def main():
    """Main migration function"""
    
    # Initialize
    logger = MigrationLogger(LOG_FILE)
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    
    logger.log("="*80)
    logger.log("VOLLSTÄNDIGE MIGRATION: Informix → PostgreSQL")
    logger.log("="*80)
    logger.log(f"Log file: {LOG_FILE}")
    logger.log(f"Checkpoint file: {CHECKPOINT_FILE}")
    logger.log("")
    
    # Set JAVA_HOME
    os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'
    
    # Connect
    logger.log("Connecting to databases...")
    try:
        ifx_conn = connect_informix()
        logger.success("✓ Informix connected (READ-ONLY)")
    except Exception as e:
        logger.error(f"✗ Informix connection failed: {e}")
        sys.exit(1)
    
    try:
        pg_conn = connect_postgres()
        logger.success("✓ PostgreSQL connected")
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {e}")
        sys.exit(1)
    
    logger.log("")
    
    # Get tables
    tables = get_all_tables(ifx_conn, logger)
    
    # Filter out already completed tables
    pending_tables = [t for t in tables if not checkpoint.is_completed(t['name'])]
    
    logger.log(f"Total tables: {len(tables)}")
    logger.log(f"Completed: {len(checkpoint.data['completed_tables'])}")
    logger.log(f"Failed: {len(checkpoint.data['failed_tables'])}")
    logger.log(f"Pending: {len(pending_tables)}")
    logger.log("")
    
    # Migrate tables
    migration_start = datetime.now()
    success_count = 0
    failed_count = 0
    
    for i, table_info in enumerate(pending_tables, 1):
        logger.log(f"[{i}/{len(pending_tables)}] Processing: {table_info['name']}")
        
        if migrate_single_table(ifx_conn, pg_conn, table_info, logger, checkpoint):
            success_count += 1
        else:
            failed_count += 1
        
        logger.log("")
    
    # Final report
    total_duration = (datetime.now() - migration_start).total_seconds()
    
    logger.log("="*80)
    logger.log("MIGRATION COMPLETED!")
    logger.log("="*80)
    logger.log(f"Duration: {total_duration/60:.1f} minutes")
    logger.log(f"Total tables: {len(tables)}")
    logger.log(f"Successfully migrated: {success_count + len(checkpoint.data['completed_tables'])}")
    logger.log(f"Failed: {failed_count + len(checkpoint.data['failed_tables'])}")
    logger.log("")
    
    if checkpoint.data['failed_tables']:
        logger.warning("Failed tables:")
        for table in checkpoint.data['failed_tables']:
            logger.warning(f"  - {table}")
    else:
        logger.success("✓✓✓ ALL TABLES MIGRATED SUCCESSFULLY! ✓✓✓")
    
    logger.log("")
    logger.log(f"Detailed log: {LOG_FILE}")
    logger.log(f"Checkpoint: {CHECKPOINT_FILE}")
    logger.log("="*80)
    
    # Close connections
    ifx_conn.close()
    pg_conn.close()
    logger.log("Connections closed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user!")
        print("Progress saved in checkpoint file.")
        print("Run script again to resume from last completed table.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
