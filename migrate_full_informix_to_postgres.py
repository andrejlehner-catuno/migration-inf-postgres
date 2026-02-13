#!/usr/bin/env python3
"""
VOLLSTÄNDIGE MIGRATION: Informix → PostgreSQL
Alle Tabellen mit automatischer Schema-Konvertierung
Sicherheits-Update: Passwörter werden über Umgebungsvariablen geladen.
"""

import jaydebeapi
import psycopg2
from datetime import datetime
import sys
import os
import json
import traceback

# --- SICHERHEITS-CHECK: Credentials laden ---
INFORMIX_PASSWORD = os.getenv('IFX_PW')
POSTGRES_PASSWORD = os.getenv('PG_PW')

if not INFORMIX_PASSWORD or not POSTGRES_PASSWORD:
    print("=" * 80)
    print("⚠️  FEHLER: DATENBANK-PASSWÖRTER NICHT GEFUNDEN!")
    print("=" * 80)
    if not INFORMIX_PASSWORD:
        print(" -> Variable 'IFX_PW' fehlt.")
    if not POSTGRES_PASSWORD:
        print(" -> Variable 'PG_PW' fehlt.")
    print("-" * 80)
    print("IM JENKINS: Stellen Sie sicher, dass das Jenkinsfile 'credentials()' nutzt.")
    print("LOKAL: Nutzen Sie 'set IFX_PW=...' und 'set PG_PW=...' in der CMD.")
    print("=" * 80)
    sys.exit(1)

# Konfiguration
INFORMIX_JDBC_URL = "jdbc:informix-sqli://localhost:9095/unostdtest:INFORMIXSERVER=ol_catuno_utf8en;CLIENT_LOCALE=en_US.utf8;DB_LOCALE=en_US.utf8;DBDATE=DMY4.;DBMONEY=.;DBDELIMITER=|"
INFORMIX_JDBC_DRIVER = "com.informix.jdbc.IfxDriver"
INFORMIX_JDBC_JAR = [
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\jdbc-4.50.11.jar",
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\bson-3.8.0.jar"
]
INFORMIX_USER = "informix"

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'catuno_production',
    'user': 'catuno',
    'password': POSTGRES_PASSWORD  # Nutzt die sichere Variable
}

BATCH_SIZE = 500
LOG_DIR = r"C:\postgres\migration"
CHECKPOINT_FILE = os.path.join(LOG_DIR, "checkpoint.json")
LOG_FILE = os.path.join(LOG_DIR, f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Datentyp-Mapping: Informix → PostgreSQL
TYPE_MAPPING = {
    0: 'CHAR', 1: 'SMALLINT', 2: 'INTEGER', 3: 'FLOAT', 4: 'SMALLFLOAT', 
    5: 'DECIMAL', 6: 'SERIAL', 7: 'DATE', 8: 'MONEY', 9: 'NULL', 
    10: 'DATETIME', 11: 'BYTE', 12: 'TEXT', 13: 'VARCHAR', 14: 'INTERVAL', 
    15: 'NCHAR', 16: 'NVARCHAR', 17: 'INT8', 18: 'SERIAL8', 19: 'SET', 
    20: 'MULTISET', 21: 'LIST', 22: 'ROW', 23: 'COLLECTION', 40: 'LVARCHAR', 
    41: 'BLOB', 43: 'BOOLEAN', 52: 'BIGINT', 53: 'BIGSERIAL',
}

POSTGRES_TYPE_MAPPING = {
    'CHAR': 'CHAR', 'SMALLINT': 'SMALLINT', 'INTEGER': 'INTEGER', 
    'FLOAT': 'DOUBLE PRECISION', 'SMALLFLOAT': 'REAL', 'DECIMAL': 'NUMERIC', 
    'SERIAL': 'SERIAL', 'DATE': 'DATE', 'MONEY': 'NUMERIC(12,2)', 
    'DATETIME': 'TIMESTAMP', 'BYTE': 'BYTEA', 'TEXT': 'TEXT', 
    'VARCHAR': 'VARCHAR', 'LVARCHAR': 'TEXT', 'INTERVAL': 'INTERVAL', 
    'NCHAR': 'CHAR', 'NVARCHAR': 'VARCHAR', 'INT8': 'BIGINT', 
    'SERIAL8': 'BIGSERIAL', 'BLOB': 'BYTEA', 'BOOLEAN': 'BOOLEAN', 
    'BIGINT': 'BIGINT', 'BIGSERIAL': 'BIGSERIAL',
}

POSTGRES_RESERVED_KEYWORDS = {
    'user', 'order', 'select', 'from', 'where', 'insert', 'update', 'delete',
    'group', 'having', 'create', 'drop', 'alter', 'table', 'index', 'view',
    'union', 'all', 'and', 'or', 'not', 'null', 'default', 'primary', 'foreign',
    'key', 'check', 'unique', 'references', 'on', 'to', 'as', 'is', 'in',
    'exists', 'like', 'between', 'distinct', 'case', 'when', 'then', 'else',
    'end', 'cast', 'extract', 'interval', 'timestamp', 'date', 'time'
}

def escape_identifier(name):
    if name.lower() in POSTGRES_RESERVED_KEYWORDS:
        return f'"{name}"'
    return name

class MigrationLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.start_time = datetime.now()
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] [{level}] {message}"
        print(log_line)
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_line + '\n')
    def error(self, message): self.log(message, "ERROR")
    def warning(self, message): self.log(message, "WARN")
    def success(self, message): self.log(message, "SUCCESS")

class Checkpoint:
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.data = self.load()
    def load(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f: return json.load(f)
        return {'completed_tables': [], 'failed_tables': [], 'last_table': None, 'start_time': datetime.now().isoformat(), 'stats': {}}
    def save(self):
        with open(self.checkpoint_file, 'w') as f: json.dump(self.data, f, indent=2)
    def mark_completed(self, table_name, row_count, duration):
        self.data['completed_tables'].append(table_name)
        self.data['stats'][table_name] = {'rows': row_count, 'duration': duration, 'status': 'completed'}
        self.save()
    def mark_failed(self, table_name, error):
        self.data['failed_tables'].append(table_name)
        self.data['stats'][table_name] = {'status': 'failed', 'error': str(error)}
        self.save()
    def is_completed(self, table_name): return table_name in self.data['completed_tables']

def connect_informix():
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
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        raise Exception(f"PostgreSQL connection failed: {e}")

def get_all_tables(ifx_conn, logger):
    logger.log("Fetching table list from Informix...")
    cursor = ifx_conn.cursor()
    cursor.execute("SELECT tabname, nrows FROM systables WHERE tabid > 99 AND tabtype = 'T' ORDER BY nrows ASC")
    tables = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        tables.append({'name': row[0], 'rows': row[1] if row[1] else 0})
    cursor.close()
    return tables

def get_table_schema(ifx_conn, table_name, logger):
    cursor = ifx_conn.cursor()
    cursor.execute(f"SELECT c.colname, c.coltype, MOD(c.coltype, 256) as base_type, c.collength, CASE WHEN c.coltype >= 256 THEN 1 ELSE 0 END as not_null FROM syscolumns c JOIN systables t ON c.tabid = t.tabid WHERE t.tabname = '{table_name}' ORDER BY c.colno")
    columns = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        col_name, base_type_code, col_length, not_null = row[0], row[2], row[3], row[4] == 1
        ifx_type = TYPE_MAPPING.get(base_type_code, 'VARCHAR')
        pg_type = POSTGRES_TYPE_MAPPING.get(ifx_type, 'TEXT')
        if ifx_type in ['CHAR', 'NCHAR'] and col_length: pg_type = f"CHAR({col_length})"
        elif ifx_type in ['VARCHAR', 'NVARCHAR'] and col_length: pg_type = f"VARCHAR({col_length})"
        elif ifx_type == 'DECIMAL' and col_length:
            precision, scale = (col_length >> 8) & 0xFF, col_length & 0xFF
            pg_type = f"NUMERIC({precision},{scale})" if 0 < precision <= 1000 else "NUMERIC(12,2)"
        columns.append({'name': col_name, 'type': pg_type, 'not_null': not_null})
    cursor.close()
    return columns

def create_table_postgres(pg_conn, table_name, columns, logger):
    escaped_table_name = escape_identifier(table_name)
    col_defs = [f"{escape_identifier(c['name'])} {c['type']} {'NOT NULL' if c['not_null'] else ''}" for c in columns]
    create_sql = f"CREATE TABLE IF NOT EXISTS {escaped_table_name} (\n  " + ",\n  ".join(col_defs) + "\n)"
    try:
        cursor = pg_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {escaped_table_name} CASCADE")
        cursor.execute(create_sql)
        pg_conn.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        pg_conn.rollback()
        return False

def migrate_table_data(ifx_conn, pg_conn, table_name, columns, total_rows, logger):
    escaped_table_name = escape_identifier(table_name)
    escaped_col_names = [escape_identifier(col['name']) for col in columns]
    placeholders = ', '.join(['%s'] * len(escaped_col_names))
    insert_sql = f"INSERT INTO {escaped_table_name} ({', '.join(escaped_col_names)}) VALUES ({placeholders})"
    ifx_cursor, pg_cursor = ifx_conn.cursor(), pg_conn.cursor()
    ifx_cursor.execute(f"SELECT * FROM {table_name}")
    rows_migrated, batch = 0, []
    while True:
        row = ifx_cursor.fetchone()
        if row is None: break
        batch.append(tuple(row))
        if len(batch) >= BATCH_SIZE:
            pg_cursor.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_migrated += len(batch)
            batch = []
    if batch:
        pg_cursor.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_migrated += len(batch)
    ifx_cursor.close(); pg_cursor.close()
    return rows_migrated

def migrate_single_table(ifx_conn, pg_conn, table_info, logger, checkpoint):
    table_name, total_rows = table_info['name'], table_info['rows']
    start_time = datetime.now()
    try:
        columns = get_table_schema(ifx_conn, table_name, logger)
        if not create_table_postgres(pg_conn, table_name, columns, logger): raise Exception("Creation failed")
        rows = migrate_table_data(ifx_conn, pg_conn, table_name, columns, total_rows, logger) if total_rows > 0 else 0
        checkpoint.mark_completed(table_name, rows, (datetime.now() - start_time).total_seconds())
        return True
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        checkpoint.mark_failed(table_name, str(e))
        return False

def main():
    logger, checkpoint = MigrationLogger(LOG_FILE), Checkpoint(CHECKPOINT_FILE)
    os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'
    try:
        ifx_conn, pg_conn = connect_informix(), connect_postgres()
        logger.success("Databases connected via environment secrets")
        tables = get_all_tables(ifx_conn, logger)
        pending = [t for t in tables if not checkpoint.is_completed(t['name'])]
        for i, t in enumerate(pending, 1):
            migrate_single_table(ifx_conn, pg_conn, t, logger, checkpoint)
        ifx_conn.close(); pg_conn.close()
    except Exception as e:
        logger.error(f"FATAL: {e}"); sys.exit(1)

if __name__ == "__main__":
    main()