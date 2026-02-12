#!/usr/bin/env python3
"""
Test-Migration: pshvar Tabelle von Informix nach PostgreSQL
Informix: READ-ONLY (JDBC)
PostgreSQL: WRITE
"""

import jaydebeapi
import psycopg2
from datetime import datetime
import sys

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
    'database': 'catunoqs_pg',
    'user': 'catuno',
    'password': 'start12345'
}

TABLE_NAME = 'uno_awlp'
BATCH_SIZE = 100

def log(message):
    """Simple logging with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def connect_informix():
    """Connect to Informix via JDBC (READ-ONLY)"""
    try:
        log("Connecting to Informix via JDBC...")
        conn = jaydebeapi.connect(
            INFORMIX_JDBC_DRIVER,
            INFORMIX_JDBC_URL,
            [INFORMIX_USER, INFORMIX_PASSWORD],
            INFORMIX_JDBC_JAR
        )
        log("✓ Informix connected via JDBC (READ-ONLY)")
        return conn
    except Exception as e:
        log(f"✗ Informix JDBC connection failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def connect_postgres():
    """Connect to PostgreSQL"""
    try:
        log("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        log("✓ PostgreSQL connected")
        return conn
    except Exception as e:
        log(f"✗ PostgreSQL connection failed: {e}")
        sys.exit(1)

def count_rows_informix(conn):
    """Count rows in Informix"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def count_rows_postgres(conn):
    """Count rows in PostgreSQL"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def migrate_data(ifx_conn, pg_conn):
    """Migrate data from Informix to PostgreSQL"""
    
    # Count source rows
    total_rows = count_rows_informix(ifx_conn)
    log(f"Source table has {total_rows} rows")
    
    # Clear target table
    log("Clearing target table...")
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    pg_conn.commit()
    log("✓ Target table cleared")
    
    # Read from Informix
    log("Reading data from Informix...")
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    
    # Insert into PostgreSQL in batches
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} 
    (u40_awlnr, u40_spr, u40_lfdnr, u40_anzwert, u40_dbwert, 
     u40_explain, u40_zus1, u40_zus2, u40_zus3, u40_zus4, u40_zus5)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    rows_migrated = 0
    batch = []
    
    log("Migrating data...")
    
    while True:
        row = ifx_cursor.fetchone()
        if row is None:
            break
    
        batch.append(tuple(row))
    
        if len(batch) >= BATCH_SIZE:
            pg_cursor.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_migrated += len(batch)
            print(f"\rProgress: {rows_migrated}/{total_rows} rows ({100*rows_migrated//total_rows}%)", end='')
            batch = []
    
    # Insert remaining rows
    if batch:
        pg_cursor.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_migrated += len(batch)
        print(f"\rProgress: {rows_migrated}/{total_rows} rows (100%)")
    
    ifx_cursor.close()
    pg_cursor.close()
    
    log(f"✓ Migrated {rows_migrated} rows")
    
    return rows_migrated

def verify_migration(ifx_conn, pg_conn):
    """Verify migration by comparing row counts"""
    log("Verifying migration...")
    
    ifx_count = count_rows_informix(ifx_conn)
    pg_count = count_rows_postgres(pg_conn)
    
    log(f"Informix rows: {ifx_count}")
    log(f"PostgreSQL rows: {pg_count}")
    
    if ifx_count == pg_count:
        log("✓ Row counts match!")
        return True
    else:
        log(f"✗ Row count mismatch! Difference: {abs(ifx_count - pg_count)}")
        return False

def main():
    """Main migration function"""
    log("=" * 60)
    log(f"TEST MIGRATION: {TABLE_NAME}")
    log("=" * 60)
    
    # Connect
    ifx_conn = connect_informix()
    pg_conn = connect_postgres()
    
    try:
        # Migrate
        start_time = datetime.now()
        rows = migrate_data(ifx_conn, pg_conn)
        duration = (datetime.now() - start_time).total_seconds()
        
        log(f"Migration completed in {duration:.2f} seconds")
        log(f"Speed: {rows/duration:.0f} rows/sec")
        
        # Verify
        success = verify_migration(ifx_conn, pg_conn)
        
        if success:
            log("=" * 60)
            log("✓✓✓ TEST MIGRATION SUCCESSFUL! ✓✓✓")
            log("=" * 60)
        else:
            log("=" * 60)
            log("✗✗✗ TEST MIGRATION FAILED! ✗✗✗")
            log("=" * 60)
            sys.exit(1)
            
    except Exception as e:
        log(f"✗ Migration error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        ifx_conn.close()
        pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    main()
