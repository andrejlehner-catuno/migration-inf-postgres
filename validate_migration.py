#!/usr/bin/env python3
"""
Validierung: Informix → PostgreSQL Migration
Vergleicht Tabellenzahlen und Row Counts
"""

import jaydebeapi
import psycopg2
import os

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

os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'

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

def validate_table_count(ifx_conn, pg_conn):
    """Validate table counts"""
    print("=" * 80)
    print("1. TABELLEN-ANZAHL VALIDIERUNG")
    print("=" * 80)
    
    # Informix
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute("""
        SELECT COUNT(*) 
        FROM systables 
        WHERE tabtype = 'T' AND tabid > 99
    """)
    ifx_count = ifx_cursor.fetchone()[0]
    ifx_cursor.close()
    
    # PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    pg_count = pg_cursor.fetchone()[0]
    pg_cursor.close()
    
    print(f"Informix Tables:   {ifx_count}")
    print(f"PostgreSQL Tables: {pg_count}")
    
    if ifx_count == pg_count:
        print("✓ TABLE COUNT MATCHES!")
    else:
        print(f"✗ MISMATCH! Difference: {abs(ifx_count - pg_count)}")
    
    print()
    return ifx_count == pg_count

def validate_large_tables(ifx_conn, pg_conn):
    """Validate row counts for large tables"""
    print("=" * 80)
    print("2. GROSSE TABELLEN - ROW COUNT VALIDIERUNG")
    print("=" * 80)
    
    # Get top 10 tables by row count from PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT relname, n_live_tup 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC 
        LIMIT 10
    """)
    
    all_match = True
    
    for table_name, pg_rows in pg_cursor.fetchall():
        # Count in Informix
        ifx_cursor = ifx_conn.cursor()
        try:
            ifx_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            ifx_rows = ifx_cursor.fetchone()[0]
            ifx_cursor.close()
            
            match = "✓" if ifx_rows == pg_rows else "✗"
            print(f"{match} {table_name:30} | IFX: {ifx_rows:>10} | PG: {pg_rows:>10}")
            
            if ifx_rows != pg_rows:
                all_match = False
                print(f"   MISMATCH: Difference = {abs(ifx_rows - pg_rows)}")
        except Exception as e:
            print(f"✗ {table_name:30} | ERROR: {e}")
            all_match = False
    
    pg_cursor.close()
    print()
    return all_match

def validate_decimal_fields(pg_conn):
    """Validate DECIMAL/NUMERIC fields"""
    print("=" * 80)
    print("3. DECIMAL/NUMERIC FELDER VALIDIERUNG")
    print("=" * 80)
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT 
            table_name,
            column_name,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type = 'numeric'
          AND (numeric_precision > 1000 OR numeric_precision IS NULL)
        LIMIT 20
    """)
    
    invalid_decimals = cursor.fetchall()
    
    if not invalid_decimals:
        print("✓ All DECIMAL fields have valid precision!")
    else:
        print("✗ Found invalid DECIMAL fields:")
        for table, column, prec, scale in invalid_decimals:
            print(f"  {table}.{column}: NUMERIC({prec},{scale})")
    
    cursor.close()
    print()
    return len(invalid_decimals) == 0

def validate_data_integrity(pg_conn):
    """Basic data integrity checks"""
    print("=" * 80)
    print("4. DATEN-INTEGRITÄT CHECKS")
    print("=" * 80)
    
    cursor = pg_conn.cursor()
    
    # Check for empty tables that shouldn't be empty
    cursor.execute("""
        SELECT relname 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
        AND n_live_tup = 0
        LIMIT 10
    """)
    
    empty_tables = cursor.fetchall()
    print(f"Empty tables: {len(empty_tables)}")
    if empty_tables:
        for (table,) in empty_tables[:5]:
            print(f"  - {table}")
    
    # Total row count
    cursor.execute("""
        SELECT SUM(n_live_tup) 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
    """)
    total_rows = cursor.fetchone()[0]
    print(f"\nTotal rows migrated: {total_rows:,}")
    
    cursor.close()
    print()

def main():
    """Main validation"""
    print("=" * 80)
    print("CATUNO MIGRATION VALIDIERUNG")
    print("Informix → PostgreSQL")
    print("=" * 80)
    print()
    
    # Connect
    print("Connecting to databases...")
    ifx_conn = connect_informix()
    print("✓ Informix connected")
    
    pg_conn = connect_postgres()
    print("✓ PostgreSQL connected")
    print()
    
    try:
        # Run validations
        result1 = validate_table_count(ifx_conn, pg_conn)
        result2 = validate_large_tables(ifx_conn, pg_conn)
        result3 = validate_decimal_fields(pg_conn)
        validate_data_integrity(pg_conn)
        
        # Summary
        print("=" * 80)
        print("VALIDIERUNGS-ZUSAMMENFASSUNG")
        print("=" * 80)
        print(f"Table Count Match:     {'✓ PASS' if result1 else '✗ FAIL'}")
        print(f"Row Count Match:       {'✓ PASS' if result2 else '✗ FAIL'}")
        print(f"DECIMAL Fields Valid:  {'✓ PASS' if result3 else '✗ FAIL'}")
        
        if result1 and result2 and result3:
            print()
            print("✓✓✓ MIGRATION VALIDATION SUCCESSFUL! ✓✓✓")
        else:
            print()
            print("✗✗✗ VALIDATION FAILED - CHECK DETAILS ABOVE ✗✗✗")
        
    finally:
        ifx_conn.close()
        pg_conn.close()
        print()
        print("Connections closed")

if __name__ == "__main__":
    main()
