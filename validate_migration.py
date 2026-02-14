#!/usr/bin/env python3
"""
Validierung: Informix → PostgreSQL Migration
Zentralisiertes Sicherheits-Update: Nutzt db_config.py
"""

import sys
import os
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres, PG_CONFIG

def validate_table_count(ifx_conn, pg_conn):
    """Vergleicht die Anzahl der Tabellen in beiden Systemen"""
    print("=" * 80)
    print("1. TABELLEN-ANZAHL VALIDIERUNG")
    print("=" * 80)
    
    # Informix
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute("SELECT COUNT(*) FROM systables WHERE tabtype = 'T' AND tabid > 99")
    ifx_count = ifx_cursor.fetchone()[0]
    
    # PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
    pg_count = pg_cursor.fetchone()[0]
    
    print(f"Informix Tables:   {ifx_count}")
    print(f"PostgreSQL Tables: {pg_count}")
    
    match = ifx_count == pg_count
    print(f"{'✓' if match else '✗'} {'MATCH!' if match else 'MISMATCH!'}")
    return match

def validate_large_tables(ifx_conn, pg_conn):
    """Vergleicht Zeilenzahlen der 10 größten Tabellen"""
    print("\n" + "=" * 80)
    print("2. TOP 10 TABELLEN - ROW COUNT VALIDIERUNG")
    print("=" * 80)
    
    pg_cursor = pg_conn.cursor()
    # Wir holen die Top 10 Tabellennamen basierend auf der aktuellen Statistik
    pg_cursor.execute("""
        SELECT relname, n_live_tup 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC 
        LIMIT 10
    """)
    
    all_match = True
    rows = pg_cursor.fetchall()
    
    if not rows:
        print("⚠ Keine Daten in PostgreSQL gefunden oder Statistiken nicht aktuell.")
        return False

    for table_name, pg_rows in rows:
        ifx_cursor = ifx_conn.cursor()
        try:
            ifx_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            ifx_rows = ifx_cursor.fetchone()[0]
            
            # Da n_live_tup ein Schätzwert sein kann, machen wir zur Sicherheit 
            # bei Mismatch einen echten COUNT(*) in PG
            if ifx_rows != pg_rows:
                pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                pg_rows = pg_cursor.fetchone()[0]

            is_match = ifx_rows == pg_rows
            icon = "✓" if is_match else "✗"
            print(f"{icon} {table_name:30} | IFX: {ifx_rows:>10,} | PG: {pg_rows:>10,}")
            
            if not is_match:
                all_match = False
        except Exception as e:
            print(f"✗ {table_name:30} | ERROR: {e}")
            all_match = False
            
    return all_match

def validate_data_integrity(pg_conn):
    """Zusätzliche Integritätschecks"""
    print("\n" + "=" * 80)
    print("3. DATEN-INTEGRITÄT & STATISTIK")
    print("=" * 80)
    
    cursor = pg_conn.cursor()
    
    # Gesamtzeilenzahl
    cursor.execute("SELECT SUM(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'public'")
    total_rows = cursor.fetchone()[0] or 0
    print(f"Gesamtanzahl migrierter Zeilen (ca.): {total_rows:,}")
    
    # Datenbankgröße
    db_name = PG_CONFIG['database']
    cursor.execute(f"SELECT pg_size_pretty(pg_database_size('{db_name}'))")
    size = cursor.fetchone()[0]
    print(f"Größe der PostgreSQL Datenbank '{db_name}': {size}")

def main():
    print("=" * 80)
    print("CATUNO MIGRATION VALIDIERUNG (SECURE MODE)")
    print("=" * 80)
    
    try:
        # Verbindungen über zentrale Config
        ifx_conn = connect_informix()
        pg_conn = connect_postgres()
        print("✓ Verbindungen erfolgreich aufgebaut\n")
        
        # Validierungen ausführen
        res_count = validate_table_count(ifx_conn, pg_conn)
        res_rows = validate_large_tables(ifx_conn, pg_conn)
        validate_data_integrity(pg_conn)
        
        # Fazit
        print("\n" + "=" * 80)
        print("ERGEBNIS")
        print("-" * 80)
        if res_count and res_rows:
            print("✓✓✓ VALIDIERUNG ERFOLGREICH! Alle Kern-Metriken passen. ✓✓✓")
            return 0
        else:
            print("✗✗✗ VALIDIERUNG FEHLGESCHLAGEN! Bitte Logs prüfen. ✗✗✗")
            return 1
            
    except Exception as e:
        print(f"❌ KRITISCHER FEHLER: {e}")
        return 1
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        print("\nVerbindungen geschlossen.")

if __name__ == "__main__":
    sys.exit(main())