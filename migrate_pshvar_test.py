#!/usr/bin/env python3
"""
MIGRATION: uno_awlp (Informix → PostgreSQL)
Zentralisiertes Sicherheits-Update: Nutzt db_config.py
"""

import os
import sys
from datetime import datetime
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres

# Konfiguration
TABLE_NAME = 'uno_awlp'
BATCH_SIZE = 100

def log(message):
    """Einfaches Logging mit Zeitstempel"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def count_rows(conn, table, db_type="PostgreSQL"):
    """Zählt Zeilen in der angegebenen Datenbank"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def migrate_data(ifx_conn, pg_conn):
    """Migriert Daten von Informix nach PostgreSQL"""
    
    total_rows = count_rows(ifx_conn, TABLE_NAME, "Informix")
    log(f"Quell-Tabelle {TABLE_NAME} hat {total_rows} Zeilen")
    
    # Ziel-Tabelle leeren
    log(f"Leere Ziel-Tabelle {TABLE_NAME}...")
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    pg_conn.commit()
    log("✓ Ziel-Tabelle geleert")
    
    # Aus Informix lesen
    log("Lese Daten aus Informix...")
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    
    # In PostgreSQL einfügen (Batch-Verfahren)
    # WICHTIG: Spaltennamen explizit angeben
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} 
    (u40_awlnr, u40_spr, u40_lfdnr, u40_anzwert, u40_dbwert, 
     u40_explain, u40_zus1, u40_zus2, u40_zus3, u40_zus4, u40_zus5)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    rows_migrated = 0
    batch = []
    
    log("Starte Migration...")
    
    while True:
        row = ifx_cursor.fetchone()
        if row is None:
            break
    
        batch.append(tuple(row))
    
        if len(batch) >= BATCH_SIZE:
            pg_cursor.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_migrated += len(batch)
            # Fortschrittsanzeige in einer Zeile
            sys.stdout.write(f"\rFortschritt: {rows_migrated}/{total_rows} Zeilen ({100*rows_migrated//total_rows}%)")
            sys.stdout.flush()
            batch = []
    
    # Verbleibende Zeilen einfügen
    if batch:
        pg_cursor.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_migrated += len(batch)
        print(f"\rFortschritt: {rows_migrated}/{total_rows} Zeilen (100%)")
    
    ifx_cursor.close()
    pg_cursor.close()
    
    log(f"✓ {rows_migrated} Zeilen migriert")
    return rows_migrated

def main():
    log("=" * 60)
    log(f"TEST MIGRATION: {TABLE_NAME} (Secure Mode)")
    log("=" * 60)
    
    try:
        # Verbindungen über zentrale Config
        ifx_conn = connect_informix()
        pg_conn = connect_postgres()
        
        start_time = datetime.now()
        rows = migrate_data(ifx_conn, pg_conn)
        duration = (datetime.now() - start_time).total_seconds()
        
        # Verifizierung
        pg_count = count_rows(pg_conn, TABLE_NAME)
        
        log(f"Dauer: {duration:.2f} Sekunden")
        if rows > 0 and duration > 0:
            log(f"Geschwindigkeit: {rows/duration:.0f} Zeilen/Sek")
        
        if pg_count == rows:
            log("=" * 60)
            log("✓✓✓ MIGRATION ERFOLGREICH! ✓✓✓")
            log("=" * 60)
        else:
            log("=" * 60)
            log(f"✗ FEHLER: Zeilenzahl weicht ab! (PG: {pg_count})")
            log("=" * 60)
            sys.exit(1)
            
    except Exception as e:
        log(f"✗ Migrationsfehler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        log("Verbindungen geschlossen")

if __name__ == "__main__":
    main()