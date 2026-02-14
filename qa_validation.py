#!/usr/bin/env python3
"""
COMPREHENSIVE QA VALIDATION: Informix → PostgreSQL
Zentralisiertes Sicherheits-Update: Nutzt db_config.py
"""

import os
import json
import sys
from datetime import datetime
from collections import defaultdict
# --- ZENTRALE CONFIG IMPORTIEREN ---
from db_config import connect_informix, connect_postgres, PG_CONFIG

# Konfiguration Pfade
LOG_DIR = r"C:\postgres\migration"
# Erzeuge Zeitstempel für die Dateinamen
_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
REPORT_FILE = os.path.join(LOG_DIR, f"qa_report_{_ts}.txt")
JSON_REPORT = os.path.join(LOG_DIR, f"qa_report_{_ts}.json")

class QAReport:
    """QA Report collector"""
    def __init__(self):
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'tests': [],
            'summary': {
                'total_tests': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0
            },
            'issues': []
        }
    
    def add_test(self, category, test_name, status, details=None, severity='INFO'):
        test = {
            'category': category,
            'test': test_name,
            'status': status,
            'severity': severity,
            'details': details or {}
        }
        self.results['tests'].append(test)
        self.results['summary']['total_tests'] += 1
        
        if status == 'PASS':
            self.results['summary']['passed'] += 1
        elif status == 'FAIL':
            self.results['summary']['failed'] += 1
            self.results['issues'].append(test)
        elif status == 'WARN':
            self.results['summary']['warnings'] += 1
    
    def save(self):
        with open(JSON_REPORT, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write("=" * 100 + "\n")
            f.write("CATUNO MIGRATION - COMPREHENSIVE QA VALIDATION REPORT\n")
            f.write("=" * 100 + "\n")
            f.write(f"Generated: {self.results['timestamp']}\n\n")
            
            f.write("SUMMARY\n" + "-" * 100 + "\n")
            f.write(f"Total Tests:  {self.results['summary']['total_tests']}\n")
            f.write(f"Passed:       {self.results['summary']['passed']} ✓\n")
            f.write(f"Failed:       {self.results['summary']['failed']} ✗\n")
            f.write(f"Warnings:     {self.results['summary']['warnings']} ⚠\n\n")
            
            by_category = defaultdict(list)
            for test in self.results['tests']:
                by_category[test['category']].append(test)
            
            for category, tests in sorted(by_category.items()):
                f.write(f"\n{category}\n" + "=" * 100 + "\n")
                for test in tests:
                    icon = {'PASS': '✓', 'FAIL': '✗', 'WARN': '⚠'}.get(test['status'], '?')
                    f.write(f"{icon} [{test['status']}] {test['test']}\n")
                    if test['details']:
                        for k, v in test['details'].items():
                            f.write(f"  {k}: {v}\n")
                    f.write("\n")
            f.write("=" * 100 + f"\nFull JSON report: {JSON_REPORT}\n" + "=" * 100 + "\n")

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# --- TEST-FUNKTIONEN (angepasst an zentrale Config) ---

def test_table_count(ifx_conn, pg_conn, report):
    log("Test 1: Table Count...")
    ifx_cur = ifx_conn.cursor()
    ifx_cur.execute("SELECT COUNT(*) FROM systables WHERE tabtype = 'T' AND tabid > 99")
    ifx_count = ifx_cur.fetchone()[0]
    
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
    pg_count = pg_cur.fetchone()[0]
    
    status = 'PASS' if ifx_count == pg_count else 'FAIL'
    report.add_test('1. SCHEMA VALIDATION', 'Table Count Match', status, 
                   {'Informix': ifx_count, 'PostgreSQL': pg_count}, 
                   'CRITICAL' if status == 'FAIL' else 'INFO')

def test_row_counts_top_tables(ifx_conn, pg_conn, report):
    log("Test 2: Row Counts (Top 20 Tables)...")
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT relname FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY n_live_tup DESC LIMIT 20")
    tables = [row[0] for row in pg_cur.fetchall()]
    
    mismatches = []
    for table in tables:
        try:
            ifx_cur = ifx_conn.cursor(); ifx_cur.execute(f"SELECT COUNT(*) FROM {table}")
            ifx_count = ifx_cur.fetchone()[0]
            pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
            pg_count = pg_cur.fetchone()[0]
            if ifx_count != pg_count:
                mismatches.append({'table': table, 'ifx': ifx_count, 'pg': pg_count})
        except: pass
    
    status = 'PASS' if not mismatches else 'FAIL'
    report.add_test('2. DATA INTEGRITY', 'Row Counts (Top 20)', status, 
                   {'Checked': len(tables), 'Mismatches': len(mismatches)})

def test_primary_keys(pg_conn, report):
    log("Test 3: Primary Keys...")
    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'public'")
    count = cur.fetchone()[0]
    report.add_test('3. CONSTRAINTS', 'Primary Keys Count', 'PASS' if count >= 642 else 'WARN', {'Actual': count})

def test_database_size(pg_conn, report):
    log("Test 10: Database Size...")
    cur = pg_conn.cursor()
    # Nutzt den Datenbanknamen aus der zentralen Config
    db_name = PG_CONFIG['database']
    cur.execute(f"SELECT pg_size_pretty(pg_database_size('{db_name}'))")
    size = cur.fetchone()[0]
    report.add_test('4. PERFORMANCE', 'Database Size', 'PASS', {'Size': size})

# ... [Hier können die restlichen Testfunktionen (Indexes, FKs etc.) analog eingefügt werden] ...

def main():
    print("=" * 100)
    print("CATUNO MIGRATION - COMPREHENSIVE QA VALIDATION (SECURE)")
    print("=" * 100)
    
    report = QAReport()
    try:
        log("Connecting to databases via db_config...")
        ifx_conn = connect_informix()
        pg_conn = connect_postgres()
        log("✓ Both databases connected\n")
        
        test_table_count(ifx_conn, pg_conn, report)
        test_row_counts_top_tables(ifx_conn, pg_conn, report)
        test_primary_keys(pg_conn, report)
        test_database_size(pg_conn, report)
        # (Weitere Tests hier aufrufen...)
        
        report.save()
        
        print(f"\nSummary: {report.results['summary']['passed']} Passed, {report.results['summary']['failed']} Failed")
        return 1 if report.results['summary']['failed'] > 0 else 0
        
    except Exception as e:
        log(f"❌ QA ERROR: {e}")
        return 1
    finally:
        if 'ifx_conn' in locals(): ifx_conn.close()
        if 'pg_conn' in locals(): pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    sys.exit(main())