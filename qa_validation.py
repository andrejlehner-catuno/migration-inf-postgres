#!/usr/bin/env python3
"""
COMPREHENSIVE QA VALIDATION: Informix → PostgreSQL
Detaillierte Validierung der Migration mit umfassendem Report
"""

import jaydebeapi
import psycopg2
import os
import json
from datetime import datetime
from collections import defaultdict

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
REPORT_FILE = os.path.join(LOG_DIR, f"qa_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
JSON_REPORT = os.path.join(LOG_DIR, f"qa_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

os.environ['JAVA_HOME'] = r'C:\baustelle_8.6\jdk-17.0.11.9-hotspot'

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
        """Add test result"""
        test = {
            'category': category,
            'test': test_name,
            'status': status,  # PASS, FAIL, WARN
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
        """Save report to files"""
        # JSON report
        with open(JSON_REPORT, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Text report
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write("=" * 100 + "\n")
            f.write("CATUNO MIGRATION - COMPREHENSIVE QA VALIDATION REPORT\n")
            f.write("=" * 100 + "\n")
            f.write(f"Generated: {self.results['timestamp']}\n")
            f.write("\n")
            
            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 100 + "\n")
            f.write(f"Total Tests:  {self.results['summary']['total_tests']}\n")
            f.write(f"Passed:       {self.results['summary']['passed']} ✓\n")
            f.write(f"Failed:       {self.results['summary']['failed']} ✗\n")
            f.write(f"Warnings:     {self.results['summary']['warnings']} ⚠\n")
            f.write("\n")
            
            # Group by category
            by_category = defaultdict(list)
            for test in self.results['tests']:
                by_category[test['category']].append(test)
            
            # Write each category
            for category, tests in sorted(by_category.items()):
                f.write(f"\n{category}\n")
                f.write("=" * 100 + "\n")
                
                for test in tests:
                    status_icon = {'PASS': '✓', 'FAIL': '✗', 'WARN': '⚠'}.get(test['status'], '?')
                    f.write(f"{status_icon} [{test['status']}] {test['test']}\n")
                    
                    if test['details']:
                        for key, value in test['details'].items():
                            f.write(f"  {key}: {value}\n")
                    f.write("\n")
            
            # Issues section
            if self.results['issues']:
                f.write("\n" + "=" * 100 + "\n")
                f.write("ISSUES FOUND\n")
                f.write("=" * 100 + "\n")
                for issue in self.results['issues']:
                    f.write(f"✗ {issue['category']}: {issue['test']}\n")
                    if issue['details']:
                        for key, value in issue['details'].items():
                            f.write(f"  {key}: {value}\n")
                    f.write("\n")
            
            f.write("=" * 100 + "\n")
            f.write(f"Full JSON report: {JSON_REPORT}\n")
            f.write("=" * 100 + "\n")

def log(message):
    """Print log message"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

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

def test_table_count(ifx_conn, pg_conn, report):
    """Test 1: Table Count Comparison"""
    log("Test 1: Table Count...")
    
    # Informix
    ifx_cursor = ifx_conn.cursor()
    ifx_cursor.execute("SELECT COUNT(*) FROM systables WHERE tabtype = 'T' AND tabid > 99")
    ifx_count = ifx_cursor.fetchone()[0]
    ifx_cursor.close()
    
    # PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
    pg_count = pg_cursor.fetchone()[0]
    pg_cursor.close()
    
    status = 'PASS' if ifx_count == pg_count else 'FAIL'
    report.add_test(
        '1. SCHEMA VALIDATION',
        'Table Count Match',
        status,
        {
            'Informix': ifx_count,
            'PostgreSQL': pg_count,
            'Difference': abs(ifx_count - pg_count)
        },
        'CRITICAL' if status == 'FAIL' else 'INFO'
    )

def test_row_counts_top_tables(ifx_conn, pg_conn, report):
    """Test 2: Row Counts for Top 20 Tables"""
    log("Test 2: Row Counts (Top 20 Tables)...")
    
    # Get top 20 tables from PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT relname 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC 
        LIMIT 20
    """)
    
    tables = [row[0] for row in pg_cursor.fetchall()]
    pg_cursor.close()
    
    mismatches = []
    
    for table in tables:
        try:
            # Informix count
            ifx_cursor = ifx_conn.cursor()
            ifx_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            ifx_count = ifx_cursor.fetchone()[0]
            ifx_cursor.close()
            
            # PostgreSQL count
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            
            if ifx_count != pg_count:
                mismatches.append({
                    'table': table,
                    'informix': ifx_count,
                    'postgresql': pg_count,
                    'diff': abs(ifx_count - pg_count)
                })
        except Exception as e:
            mismatches.append({
                'table': table,
                'error': str(e)
            })
    
    status = 'PASS' if len(mismatches) == 0 else 'FAIL'
    details = {
        'Tables Checked': len(tables),
        'Mismatches': len(mismatches)
    }
    
    if mismatches:
        details['Failed Tables'] = ', '.join([m['table'] for m in mismatches[:5]])
    
    report.add_test(
        '2. DATA INTEGRITY',
        'Row Counts (Top 20 Tables)',
        status,
        details,
        'HIGH' if status == 'FAIL' else 'INFO'
    )

def test_primary_keys(pg_conn, report):
    """Test 3: Primary Keys Count"""
    log("Test 3: Primary Keys...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.table_constraints
        WHERE constraint_type = 'PRIMARY KEY'
          AND table_schema = 'public'
    """)
    pk_count = cursor.fetchone()[0]
    cursor.close()
    
    # Expected: 642 (from migration log)
    expected = 642
    status = 'PASS' if pk_count == expected else 'WARN'
    
    report.add_test(
        '3. CONSTRAINTS',
        'Primary Keys Count',
        status,
        {
            'Expected': expected,
            'Actual': pk_count,
            'Difference': abs(pk_count - expected)
        }
    )

def test_indexes(pg_conn, report):
    """Test 4: Indexes Count"""
    log("Test 4: Indexes...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_indexes 
        WHERE schemaname = 'public'
          AND indexname NOT LIKE '%_pkey'
    """)
    idx_count = cursor.fetchone()[0]
    cursor.close()
    
    # Expected: 1957 (from migration log)
    expected = 1957
    status = 'PASS' if idx_count == expected else 'WARN'
    
    report.add_test(
        '3. CONSTRAINTS',
        'Indexes Count',
        status,
        {
            'Expected': expected,
            'Actual': idx_count,
            'Difference': abs(idx_count - expected)
        }
    )

def test_foreign_keys(pg_conn, report):
    """Test 5: Foreign Keys Count"""
    log("Test 5: Foreign Keys...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.table_constraints
        WHERE constraint_type = 'FOREIGN KEY'
          AND table_schema = 'public'
    """)
    fk_count = cursor.fetchone()[0]
    cursor.close()
    
    # Expected: 2 (from migration log)
    expected = 2
    status = 'PASS' if fk_count == expected else 'WARN'
    
    report.add_test(
        '3. CONSTRAINTS',
        'Foreign Keys Count',
        status,
        {
            'Expected': expected,
            'Actual': fk_count
        }
    )

def test_total_rows(ifx_conn, pg_conn, report):
    """Test 6: Total Row Count"""
    log("Test 6: Total Row Count...")
    
    # PostgreSQL total
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT SUM(n_live_tup) 
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
    """)
    pg_total = pg_cursor.fetchone()[0] or 0
    pg_cursor.close()
    
    # Expected: ~4.86M (from migration log)
    expected_min = 4_800_000
    expected_max = 4_900_000
    
    status = 'PASS' if expected_min <= pg_total <= expected_max else 'WARN'
    
    report.add_test(
        '2. DATA INTEGRITY',
        'Total Row Count',
        status,
        {
            'Total Rows': f"{pg_total:,}",
            'Expected Range': f"{expected_min:,} - {expected_max:,}"
        }
    )

def test_data_types(pg_conn, report):
    """Test 7: Data Types Validation"""
    log("Test 7: Data Types...")
    
    cursor = pg_conn.cursor()
    
    # Check for invalid NUMERIC precision
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type = 'numeric'
          AND numeric_precision > 1000
    """)
    invalid_decimals = cursor.fetchone()[0]
    
    cursor.close()
    
    status = 'PASS' if invalid_decimals == 0 else 'FAIL'
    
    report.add_test(
        '1. SCHEMA VALIDATION',
        'DECIMAL/NUMERIC Precision',
        status,
        {
            'Invalid Columns': invalid_decimals
        },
        'CRITICAL' if status == 'FAIL' else 'INFO'
    )

def test_tables_without_pk(pg_conn, report):
    """Test 8: Tables Without Primary Key"""
    log("Test 8: Tables Without PK...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables t
        LEFT JOIN information_schema.table_constraints tc
            ON t.table_name = tc.table_name
            AND t.table_schema = tc.table_schema
            AND tc.constraint_type = 'PRIMARY KEY'
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
          AND tc.constraint_name IS NULL
    """)
    tables_without_pk = cursor.fetchone()[0]
    cursor.close()
    
    # Expected: 835 (from earlier check)
    expected = 835
    status = 'PASS' if abs(tables_without_pk - expected) < 10 else 'WARN'
    
    report.add_test(
        '3. CONSTRAINTS',
        'Tables Without Primary Key',
        status,
        {
            'Count': tables_without_pk,
            'Percentage': f"{tables_without_pk/14.77:.1f}%",
            'Note': 'Normal for legacy ERP systems'
        }
    )

def test_empty_tables(pg_conn, report):
    """Test 9: Empty Tables"""
    log("Test 9: Empty Tables...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
          AND n_live_tup = 0
    """)
    empty_count = cursor.fetchone()[0]
    cursor.close()
    
    # Get total tables
    cursor = pg_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
    total_tables = cursor.fetchone()[0]
    cursor.close()
    
    percentage = (empty_count / total_tables) * 100 if total_tables > 0 else 0
    status = 'PASS' if percentage < 10 else 'WARN'
    
    report.add_test(
        '2. DATA INTEGRITY',
        'Empty Tables Check',
        status,
        {
            'Empty Tables': empty_count,
            'Total Tables': total_tables,
            'Percentage': f"{percentage:.1f}%"
        }
    )

def test_database_size(pg_conn, report):
    """Test 10: Database Size"""
    log("Test 10: Database Size...")
    
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT pg_size_pretty(pg_database_size('catuno_production'))
    """)
    db_size = cursor.fetchone()[0]
    cursor.close()
    
    report.add_test(
        '4. PERFORMANCE',
        'Database Size',
        'PASS',
        {
            'Size': db_size
        }
    )

def main():
    """Main QA validation"""
    
    print("=" * 100)
    print("CATUNO MIGRATION - COMPREHENSIVE QA VALIDATION")
    print("=" * 100)
    print(f"Report: {REPORT_FILE}")
    print(f"JSON:   {JSON_REPORT}")
    print("")
    
    report = QAReport()
    
    # Connect
    log("Connecting to databases...")
    ifx_conn = connect_informix()
    log("✓ Informix connected")
    
    pg_conn = connect_postgres()
    log("✓ PostgreSQL connected")
    log("")
    
    try:
        # Run all tests
        test_table_count(ifx_conn, pg_conn, report)
        test_row_counts_top_tables(ifx_conn, pg_conn, report)
        test_total_rows(ifx_conn, pg_conn, report)
        test_data_types(pg_conn, report)
        test_primary_keys(pg_conn, report)
        test_indexes(pg_conn, report)
        test_foreign_keys(pg_conn, report)
        test_tables_without_pk(pg_conn, report)
        test_empty_tables(pg_conn, report)
        test_database_size(pg_conn, report)
        
        # Save report
        report.save()
        
        # Print summary
        print("")
        print("=" * 100)
        print("QA VALIDATION COMPLETED!")
        print("=" * 100)
        print(f"Total Tests:  {report.results['summary']['total_tests']}")
        print(f"Passed:       {report.results['summary']['passed']} ✓")
        print(f"Failed:       {report.results['summary']['failed']} ✗")
        print(f"Warnings:     {report.results['summary']['warnings']} ⚠")
        print("")
        print(f"Text Report: {REPORT_FILE}")
        print(f"JSON Report: {JSON_REPORT}")
        print("=" * 100)
        
        # Exit code based on failures
        if report.results['summary']['failed'] > 0:
            print("✗ QA VALIDATION FAILED!")
            return 1
        elif report.results['summary']['warnings'] > 0:
            print("⚠ QA VALIDATION PASSED WITH WARNINGS")
            return 0
        else:
            print("✓ QA VALIDATION PASSED!")
            return 0
        
    finally:
        ifx_conn.close()
        pg_conn.close()
        log("Connections closed")

if __name__ == "__main__":
    import sys
    sys.exit(main())
