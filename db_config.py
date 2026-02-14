import os
import sys
import jaydebeapi
import psycopg2

def get_credentials():
    ifx_pw = os.getenv('IFX_PW')
    pg_pw = os.getenv('PG_PW')
    
    if not ifx_pw or not pg_pw:
        print("❌ FEHLER: Datenbank-Passwörter (IFX_PW/PG_PW) nicht gefunden!")
        sys.exit(1)
    return ifx_pw, pg_pw

def connect_informix():
    ifx_pw, _ = get_credentials()
    url = "jdbc:informix-sqli://localhost:9095/unostdtest:INFORMIXSERVER=ol_catuno_utf8en;CLIENT_LOCALE=en_US.utf8;DB_LOCALE=en_US.utf8;DBDATE=DMY4.;DBMONEY=.;DBDELIMITER=|"
    driver = "com.informix.jdbc.IfxDriver"
    jars = [
        r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\jdbc-4.50.11.jar",
        r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\bson-3.8.0.jar"
    ]
    return jaydebeapi.connect(driver, url, ["informix", ifx_pw], jars)

def connect_postgres():
    _, pg_pw = get_credentials()
    return psycopg2.connect(
        host='localhost',
        port=5432,
        database='catuno_production',
        user='catuno',
        password=pg_pw
    )