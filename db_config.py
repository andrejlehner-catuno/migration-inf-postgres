import os
import jaydebeapi
import psycopg2

# Falls eine Variable fehlt, wirft os.environ[key] sofort einen KeyError
# Das ist genau das "Hart-Abbrechen", das wir wollen.
PG_PASSWORD = os.environ['PG_PW']
IFX_PASSWORD = os.environ['IFX_PW']

# Zentrale PostgreSQL Config
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'catuno_production',
    'user': 'catuno',
    'password': PG_PASSWORD
}

# Informix JDBC Details
INFORMIX_JDBC_URL = "jdbc:informix-sqli://localhost:9095/unostdtest:INFORMIXSERVER=ol_catuno_utf8en;CLIENT_LOCALE=en_US.utf8;DB_LOCALE=en_US.utf8;DBDATE=DMY4.;DBMONEY=.;DBDELIMITER=|"
INFORMIX_JDBC_DRIVER = "com.informix.jdbc.IfxDriver"
INFORMIX_JDBC_JAR = [
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\jdbc-4.50.11.jar",
    r"C:\baustelle_8.6\de.cerpsw.barracuda.runtime\lib\de.cerpsw.sysfunction\bson-3.8.0.jar"
]

def connect_postgres():
    """Verbindung zu PostgreSQL mit den Jenkins-Secrets"""
    return psycopg2.connect(**PG_CONFIG)

def connect_informix():
    """Verbindung zu Informix mit den Jenkins-Secrets"""
    return jaydebeapi.connect(
        INFORMIX_JDBC_DRIVER,
        INFORMIX_JDBC_URL,
        ["informix", IFX_PASSWORD],
        INFORMIX_JDBC_JAR
    )