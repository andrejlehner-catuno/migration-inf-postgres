pipeline {
    agent {
        label 'migration-worker'
    }

    environment {
        JAVA_HOME        = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN       = 'C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
        PYTHONIOENCODING = 'utf-8'
        PYTHONUNBUFFERED = '1'

        // Jenkins Credentials
        IFX_PW = credentials('INFORMIX_PASSWORD')
        PG_PW  = credentials('POSTGRES_PASSWORD')

        // PLAYGROUND KONFIGURATION
        PG_CONTAINER = 'postgres-playground'
        PG_VOLUME    = 'postgres-playground-data'
        PG_PORT      = '5433'
        PG_DATABASE  = 'catuno_production'

        // Skripte und Logs
        SCRIPTS_DIR   = 'C:\\postgres'
        MIGRATION_DIR = 'C:\\postgres\\migration'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timeout(time: 4, unit: 'HOURS')
        timestamps()
        disableConcurrentBuilds()
    }

    stages {
        stage('Check: Sicherheit') {
            steps {
                echo '========================================================'
                echo 'STAGE 1: Sicherheitscheck'
                script {
                    if (env.PG_CONTAINER == 'postgres-catuno') {
                        error('SICHERHEITSFEHLER: PG_CONTAINER zeigt auf Produktiv! Abbruch.')
                    }
                    if (env.PG_PORT == '5432') {
                        error('SICHERHEITSFEHLER: PG_PORT 5432 ist Produktiv! Abbruch.')
                    }
                    echo "OK: Container=${env.PG_CONTAINER}, Port=${env.PG_PORT}"
                }
            }
        }

        stage('Teardown: Playground') {
            steps {
                echo '========================================================'
                echo 'STAGE 2: Teardown - nur Playground-Ressourcen'
                bat """
                    podman stop ${PG_CONTAINER}  2>nul || exit /b 0
                    podman rm -f ${PG_CONTAINER} 2>nul || exit /b 0
                    podman volume rm ${PG_VOLUME} 2>nul || exit /b 0
                """
                echo 'Teardown abgeschlossen.'
            }
        }

        stage('Setup: PostgreSQL Container') {
            steps {
                echo '========================================================'
                echo 'STAGE 3: PostgreSQL Playground Container starten (Port 5433)'
                bat """
                    podman run -d ^
                        --name ${PG_CONTAINER} ^
                        -e POSTGRES_PASSWORD=postgres ^
                        -e POSTGRES_USER=postgres ^
                        -e POSTGRES_DB=postgres ^
                        -p ${PG_PORT}:5432 ^
                        -v ${PG_VOLUME}:/var/lib/postgresql/data ^
                        docker.io/library/postgres:16
                """
                bat 'ping -n 20 127.0.0.1 > nul'
                bat "podman exec ${PG_CONTAINER} pg_isready -U postgres"
            }
        }

        stage('Setup: Datenbank anlegen') {
            steps {
                echo '========================================================'
                echo 'STAGE 4: Datenbank catuno_production anlegen'
                bat """
                    podman exec ${PG_CONTAINER} psql -U postgres -c "CREATE DATABASE ${PG_DATABASE} ENCODING 'UTF8';"
                    podman exec ${PG_CONTAINER} psql -U postgres -c "CREATE USER catuno WITH PASSWORD '%PG_PW%';"
                    podman exec ${PG_CONTAINER} psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${PG_DATABASE} TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL ON SCHEMA public TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO catuno;"
                    
                    podman exec ${PG_CONTAINER} psql -U postgres -l
                """
                echo 'Datenbank catuno_production im Playground bereit.'
            }
        }

        stage('Setup: Python Dependencies') {
            steps {
                echo '========================================================'
                echo 'STAGE 5: Python Dependencies pruefen'
                bat """
                    "%PYTHON_BIN%" -m pip install --break-system-packages jaydebeapi      || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages JPype1          || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages psycopg2-binary || exit /b 0
                """
            }
        }

        stage('Check: Informix') {
            steps {
                echo '========================================================'
                echo 'STAGE 6: Informix Connection Check'
                bat """
                    podman ps | findstr kollegen_db
                    if errorlevel 1 (
                        echo FEHLER: Informix Container kollegen_db ist nicht aktiv
                        exit /b 1
                    )
                """
            }
        }

        stage('Migration: Daten') {
            steps {
                echo '========================================================'
                echo 'STAGE 7: Datenmigration'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_full_informix_to_postgres.py
                """
            }
        }

        stage('Migration: Primary Keys') {
            steps {
                echo '========================================================'
                echo 'STAGE 8: Primary Keys'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_primary_keys.py
                """
            }
        }

        stage('Migration: Indizes') {
            steps {
                echo '========================================================'
                echo 'STAGE 9: Indizes'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_indexes.py
                """
            }
        }

        stage('Migration: Foreign Keys') {
            steps {
                echo '========================================================'
                echo 'STAGE 10: Foreign Keys'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_foreign_keys.py
                """
            }
        }

        stage('QA: Validierung') {
            steps {
                echo '========================================================'
                echo 'STAGE 11: QA Validierung'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u qa_validation.py
                """
            }
        }
    }

    post {
        always {
            script {
                def today = new Date().format("yyyyMMdd")
                echo "Archiviere Reports fuer den Lauf am: ${today}"
                bat """
                    if not exist migration mkdir migration
                    if exist ${MIGRATION_DIR}\\*_${today}_*.log   xcopy ${MIGRATION_DIR}\\*_${today}_*.log   migration\\ /Y /I
                    if exist ${MIGRATION_DIR}\\*_${today}_*.json  xcopy ${MIGRATION_DIR}\\*_${today}_*.json  migration\\ /Y /I
                    if exist ${MIGRATION_DIR}\\*_${today}_*.txt   xcopy ${MIGRATION_DIR}\\*_${today}_*.txt   migration\\ /Y /I
                    if exist ${MIGRATION_DIR}\\*checkpoint.json   xcopy ${MIGRATION_DIR}\\*checkpoint.json   migration\\ /Y /I
                """
            }
            archiveArtifacts artifacts: 'migration/*', allowEmptyArchive: true
        }
        success {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "SUCCESS: Playground Migration #${env.BUILD_NUMBER}",
                 body: "Migration erfolgreich. URL: ${env.BUILD_URL}"
        }
        failure {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "FAILURE: Playground Migration #${env.BUILD_NUMBER}",
                 body: "Fehlgeschlagen. Log: ${env.BUILD_URL}console"
        }
    }
}