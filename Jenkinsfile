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

        // ⚠️  PLAYGROUND KONFIGURATION
        // Strikte Trennung zu Produktiv:
        //   Prod:       postgres-catuno    Port 5432
        //   Playground: postgres-playground Port 5433  ← dieser Job
        PG_CONTAINER = 'postgres-playground'
        PG_VOLUME    = 'postgres-playground-data'
        PG_PORT      = '5433'
        PG_DATABASE  = 'catuno_production'

        // Skripte & Logs
        SCRIPTS_DIR   = 'C:\\postgres'
        MIGRATION_DIR = 'C:\\postgres\\migration'
    }

    // Nur manueller Start — kein Cron, kein SCM Polling
    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timeout(time: 4, unit: 'HOURS')
        timestamps()
        disableConcurrentBuilds()
    }

    stages {

        // --------------------------------------------------------
        // STAGE 1: SICHERHEITSCHECK
        // Stellt sicher dass Produktiv-Container NICHT berührt wird
        // --------------------------------------------------------
        stage('Check: Produktiv-Schutz') {
            steps {
                echo '========================================================'
                echo 'STAGE 1: Sicherheitscheck'
                echo '  Playground: postgres-playground (Port 5433)'
                echo '  Produktiv:  postgres-catuno     (Port 5432) — wird NICHT angefasst'
                echo '========================================================'
                script {
                    // Explizit sicherstellen dass wir NICHT den Prod-Container verwenden
                    def container = env.PG_CONTAINER
                    if (container == 'postgres-catuno') {
                        error('SICHERHEITSFEHLER: PG_CONTAINER zeigt auf Produktiv! Abbruch.')
                    }
                    def port = env.PG_PORT
                    if (port == '5432') {
                        error('SICHERHEITSFEHLER: PG_PORT 5432 ist Produktiv! Abbruch.')
                    }
                    echo "OK: Container=${container}, Port=${port}"
                }
            }
        }

        // --------------------------------------------------------
        // STAGE 2: TEARDOWN
        // Nur Playground-Ressourcen entfernen
        // --------------------------------------------------------
        stage('Teardown: Playground bereinigen') {
            steps {
                echo '========================================================'
                echo 'STAGE 2: Teardown — nur Playground-Ressourcen'
                echo '========================================================'
                bat """
                    podman stop ${PG_CONTAINER}  2>nul || echo "Container nicht aktiv — OK"
                    podman rm -f ${PG_CONTAINER} 2>nul || echo "Container nicht vorhanden — OK"
                    podman volume rm ${PG_VOLUME} 2>nul || echo "Volume nicht vorhanden — OK"
                """
                echo 'Teardown abgeschlossen.'
            }
        }

        // --------------------------------------------------------
        // STAGE 3: POSTGRESQL PLAYGROUND CONTAINER STARTEN
        // Neuer Container auf Port 5433
        // Produktiv läuft unabhängig auf Port 5432
        // --------------------------------------------------------
        stage('Setup: PostgreSQL Container') {
            steps {
                echo '========================================================'
                echo 'STAGE 3: PostgreSQL Playground Container starten (Port 5433)'
                echo '========================================================'
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
                // Warten bis PostgreSQL hochgefahren ist
                bat 'ping -n 20 127.0.0.1 > nul'
                // Health Check
                bat "podman exec ${PG_CONTAINER} pg_isready -U postgres"
                echo 'PostgreSQL Playground Container ist bereit.'
            }
        }

        // --------------------------------------------------------
        // STAGE 4: DATENBANK ANLEGEN
        // catuno_production — gleicher Name wie Produktiv
        // db_config.py verbindet via PG_PORT=5433 → Playground
        // --------------------------------------------------------
        stage('Setup: Datenbank anlegen') {
            steps {
                echo '========================================================'
                echo 'STAGE 4: Datenbank catuno_production anlegen'
                echo '========================================================'
                bat """
                    podman exec ${PG_CONTAINER} psql -U postgres -c "CREATE DATABASE ${PG_DATABASE} ENCODING 'UTF8';"
                    podman exec ${PG_CONTAINER} psql -U postgres -c "CREATE USER catuno WITH PASSWORD '%PG_PW%';"
                    podman exec ${PG_CONTAINER} psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${PG_DATABASE} TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL ON SCHEMA public TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO catuno;"
                    podman exec ${PG_CONTAINER} psql -U postgres -d ${PG_DATABASE} -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO catuno;"
                """
                bat "podman exec ${PG_CONTAINER} psql -U postgres -c \"\\\\l\""
                echo 'Datenbank catuno_production im Playground bereit.'
            }
        }

        // --------------------------------------------------------
        // STAGE 5: PYTHON DEPENDENCIES
        // --------------------------------------------------------
        stage('Setup: Python Dependencies') {
            steps {
                echo '========================================================'
                echo 'STAGE 5: Python Dependencies pruefen'
                echo '========================================================'
                bat """
                    "%PYTHON_BIN%" -m pip install --break-system-packages jaydebeapi      || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages JPype1          || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages psycopg2-binary || exit /b 0
                """
            }
        }

        // --------------------------------------------------------
        // STAGE 6: INFORMIX CHECK
        // Informix muss laufen bevor Migration startet
        // Schlägt hier fehl → Abbruch, PostgreSQL noch leer → kein Schaden
        // --------------------------------------------------------
        stage('Check: Informix erreichbar') {
            steps {
                echo '========================================================'
                echo 'STAGE 6: Informix Connection Check'
                echo '  Erwartet: kollegen_db auf Port 9095'
                echo '========================================================'
                bat """
                    podman ps | findstr kollegen_db || (
                        echo FEHLER: Informix Container kollegen_db ist nicht aktiv!
                        echo Starten mit: podman start kollegen_db
                        exit /b 1
                    )
                """
                echo 'Informix Container kollegen_db laeuft.'
            }
        }

        // --------------------------------------------------------
        // STAGE 7: DATENMIGRATION
        // Quelle:  Informix  localhost:9095 / unostdtest
        // Ziel:    Playground localhost:5433 / catuno_production
        //          (db_config.py liest PG_PORT=5433 aus ENV)
        // --------------------------------------------------------
        stage('Migration: Daten') {
            steps {
                echo '========================================================'
                echo 'STAGE 7: Datenmigration Informix → PostgreSQL Playground'
                echo '  Quelle: localhost:9095 (Informix / unostdtest)'
                echo '  Ziel:   localhost:5433 (Playground / catuno_production)'
                echo '========================================================'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_full_informix_to_postgres.py
                """
            }
        }

        // --------------------------------------------------------
        // STAGE 8: PRIMARY KEYS (642 erwartet)
        // --------------------------------------------------------
        stage('Migration: Primary Keys') {
            steps {
                echo '========================================================'
                echo 'STAGE 8: Primary Keys migrieren (642 erwartet)'
                echo '========================================================'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_primary_keys.py
                """
            }
        }

        // --------------------------------------------------------
        // STAGE 9: INDIZES (1957 erwartet)
        // --------------------------------------------------------
        stage('Migration: Indizes') {
            steps {
                echo '========================================================'
                echo 'STAGE 9: Indizes migrieren (1957 erwartet)'
                echo '========================================================'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_indexes.py
                """
            }
        }

        // --------------------------------------------------------
        // STAGE 10: FOREIGN KEYS (2 erwartet)
        // --------------------------------------------------------
        stage('Migration: Foreign Keys') {
            steps {
                echo '========================================================'
                echo 'STAGE 10: Foreign Keys migrieren (2 erwartet)'
                echo '========================================================'
                bat """
                    cd /d "${SCRIPTS_DIR}"
                    set JAVA_HOME=%JAVA_HOME%
                    set PG_PORT=${PG_PORT}
                    "%PYTHON_BIN%" -u migrate_foreign_keys.py
                """
            }
        }

        // --------------------------------------------------------
        // STAGE 11: QA VALIDIERUNG
        // --------------------------------------------------------
        stage('QA: Validierung') {
            steps {
                echo '========================================================'
                echo 'STAGE 11: QA Validierung'
                echo '========================================================'
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
                 subject: "✅ CATUNO Playground Migration ERFOLGREICH - Build #${env.BUILD_NUMBER}",
                 body: """Playground Migration erfolgreich abgeschlossen.

Build:      #${env.BUILD_NUMBER}
Dauer:      ${currentBuild.durationString}
Container:  ${PG_CONTAINER} (Port ${PG_PORT})
Datenbank:  ${PG_DATABASE}

Stages:
  Stage 1   ✅ Sicherheitscheck
  Stage 2   ✅ Teardown
  Stage 3   ✅ PostgreSQL Container gestartet (Port 5433)
  Stage 4   ✅ Datenbank angelegt
  Stage 5   ✅ Python Dependencies
  Stage 6   ✅ Informix erreichbar
  Stage 7   ✅ Datenmigration
  Stage 8   ✅ Primary Keys (642)
  Stage 9   ✅ Indizes (1957)
  Stage 10  ✅ Foreign Keys (2)
  Stage 11  ✅ QA Validierung

Build URL:  ${env.BUILD_URL}
Artifacts:  ${env.BUILD_URL}artifact/migration/"""
        }

        failure {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "❌ CATUNO Playground Migration FEHLGESCHLAGEN - Build #${env.BUILD_NUMBER}",
                 body: """Playground Migration fehlgeschlagen.

Build:      #${env.BUILD_NUMBER}
Dauer:      ${currentBuild.durationString}
Container:  ${PG_CONTAINER} (Port ${PG_PORT})

Console Output: ${env.BUILD_URL}console

Produktiv-Container postgres-catuno (Port 5432) wurde nicht beruehrt."""
        }
    }
}
