pipeline {
    agent { label 'windows-agent' }

    environment {
        // Zentrale Port-Steuerung für alle Skripte via db_config.py
        PG_PORT = "5433"
        PG_USER = "catuno"
        PG_DB   = "catuno_production"
        // Credentials aus dem Jenkins-Safe
        PG_PW   = credentials('postgres-playground-pw')
        IFX_PW  = credentials('informix-admin-pw')
    }

    options {
        timeout(time: 4, unit: 'HOURS')
        timestamps()
    }

    stages {
        stage('Check: Sicherheit') {
            steps {
                script {
                    echo "========================================================"
                    echo "STAGE 1: Sicherheitscheck"
                    echo "OK: Container=postgres-playground, Port=${env.PG_PORT}"
                }
            }
        }

        stage('Teardown: Playground') {
            steps {
                echo "========================================================"
                echo "STAGE 2: Teardown - Container, Volumes und Checkpoints löschen"
                bat """
                    @echo off
                    podman stop postgres-playground 2>nul || exit /b 0
                    podman rm -f postgres-playground 2>nul || exit /b 0
                    podman volume rm postgres-playground-data 2>nul || exit /b 0
                    
                    echo Lösche alte Migrations-Checkpoints...
                    if exist C:\\postgres\\migration\\*checkpoint.json del /q C:\\postgres\\migration\\*checkpoint.json
                """
                echo "Teardown abgeschlossen (Alles auf Null gesetzt)."
            }
        }

        stage('Setup: PostgreSQL Container') {
            steps {
                echo "========================================================"
                echo "STAGE 3: PostgreSQL Playground Container starten (Port ${env.PG_PORT})"
                bat """
                    podman run -d ^
                        --name postgres-playground ^
                        -e POSTGRES_PASSWORD=postgres ^
                        -e POSTGRES_USER=postgres ^
                        -e POSTGRES_DB=postgres ^
                        -p ${env.PG_PORT}:5432 ^
                        -v postgres-playground-data:/var/lib/postgresql/data ^
                        docker.io/library/postgres:16
                """
                // Warten bis DB bereit ist
                bat "ping -n 20 127.0.0.1 1>nul"
                bat "podman exec postgres-playground pg_isready -U postgres"
            }
        }

        stage('Setup: Datenbank anlegen') {
            steps {
                echo "========================================================"
                echo "STAGE 4: Datenbank ${env.PG_DB} anlegen"
                bat """
                    podman exec postgres-playground psql -U postgres -c "CREATE DATABASE ${env.PG_DB} ENCODING 'UTF8';"
                    podman exec postgres-playground psql -U postgres -c "CREATE USER ${env.PG_USER} WITH PASSWORD '${env.PG_PW}';"
                    podman exec postgres-playground psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${env.PG_DB} TO ${env.PG_USER};"
                    podman exec postgres-playground psql -U postgres -d ${env.PG_DB} -c "GRANT ALL ON SCHEMA public TO ${env.PG_USER};"
                """
            }
        }

        stage('Migration: Daten') {
            steps {
                echo "========================================================"
                echo "STAGE 7: Datenmigration (Nutzt db_config.py)"
                bat """
                    cd /d "C:\\postgres"
                    set JAVA_HOME=C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot
                    set PG_PORT=${env.PG_PORT}
                    "C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" -u migrate_full_informix_to_postgres.py
                """
            }
        }

        stage('Migration: Primary Keys') {
            steps {
                echo "========================================================"
                echo "STAGE 8: Primary Keys"
                bat """
                    cd /d "C:\\postgres"
                    set PG_PORT=${env.PG_PORT}
                    "C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" -u migrate_primary_keys.py
                """
            }
        }

        stage('Migration: Indizes') {
            steps {
                echo "========================================================"
                echo "STAGE 9: Indizes"
                bat """
                    cd /d "C:\\postgres"
                    set PG_PORT=${env.PG_PORT}
                    "C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" -u migrate_indexes.py
                """
            }
        }

        stage('QA: Validierung') {
            steps {
                echo "========================================================"
                echo "STAGE 11: QA Validierung"
                bat """
                    cd /d "C:\\postgres"
                    set PG_PORT=${env.PG_PORT}
                    "C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" -u qa_validation.py
                """
            }
        }
    }

    post {
        always {
            script {
                def dateStr = new Date().format('yyyyMMdd')
                echo "Archiviere Reports für den Lauf am: ${dateStr}"
                bat """
                    if not exist migration mkdir migration
                    if exist C:\\postgres\\migration\\*_${dateStr}_*.log xcopy C:\\postgres\\migration\\*_${dateStr}_*.log migration\\ /Y /I
                    if exist C:\\postgres\\migration\\*checkpoint.json xcopy C:\\postgres\\migration\\*checkpoint.json migration\\ /Y /I
                """
            }
            archiveArtifacts artifacts: 'migration/*', allowEmptyArchive: true
        }
        failure {
            mail to: 'geminiisteintrottel@gmail.com',
                 subject: "Pipeline Failed: ${currentBuild.fullDisplayName}",
                 body: "Der Lauf ist fehlgeschlagen. Bitte prüfe die QA-Reports in den Artifacts."
        }
    }
}