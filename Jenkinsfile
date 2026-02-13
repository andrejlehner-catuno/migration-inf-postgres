pipeline {
    agent {
        label 'migration-worker' 
    }
    
    environment {
        JAVA_HOME = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN = 'C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
        WORKING_DIR = 'C:\\postgres'
        PYTHONIOENCODING = 'utf-8'
        PYTHONUNBUFFERED = '1'
        // Hier die Ziel-E-Mail (Gmail oder Catuno) eintragen:
        MAIL_TO = 'geminiisteintrottel@gmail.com' 
    }
    
    stages {
        stage('Phase 2a: Primary Keys') {
            steps {
                echo 'Prüfe/Erstelle Primary Keys...'
                bat """
                    set JAVA_HOME=${env.JAVA_HOME}
                    cd /d "${env.WORKING_DIR}"
                    "${env.PYTHON_BIN}" -u migrate_primary_keys.py
                """
            }
        }
        
        stage('Phase 2b: Indexes') {
            steps {
                echo 'Erstelle Datenbank-Indizes...'
                bat """
                    set JAVA_HOME=${env.JAVA_HOME}
                    cd /d "${env.WORKING_DIR}"
                    "${env.PYTHON_BIN}" -u migrate_indexes.py
                """
            }
        }
        
        stage('Phase 2c: Foreign Keys') {
            steps {
                echo 'Erstelle Foreign Keys...'
                bat """
                    set JAVA_HOME=${env.JAVA_HOME}
                    cd /d "${env.WORKING_DIR}"
                    "${env.PYTHON_BIN}" -u migrate_foreign_keys.py
                """
            }
        }
        
        stage('QA Validation') {
            steps {
                echo 'Starte finale QA Validierung...'
                bat """
                    set JAVA_HOME=${env.JAVA_HOME}
                    cd /d "${env.WORKING_DIR}"
                    "${env.PYTHON_BIN}" -u qa_validation.py
                """
            }
        }
    }
    
    post {
        success {
            echo 'Sende Erfolgs-E-Mail...'
            mail to: "${env.MAIL_TO}",
                 subject: "SUCCESS: Migration abgeschlossen (Build #${env.BUILD_NUMBER})",
                 body: "Die Migration der Struktur und die QA-Validierung waren erfolgreich.\n\nDetails findest du hier: ${env.BUILD_URL}"
        }
        failure {
            echo 'Sende Fehler-E-Mail...'
            mail to: "${env.MAIL_TO}",
                 subject: "FAILURE: Fehler in der Migration (Build #${env.BUILD_NUMBER})",
                 body: "Der Build ist fehlgeschlagen. Bitte prüfe die Konsole in Jenkins: ${env.BUILD_URL}"
        }
        always {
            // Fix: Absoluter Pfad, damit Jenkins die Logs in C:\postgres findet
            archiveArtifacts artifacts: 'C:/postgres/migration/*.log, C:/postgres/migration/*.txt', allowEmptyArchive: true
        }
    }
}