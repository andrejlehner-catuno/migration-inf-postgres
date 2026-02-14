pipeline {
    agent {
        label 'migration-worker'
    }

    environment {
        JAVA_HOME = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN = 'C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
        
        // Saubere Echtzeit-Logs für die Jenkins Console
        PYTHONIOENCODING = 'utf-8'
        PYTHONUNBUFFERED = '1'
        
        // Jenkins Secrets sicher laden
        IFX_PW = credentials('INFORMIX_PASSWORD')
        PG_PW  = credentials('POSTGRES_PASSWORD')
    }

    triggers {
        cron('0 2 * * *')
    }
    
    options {
        buildDiscarder(logRotator(numToKeepStr: '30'))
        timeout(time: 3, unit: 'HOURS')
        timestamps()
    }
    
    stages {
        stage('Prepare') {
            steps {
                echo 'Starting CATUNO PostgreSQL Migration Pipeline'
                echo "Build: ${env.BUILD_NUMBER}"
            }
        }
        
        stage('Install Dependencies') {
            steps {
                bat '''
                    "%PYTHON_BIN%" -m pip install --break-system-packages jaydebeapi || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages JPype1 || exit /b 0
                    "%PYTHON_BIN%" -m pip install --break-system-packages psycopg2-binary || exit /b 0
                '''
            }
        }
        
        stage('Phase 2a: Primary Keys') {
            steps {
                bat '''
                    cd /d "C:\\postgres"
                    set JAVA_HOME=%JAVA_HOME%
                    "%PYTHON_BIN%" -u migrate_primary_keys.py
                '''
            }
        }
        
        stage('Phase 2b: Indexes') {
            steps {
                bat '''
                    cd /d "C:\\postgres"
                    set JAVA_HOME=%JAVA_HOME%
                    "%PYTHON_BIN%" -u migrate_indexes.py
                '''
            }
        }
        
        stage('Phase 2c: Foreign Keys') {
            steps {
                bat '''
                    cd /d "C:\\postgres"
                    set JAVA_HOME=%JAVA_HOME%
                    "%PYTHON_BIN%" -u migrate_foreign_keys.py
                '''
            }
        }
        
        stage('QA Validation') {
            steps {
                bat '''
                    cd /d "C:\\postgres"
                    set JAVA_HOME=%JAVA_HOME%
                    "%PYTHON_BIN%" -u qa_validation.py
                '''
            }
        }
    }
    
    post {
        always {
            script {
                // Ermittelt das aktuelle Datum für die Dateisuche (Format: 20260214)
                def today = new Date().format("yyyyMMdd")
                echo "Archiviere Reports für den Lauf am: ${today}"
                
                bat """
                    if not exist migration mkdir migration
                    
                    :: Kopiere gezielt nur die Dateien von HEUTE, um Logs nicht aufzublähen
                    if exist C:\\postgres\\migration\\*_${today}_*.log xcopy C:\\postgres\\migration\\*_${today}_*.log migration\\ /Y /I
                    if exist C:\\postgres\\migration\\*_${today}_*.json xcopy C:\\postgres\\migration\\*_${today}_*.json migration\\ /Y /I
                    if exist C:\\postgres\\migration\\*_${today}_*.txt xcopy C:\\postgres\\migration\\*_${today}_*.txt migration\\ /Y /I
                    
                    :: Kopiere Checkpoints (immer die aktuellsten)
                    if exist C:\\postgres\\migration\\*checkpoint.json xcopy C:\\postgres\\migration\\*checkpoint.json migration\\ /Y /I
                """
            }
            archiveArtifacts artifacts: 'migration/*', allowEmptyArchive: true
        }
        
        success {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "✅ CATUNO Migration SUCCESSFUL - Build #${env.BUILD_NUMBER}",
                 body: "Die Migration wurde erfolgreich validiert.\n\nBuild-URL: ${env.BUILD_URL}\nChecke die QA-Reports im Jenkins-Artifact-Store."
        }
        
        failure {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "❌ CATUNO Migration FAILED - Build #${env.BUILD_NUMBER}",
                 body: "Achtung: Die Pipeline ist fehlgeschlagen.\n\nPrüfe die Console: ${env.BUILD_URL}console"
        }
    }
}