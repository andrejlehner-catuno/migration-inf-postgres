pipeline {
    agent {
        label 'migration-worker'
    }
    /* 
    environment {
        JAVA_HOME = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN = 'C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
        PYTHONIOENCODING = 'utf-8'
        PYTHONUNBUFFERED = '1'
    }
    */
    environment {
        JAVA_HOME = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN = 'C:\\Users\\LAN\\AppData\\Local\\Programs\\Python\\Python312\\python.exe'
        
        // Diese beiden sorgen für saubere Echtzeit-Logs
        PYTHONIOENCODING = 'utf-8'
        PYTHONUNBUFFERED = '1'
        
        // Jenkins reicht die Secrets sicher als Umgebungsvariable an Python weiter
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
                bat '''
                    if not exist migration mkdir migration
                    if exist C:\\postgres\\migration\\*.log xcopy C:\\postgres\\migration\\*.log migration\\ /Y /I
                    if exist C:\\postgres\\migration\\*.json xcopy C:\\postgres\\migration\\*.json migration\\ /Y /I
                    if exist C:\\postgres\\migration\\*.txt xcopy C:\\postgres\\migration\\*.txt migration\\ /Y /I
                '''
            }
            archiveArtifacts artifacts: 'migration/*.log, migration/*.json, migration/*.txt', allowEmptyArchive: true
        }
        
        success {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "✅ CATUNO Migration SUCCESSFUL - Build #${env.BUILD_NUMBER}",
                 body: "Migration erfolgreich abgeschlossen.\nDetails: ${env.BUILD_URL}"
        }
        
        failure {
            mail to: 'andrej.lehner@catuno.de',
                 subject: "❌ CATUNO Migration FAILED - Build #${env.BUILD_NUMBER}",
                 body: "Fehler in der Pipeline. Prüfe das Log: ${env.BUILD_URL}console"
        }
    }
}