pipeline {
    agent any
    
    environment {
        JAVA_HOME = 'C:\\baustelle_8.6\\jdk-17.0.11.9-hotspot'
        PYTHON_BIN = 'python'
    }
    
    triggers {
        // Täglich um 2 Uhr nachts
        cron('0 2 * * *')
    }
    
    options {
        // Behalte die letzten 30 Builds
        buildDiscarder(logRotator(numToKeepStr: '30'))
        // Timeout nach 2 Stunden
        timeout(time: 2, unit: 'HOURS')
        // Timestamps in Logs
        timestamps()
    }
    
    stages {
        stage('Prepare') {
            steps {
                echo 'Starting CATUNO PostgreSQL Migration Pipeline'
                echo "Build: ${env.BUILD_NUMBER}"
                echo "Date: ${new Date()}"
                
                // Git Info
                script {
                    def gitCommit = bat(returnStdout: true, script: '@git rev-parse HEAD').trim()
                    def gitAuthor = bat(returnStdout: true, script: '@git log -1 --pretty=format:%%an').trim()
                    echo "Commit: ${gitCommit}"
                    echo "Author: ${gitAuthor}"
                }
            }
        }
        
        stage('Install Dependencies') {
            steps {
                echo 'Installing Python dependencies...'
                bat '''
                    pip install --break-system-packages jaydebeapi || exit /b 0
                    pip install --break-system-packages JPype1 || exit /b 0
                    pip install --break-system-packages psycopg2-binary || exit /b 0
                '''
            }
        }
        
        stage('Check Connections') {
            steps {
                echo 'Checking database connections...'
                bat '''
                    podman ps | findstr postgres-catuno
                    podman ps | findstr kollegen_db
                '''
            }
        }
        
        stage('Run Migration') {
            steps {
                echo 'Starting PostgreSQL migration...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python migrate_full_informix_to_postgres.py
                '''
            }
        }
        
        stage('Verify Migration') {
            steps {
                echo 'Verifying migration results...'
                script {
                    // Check if checkpoint exists
                    if (fileExists('migration/checkpoint.json')) {
                        def checkpoint = readJSON file: 'migration/checkpoint.json'
                        echo "Completed tables: ${checkpoint.completed_tables.size()}"
                        echo "Failed tables: ${checkpoint.failed_tables.size()}"
                        
                        // Fail build if too many failures
                        if (checkpoint.failed_tables.size() > 100) {
                            error("Too many failed tables: ${checkpoint.failed_tables.size()}")
                        }
                    }
                }
            }
        }
        
        stage('Create Backup') {
            steps {
                echo 'Creating PostgreSQL backup...'
                bat '''
                    set BACKUP_DATE=%date:~-4%%date:~3,2%%date:~0,2%
                    podman exec postgres-catuno pg_dump -U catuno -d catuno_production -F c -f /tmp/catuno_backup_%BACKUP_DATE%.dump
                    podman cp postgres-catuno:/tmp/catuno_backup_%BACKUP_DATE%.dump C:\\backup\\
                    echo Backup created: C:\\backup\\catuno_backup_%BACKUP_DATE%.dump
                '''
            }
        }
    }
    
    post {
        always {
            echo 'Archiving migration logs...'
            archiveArtifacts artifacts: 'migration/*.log, migration/checkpoint.json', allowEmptyArchive: true
        }
        
        success {
            echo '✓✓✓ MIGRATION SUCCESSFUL! ✓✓✓'
            
            emailext (
                subject: "✅ CATUNO Migration SUCCESSFUL - Build #${env.BUILD_NUMBER}",
                body: """
Migration completed successfully!

Build Number: ${env.BUILD_NUMBER}
Duration: ${currentBuild.durationString}
Date: ${new Date()}

Build URL: ${env.BUILD_URL}
Console Output: ${env.BUILD_URL}console

Logs are archived in Jenkins.

---
CATUNO ERP Development Team
                """,
                to: 'andrej.lehner@catuno.de',
                mimeType: 'text/plain'
            )
        }
        
        failure {
            echo '✗✗✗ MIGRATION FAILED! ✗✗✗'
            
            emailext (
                subject: "❌ CATUNO Migration FAILED - Build #${env.BUILD_NUMBER}",
                body: """
⚠️ MIGRATION FAILED! ⚠️

Build Number: ${env.BUILD_NUMBER}
Duration: ${currentBuild.durationString}
Date: ${new Date()}

Build URL: ${env.BUILD_URL}
Console Output: ${env.BUILD_URL}console

Please check the logs immediately!

Possible causes:
- Database connection issues
- DECIMAL type conversion errors
- Insufficient disk space
- Network problems

---
CATUNO ERP Development Team
                """,
                to: 'andrej.lehner@catuno.de',
                mimeType: 'text/plain',
                attachLog: true
            )
        }
    }
}
