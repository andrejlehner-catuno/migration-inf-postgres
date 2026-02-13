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
        // Timeout nach 3 Stunden
        timeout(time: 3, unit: 'HOURS')
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
        
        stage('Phase 1: Data Migration') {
            steps {
                echo 'Starting PostgreSQL data migration...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python migrate_full_informix_to_postgres.py
                '''
            }
        }
        
        stage('Phase 2a: Primary Keys') {
            steps {
                echo 'Migrating Primary Keys...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python migrate_primary_keys.py
                '''
            }
        }
        
        stage('Phase 2b: Indexes') {
            steps {
                echo 'Migrating Indexes...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python migrate_indexes.py
                '''
            }
        }
        
        stage('Phase 2c: Foreign Keys') {
            steps {
                echo 'Migrating Foreign Keys...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python migrate_foreign_keys.py
                '''
            }
        }
        
        stage('QA Validation') {
            steps {
                echo 'Running comprehensive QA validation...'
                bat '''
                    set JAVA_HOME=%JAVA_HOME%
                    python qa_validation.py
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
                    
                    // Check QA report
                    def qaReports = findFiles(glob: 'migration/qa_report_*.json')
                    if (qaReports.length > 0) {
                        def latestQA = qaReports[-1]
                        def qaData = readJSON file: latestQA.path
                        
                        echo "QA Tests: ${qaData.summary.total_tests}"
                        echo "Passed: ${qaData.summary.passed}"
                        echo "Failed: ${qaData.summary.failed}"
                        echo "Warnings: ${qaData.summary.warnings}"
                        
                        // Fail build if QA failed
                        if (qaData.summary.failed > 0) {
                            error("QA Validation failed: ${qaData.summary.failed} tests")
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
            echo 'Archiving migration logs and reports...'
            archiveArtifacts artifacts: '''
                migration/*.log, 
                migration/checkpoint.json,
                migration/*_checkpoint.json,
                migration/qa_report_*.txt,
                migration/qa_report_*.json
            ''', allowEmptyArchive: true
        }
        
        success {
            echo '✓✓✓ MIGRATION SUCCESSFUL! ✓✓✓'
            
            script {
                // Get QA summary
                def qaReports = findFiles(glob: 'migration/qa_report_*.json')
                def qaSummary = "No QA report found"
                
                if (qaReports.length > 0) {
                    def latestQA = qaReports[-1]
                    def qaData = readJSON file: latestQA.path
                    qaSummary = """
QA Validation Results:
- Total Tests: ${qaData.summary.total_tests}
- Passed: ${qaData.summary.passed} ✓
- Failed: ${qaData.summary.failed} ✗
- Warnings: ${qaData.summary.warnings} ⚠
"""
                }
                
                emailext (
                    subject: "✅ CATUNO Migration SUCCESSFUL - Build #${env.BUILD_NUMBER}",
                    body: """
Migration completed successfully!

Build Number: ${env.BUILD_NUMBER}
Duration: ${currentBuild.durationString}
Date: ${new Date()}

${qaSummary}

Build URL: ${env.BUILD_URL}
Console Output: ${env.BUILD_URL}console
Artifacts: ${env.BUILD_URL}artifact/

Logs are archived in Jenkins.

---
CATUNO ERP Development Team
                    """,
                    to: 'andrej.lehner@catuno.de',
                    mimeType: 'text/plain'
                )
            }
        }
        
        failure {
            echo '✗✗✗ MIGRATION FAILED! ✗✗✗'
            
            script {
                // Try to get failure reason
                def failureReason = "Unknown error"
                
                // Check for checkpoint failures
                if (fileExists('migration/checkpoint.json')) {
                    def checkpoint = readJSON file: 'migration/checkpoint.json'
                    if (checkpoint.failed_tables && checkpoint.failed_tables.size() > 0) {
                        failureReason = "Data migration failed for ${checkpoint.failed_tables.size()} tables"
                    }
                }
                
                // Check for QA failures
                def qaReports = findFiles(glob: 'migration/qa_report_*.json')
                if (qaReports.length > 0) {
                    def latestQA = qaReports[-1]
                    def qaData = readJSON file: latestQA.path
                    if (qaData.summary.failed > 0) {
                        failureReason = "QA Validation failed: ${qaData.summary.failed} tests"
                    }
                }
                
                emailext (
                    subject: "❌ CATUNO Migration FAILED - Build #${env.BUILD_NUMBER}",
                    body: """
⚠️ MIGRATION FAILED! ⚠️

Build Number: ${env.BUILD_NUMBER}
Duration: ${currentBuild.durationString}
Date: ${new Date()}

Failure Reason: ${failureReason}

Build URL: ${env.BUILD_URL}
Console Output: ${env.BUILD_URL}console

Please check the logs immediately!

Possible causes:
- Database connection issues
- DECIMAL type conversion errors
- Primary Key conflicts
- Index creation failures
- QA validation failures
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
}
