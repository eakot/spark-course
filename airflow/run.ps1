# Down
docker-compose down

# Remove data to avoid conflicts
$folders = 'logs', 'plugins'
foreach ($folder in $folders) {
    if (Test-Path -Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
    New-Item -Path . -Name $folder -ItemType "directory"
    icacls $folder /grant *S-1-1-0:F /T
}

# Up Spark
docker-compose build
docker-compose up -d spark-master
Start-Sleep -Seconds 5
docker-compose up -d spark-worker
Start-Sleep -Seconds 5

# Up Airflow
docker-compose up -d
