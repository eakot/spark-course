# Down docker-compose containers
docker-compose down

# Remove unused volumes
docker volume prune -f

# Remove data
$folders = 'logs', 'plugins', 'spark-events'
foreach ($folder in $folders) {
    if (Test-Path -Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}