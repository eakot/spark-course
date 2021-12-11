# Down docker-compose containers
docker-compose down

# Remove unused volumes
docker volume prune

# Remove data
$folders = 'data', 'logs', 'services'
foreach ($folder in $folders) {
    if (Test-Path -Path $folder) {
        Remove-item -Recurse -Force $folder
    }
}
