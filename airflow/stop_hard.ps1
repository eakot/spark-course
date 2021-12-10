# Down docker-compose containers
docker-compose down

# Remove unused volumes
docker volume prune

# Remove data
$folders = 'logs', 'plugins'
foreach ($folder in $folders) {
    if (Test-Path -Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}