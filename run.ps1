# Down
docker-compose down

# Remove data to avoid conflicts
$folders = 'data', 'logs', 'services'
foreach ($folder in $folders) {
    if (Test-Path -Path $folder) {
        Remove-item -Recurse -Force $folder
    }
}

# Up jobs
docker-compose build
docker-compose up -d
