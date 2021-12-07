# launch project
sudo docker-compose down

# remove data to avoid conflicts
sudo rm -rf ./data
sudo rm -rf ./services
sudo rm -rf ./logs

# add permissons to folder
mkdir -p ./services/postgresql
sudo chmod +777 -R ./services

# logs
mkdir ./logs
sudo chmod +777 ./logs

# up docker
sudo docker-compose up --build --remove-orphans