# launch project
sudo docker-compose down
cd ..
sudo docker-compose down

# CREATE FOLDERS FOR POSTGRE
sudo rm -rf ./services
mkdir -p ./services/postgresql
sudo chmod +777 -R ./services

# START POSTGRE
sudo docker-compose up -d postgresql


# CREATE FOLDERS FOR AIRFLOW
cd airflow

# remove data to avoid conflicts
sudo rm .env
sudo rm -rf ./plugins
sudo rm -rf ./logs

# add permissons to folder
mkdir -p ./logs ./plugins
sudo chmod +777 -R ./dags ./logs ./plugins ./src

# env
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo chmod +777 .env

# up docker
sudo docker-compose up --build