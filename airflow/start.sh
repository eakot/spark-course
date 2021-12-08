# launch project
sudo docker-compose down

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